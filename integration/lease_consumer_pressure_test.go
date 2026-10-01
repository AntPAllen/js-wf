//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Dedicated stream/consumers add bounded Raft and ack traffic on the same
// physical server stores. They are load generators, not workflow invocations.
type leasePressureTraffic struct {
	js        jetstream.JetStream
	stream    jetstream.Stream
	consumers []jetstream.Consumer
}
type leasePressureTrafficResult struct {
	messages int
	err      error
}

func newLeasePressureTraffic(t *testing.T, ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster) *leasePressureTraffic {
	t.Helper()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "LEASE_PRESSURE_RUN", Subjects: []string{"leasepressure.run.*"}, Storage: jetstream.FileStorage, Replicas: 3, Retention: jetstream.WorkQueuePolicy})
	if err != nil {
		t.Fatal(err)
	}
	p := &leasePressureTraffic{js: js, stream: stream}
	preferMixedAckLeader(t, ctx, cluster.Clients[0], "$JS.API.STREAM.LEADER.STEPDOWN.LEASE_PRESSURE_RUN", "wf-process-0", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
		info, err := stream.Info(ctx)
		if err != nil {
			return nil, err
		}
		return info.Cluster, nil
	})
	for i := 0; i < 8; i++ {
		name := fmt.Sprintf("LEASE_LOAD_%02d", i)
		consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: name, FilterSubject: fmt.Sprintf("leasepressure.run.%02d", i), AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute, Replicas: 3, MaxAckPending: 1})
		if err != nil {
			t.Fatal(err)
		}
		preferMixedAckLeader(t, ctx, cluster.Clients[0], "$JS.API.CONSUMER.LEADER.STEPDOWN.LEASE_PRESSURE_RUN."+name, "wf-process-0", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
			info, err := consumer.Info(ctx)
			if err != nil {
				return nil, err
			}
			return info.Cluster, nil
		})
		p.consumers = append(p.consumers, consumer)
	}
	// Warm data and all consumer Raft files before disk tracing attaches.
	result := p.run(ctx, 1)
	if result.err != nil || result.messages != 8 {
		t.Fatalf("traffic warm-up: %+v", result)
	}
	p.audit(t, ctx)
	return p
}

func (p *leasePressureTraffic) run(ctx context.Context, calls int) leasePressureTrafficResult {
	results := make([]leasePressureTrafficResult, len(p.consumers))
	var wg sync.WaitGroup
	for i, consumer := range p.consumers {
		wg.Add(1)
		go func(i int, consumer jetstream.Consumer) {
			defer wg.Done()
			result := &results[i]
			for n := 0; n < calls; n++ {
				payload := []byte(fmt.Sprintf("%d:%d", i, n))
				attempt, stop := context.WithTimeout(ctx, 3*time.Second)
				_, err := p.js.Publish(attempt, fmt.Sprintf("leasepressure.run.%02d", i), payload)
				stop()
				if err != nil {
					result.err = fmt.Errorf("traffic publish %d/%d: %w", i, n, err)
					return
				}
				batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
				if err != nil {
					result.err = fmt.Errorf("traffic fetch %d/%d: %w", i, n, err)
					return
				}
				count := 0
				for message := range batch.Messages() {
					count++
					metadata, err := message.Metadata()
					if err != nil || !bytes.Equal(message.Data(), payload) || metadata.NumDelivered != 1 {
						result.err = fmt.Errorf("traffic delivery %d/%d metadata=%+v err=%v", i, n, metadata, err)
						return
					}
					attempt, stop := context.WithTimeout(ctx, 3*time.Second)
					err = message.DoubleAck(attempt)
					stop()
					if err != nil {
						result.err = fmt.Errorf("traffic ack %d/%d: %w", i, n, err)
						return
					}
				}
				if count != 1 || batch.Error() != nil {
					result.err = fmt.Errorf("traffic batch %d/%d count=%d err=%v", i, n, count, batch.Error())
					return
				}
				result.messages++
			}
		}(i, consumer)
	}
	wg.Wait()
	var result leasePressureTrafficResult
	for _, row := range results {
		result.messages += row.messages
		if result.err == nil {
			result.err = row.err
		}
	}
	return result
}

func (p *leasePressureTraffic) audit(t *testing.T, ctx context.Context) {
	t.Helper()
	ready, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	var info *jetstream.StreamInfo
	var err error
	for ready.Err() == nil {
		info, err = p.stream.Info(ready)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || info == nil || info.State.Msgs != 0 || info.Cluster == nil || info.Cluster.Leader != "wf-process-0" {
		t.Fatalf("traffic stream not drained or changed placement: info=%+v err=%v", info, err)
	}
	for i, consumer := range p.consumers {
		info, err := consumer.Info(ctx)
		if err != nil || info.NumPending != 0 || info.NumAckPending != 0 || info.Cluster == nil || info.Cluster.Leader != "wf-process-0" {
			t.Fatalf("traffic consumer %d not drained or changed placement: info=%+v err=%v", i, info, err)
		}
	}
}

// Counters come from retained stream state and durable consumer acknowledgement
// floors, independently of the load goroutines' local completion counts.
type leasePressureCounters struct {
	sequence  uint64
	ackFloors []uint64
}

func (p *leasePressureTraffic) counters(t *testing.T, ctx context.Context) leasePressureCounters {
	t.Helper()
	attempt, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	info, err := p.stream.Info(attempt)
	if err != nil {
		t.Fatal(err)
	}
	result := leasePressureCounters{sequence: info.State.LastSeq}
	for _, consumer := range p.consumers {
		info, err := consumer.Info(attempt)
		if err != nil {
			t.Fatal(err)
		}
		result.ackFloors = append(result.ackFloors, info.AckFloor.Consumer)
	}
	return result
}
