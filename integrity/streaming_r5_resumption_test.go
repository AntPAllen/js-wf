//go:build linux

package integrity

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

// This wrapper drops the unvisited suffix of one real pull, then reports a
// transport timeout. It deliberately admits a client interruption; it makes
// no claim to reproduce a server defect or natural TCP loss.
type interruptedNativeBatch struct {
	jetstream.MessageBatch
	messages chan jetstream.Msg
	failure  error
}

func (b interruptedNativeBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b interruptedNativeBatch) Error() error {
	if err := b.MessageBatch.Error(); err != nil {
		return err
	}
	return b.failure
}

type interruptedNativeConsumer struct {
	jetstream.Consumer
	interrupted bool
	short       bool
}

func (c *interruptedNativeConsumer) Fetch(n int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	batch, err := c.Consumer.Fetch(n, opts...)
	if err != nil || c.interrupted {
		return batch, err
	}
	c.interrupted = true
	return interruptNativeBatch(batch, c.short), nil
}

func interruptNativeBatch(batch jetstream.MessageBatch, short bool) jetstream.MessageBatch {
	messages := make(chan jetstream.Msg)
	go func() {
		defer close(messages)
		delivered := 0
		for msg := range batch.Messages() {
			if delivered < 128 {
				messages <- msg
			}
			delivered++
		}
	}()
	var failure error = nats.ErrTimeout
	if short {
		failure = nil
	}
	return interruptedNativeBatch{MessageBatch: batch, messages: messages, failure: failure}
}

type interruptedNativeStream struct {
	jetstream.Stream
	starts     []uint64
	names      []string
	pointReads int
	short      bool
}

func (s *interruptedNativeStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.starts = append(s.starts, cfg.OptStartSeq)
	s.names = append(s.names, cfg.Name)
	consumer, err := s.Stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.Replicas != 5 || consumer.CachedInfo().Config.Replicas != 5 {
		return nil, fmt.Errorf("expected R5 read cursor")
	}
	if len(s.starts) == 1 {
		return &interruptedNativeConsumer{Consumer: consumer, short: s.short}, nil
	}
	return consumer, nil
}
func (s *interruptedNativeStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.pointReads++
	return s.Stream.GetMsg(ctx, seq, opts...)
}

func TestStreamingAuditR5LargeInterruptedPull(t *testing.T) {
	if os.Getenv("WF_AUDIT_R5_RESUMPTION") != "1" {
		t.Skip("opt-in five-container large retained audit interruption")
	}
	count := nativeAuditProfileCount(t)
	root := candidateNativeRoot(t)
	cluster, err := testcluster.StartDockerCluster(filepath.Join(root, "cluster"), 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0644); err != nil {
				t.Error(err)
			}
		}
	})
	var urls []string
	for node := 0; node < 5; node++ {
		urls = append(urls, cluster.ClientURL(node))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(512))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Minute)
	defer stop()
	for {
		call, done := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(call, js, 5)
		done()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	want := batchAuditPopulateLargeCohort(t, js, ctx, count)
	audit := func(scan retainedScanner) (Report, error, time.Duration) {
		attempt, done := context.WithTimeout(ctx, 20*time.Second)
		defer done()
		started := time.Now()
		report, err := checkUsingOptions(attempt, js, nil, scan, true, true)
		return report, err, time.Since(started)
	}
	baseline, err, elapsed := audit(scanBatchThrough)
	t.Logf("R5 baseline count=%d elapsed=%s report=%+v err=%v", count, elapsed, baseline, err)
	if err != nil || baseline != want || elapsed >= 20*time.Second {
		t.Fatalf("baseline failed: %+v want=%+v elapsed=%s err=%v", baseline, want, elapsed, err)
	}
	for _, short := range []bool{false, true} {
		var observed *interruptedNativeStream
		visited := 0
		report, err, elapsed := audit(func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
			if stream.CachedInfo().Config.Name != "WF_JRN" {
				return scanBatchThrough(call, stream, cutoff, visit)
			}
			observed = &interruptedNativeStream{Stream: stream, short: short}
			return scanBatchThrough(call, observed, cutoff, func(msg *jetstream.RawStreamMsg) error {
				visited++
				if msg.Sequence != uint64(visited) {
					return fmt.Errorf("duplicate or omitted retained record: visited=%d sequence=%d", visited, msg.Sequence)
				}
				return visit(msg)
			})
		})
		if observed == nil {
			t.Fatalf("journal scan not reached: report=%+v err=%v", report, err)
		}
		t.Logf("R5 interrupted short=%v count=%d visited=%d starts=%v names=%v point_reads=%d elapsed=%s report=%+v err=%v", short, count, visited, observed.starts, observed.names, observed.pointReads, elapsed, report, err)
		wantStart, wantReads := uint64(129), 0
		if short {
			wantStart, wantReads = 130, 1
		}
		if err != nil || report != want || visited != want.Entries || !reflect.DeepEqual(observed.starts, []uint64{1, wantStart}) || observed.pointReads != wantReads || observed.names[0] == observed.names[1] || elapsed >= 20*time.Second {
			t.Fatalf("interruption recovery failed: report=%+v want=%+v err=%v", report, want, err)
		}
		for _, name := range []string{"WF_INV", "WF_JRN"} {
			stream, err := js.Stream(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			info, err := stream.Info(ctx)
			if err != nil || info.State.Consumers != 0 {
				t.Fatalf("consumer cleanup: stream=%s info=%+v err=%v", name, info, err)
			}
		}
	}
}
