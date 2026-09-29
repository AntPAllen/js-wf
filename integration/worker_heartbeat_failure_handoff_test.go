package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type heartbeatFailurePort struct {
	consumer              jetstream.Consumer
	failed                atomic.Bool
	injectProgressFailure bool
	stop                  context.CancelFunc
	nacked                chan struct{}
}

func (p *heartbeatFailurePort) Consumer(context.Context, uint32) (worker.DispatchConsumer, error) {
	return heartbeatFailureConsumer{consumer: p.consumer, port: p}, nil
}

func (*heartbeatFailurePort) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type heartbeatFailureConsumer struct {
	consumer jetstream.Consumer
	port     *heartbeatFailurePort
}

func (c heartbeatFailureConsumer) FetchOne(context.Context) (worker.DispatchBatch, error) {
	batch, err := c.consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		return nil, err
	}
	messages := make(chan jetstream.Msg, 1)
	for msg := range batch.Messages() {
		messages <- &heartbeatFailureMsg{Msg: msg, port: c.port}
	}
	close(messages)
	return heartbeatFailureBatch{messages: messages, err: batch.Error()}, nil
}

func (c heartbeatFailureConsumer) Info(ctx context.Context) (uint64, int, error) {
	info, err := c.consumer.Info(ctx)
	if err != nil {
		return 0, 0, err
	}
	return info.NumPending, info.NumAckPending, nil
}

type heartbeatFailureBatch struct {
	messages <-chan jetstream.Msg
	err      error
}

func (b heartbeatFailureBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b heartbeatFailureBatch) Error() error                   { return b.err }

type heartbeatFailureMsg struct {
	jetstream.Msg
	port *heartbeatFailurePort
}

func (m *heartbeatFailureMsg) InProgress() error {
	if m.port.injectProgressFailure && m.port.failed.CompareAndSwap(false, true) {
		return errors.New("injected progress write failure")
	}
	return m.Msg.InProgress()
}

func (m *heartbeatFailureMsg) NakWithDelay(delay time.Duration) error {
	err := m.Msg.NakWithDelay(delay)
	m.port.stop()
	close(m.port.nacked)
	return err
}

func TestWorkerFailedProgressWriteHandsOffUnfinishedStep(t *testing.T) {
	runWorkerHeartbeatFailureHandoff(t, "progress")
}

func TestWorkerClosedHeartbeatTicksHandsOffUnfinishedStep(t *testing.T) {
	runWorkerHeartbeatFailureHandoff(t, "ticks_closed")
}

func runWorkerHeartbeatFailureHandoff(t *testing.T, mode string) {
	t.Helper()
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const typ = "heartbeat"
	id := "failed-progress-handoff"
	if mode == "ticks_closed" {
		id = "closed-ticks-handoff"
	}
	partition := identity.Partition(typ, id, provision.Partitions)
	entered := make(chan struct{})
	effectStopped := make(chan struct{})
	var effects atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "held", 0, func(effectCtx context.Context) (int, error) {
			if effects.Add(1) == 1 {
				close(entered)
				<-effectCtx.Done()
				close(effectStopped)
				return 0, effectCtx.Err()
			}
			return 42, nil
		})
		if err != nil {
			return nil, err
		}
		return json.RawMessage(fmt.Sprint(value)), nil
	}
	firstOptions := []worker.Option{worker.WithDispatchTiming(12*time.Second, 2*time.Second)}
	ticks := make(chan time.Time)
	if mode == "ticks_closed" {
		firstOptions = append(firstOptions, worker.WithHeartbeatTicks(ticks))
	}
	first, err := worker.New(ctx, all[0], "heartbeat-failure-first", map[string]worker.Handler{typ: handler}, firstOptions...)
	if err != nil {
		t.Fatal(err)
	}
	second, err := worker.New(ctx, all[1], "heartbeat-failure-second", map[string]worker.Handler{typ: handler}, worker.WithDispatchTiming(12*time.Second, 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("WF_P_%02d", partition)
	consumer, err := run.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Name: name, Durable: name, FilterSubject: fmt.Sprintf("wf.run.%d", partition),
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 12 * time.Second, MaxDeliver: -1, MaxAckPending: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	port := &heartbeatFailurePort{consumer: consumer, stop: stopFirst, nacked: make(chan struct{}), injectProgressFailure: mode == "progress"}
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartitionWithTransport(firstCtx, partition, port) }()
	c := client.New(all[2])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case err := <-firstDone:
		t.Fatalf("first worker exited before effect: %v", err)
	case <-ctx.Done():
		t.Fatal("first effect did not enter")
	}
	if mode == "ticks_closed" {
		close(ticks)
	}
	select {
	case <-port.nacked:
	case <-ctx.Done():
		t.Fatal("heartbeat failure did not nak")
	}
	select {
	case <-effectStopped:
	case <-ctx.Done():
		t.Fatal("heartbeat failure did not cancel effect")
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	partial, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(partial) != 2 || partial[1].Kind != journal.StepRequested {
		t.Fatalf("partial journal=%+v err=%v", partial, err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "42" {
		t.Fatalf("successor result=%s err=%v", result, err)
	}
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("run message did not drain after handoff")
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) != 4 || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed || records[2].Epoch <= records[1].Epoch || effects.Load() != 2 || port.failed.Load() != (mode == "progress") {
		t.Fatalf("handoff records=%+v effects=%d failed=%v err=%v", records, effects.Load(), port.failed.Load(), err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
}
