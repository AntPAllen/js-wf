package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestLongHandlerHeartbeatPreventsAckWaitRedelivery(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	const typ, id = "heartbeat", "with-progress"
	partition := identity.Partition(typ, id, provision.Partitions)
	manualID := ""
	for i := 0; i < 100; i++ {
		candidate := fmt.Sprintf("without-progress-%d", i)
		if identity.Partition(typ, candidate, provision.Partitions) != partition {
			manualID = candidate
			break
		}
	}
	if manualID == "" {
		t.Fatal("could not find separate control partition")
	}
	manualPartition := identity.Partition(typ, manualID, provision.Partitions)
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	manualName := fmt.Sprintf("WF_P_%02d", manualPartition)
	manual, err := run.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: manualName, Durable: manualName, FilterSubject: fmt.Sprintf("wf.run.%d", manualPartition), AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := all[0].Publish(ctx, identity.RunSubject(typ, manualID, provision.Partitions), []byte(identity.Key(typ, manualID))); err != nil {
		t.Fatal(err)
	}
	firstBatch, err := manual.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var firstManual jetstream.Msg
	for msg := range firstBatch.Messages() {
		firstManual = msg
	}
	if firstManual == nil || firstBatch.Error() != nil {
		t.Fatalf("control first delivery=%v batch_err=%v", firstManual, firstBatch.Error())
	}
	metadata, err := firstManual.Metadata()
	if err != nil || metadata.NumDelivered != 1 {
		t.Fatalf("control first metadata=%+v err=%v", metadata, err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-c.Context().Done():
				return nil, c.Context().Err()
			}
		}
		return json.RawMessage(`true`), nil
	}
	first, err := worker.New(ctx, all[1], "heartbeat-first", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	second, err := worker.New(ctx, all[2], "heartbeat-second", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("heartbeat worker did not enter handler")
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	select {
	case <-time.After(35 * time.Second):
	case <-ctx.Done():
		t.Fatal("long handler did not reach ack wait boundary")
	}
	controlBatch, err := manual.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var controlRedelivery jetstream.Msg
	for msg := range controlBatch.Messages() {
		controlRedelivery = msg
	}
	if controlRedelivery == nil || controlBatch.Error() != nil {
		t.Fatalf("control redelivery=%v batch_err=%v", controlRedelivery, controlBatch.Error())
	}
	metadata, err = controlRedelivery.Metadata()
	if err != nil || metadata.NumDelivered < 2 {
		t.Fatalf("control did not redeliver after AckWait: metadata=%+v err=%v", metadata, err)
	}
	if err := controlRedelivery.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	protected, err := run.Consumer(ctx, fmt.Sprintf("WF_P_%02d", partition))
	if err != nil {
		t.Fatal(err)
	}
	info, err := protected.Info(ctx)
	if err != nil || info.NumRedelivered != 0 || calls.Load() != 1 {
		t.Fatalf("progress heartbeat failed: info=%+v calls=%d err=%v", info, calls.Load(), err)
	}
	releaseOnce.Do(func() { close(release) })
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("run queue not drained: info=%+v err=%v", info, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopFirst()
	stopSecond()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || first.Metrics().Redeliveries != 0 || second.Metrics().Redeliveries != 0 {
		t.Fatalf("unexpected worker delivery: calls=%d first=%+v second=%+v", calls.Load(), first.Metrics(), second.Metrics())
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) != 2 || records[0].Kind != journal.Started || records[1].Kind != journal.Completed {
		t.Fatalf("long handler journal=%+v err=%v", records, err)
	}
}
