package integration_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestWorkerDispatchMetricsAfterLeaseContention(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const typ, id = "metrics", "lease-contention"
	blockers, err := lease.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := blockers.Acquire(ctx, typ, id, "blocker")
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "metrics-worker", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(all[2])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	// Holding the lease forces the first delivery to be nacked. Wait until
	// JetStream redelivers it, then let this worker complete the invocation.
	for w.Metrics().Redeliveries == 0 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("run message was not redelivered")
	}
	if err := blocker.Release(ctx); err != nil {
		t.Fatal(err)
	}
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m := w.Metrics()
	if m.LeaseAcquisitions != 1 || m.FencingEvents != 0 || m.Redeliveries < 1 || m.EnqueueToLeaseSamples != 1 || m.EnqueueToLeaseMaximum < 0 || m.EnqueueToLeaseTotal < m.EnqueueToLeaseMaximum {
		t.Fatalf("dispatch metrics: %+v", m)
	}
	var bucketSamples uint64
	for _, count := range m.EnqueueToLeaseBuckets {
		bucketSamples += count
	}
	if bucketSamples != m.EnqueueToLeaseSamples {
		t.Fatalf("enqueue-to-lease histogram has %d samples, want %d: %+v", bucketSamples, m.EnqueueToLeaseSamples, m)
	}
}

func TestWorkerFencingMetricAndTerminalAppend(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "metrics", "fenced"
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var calls atomic.Int32
	w, err := worker.New(ctx, all[1], "fenced-worker", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			entered <- struct{}{}
			<-release
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("worker did not enter handler")
	}
	kv, err := all[2].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	key := identity.Key(typ, id)
	previous, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Update(ctx, key, []byte(`{"worker":"replacement","epoch":999}`), previous.Revision()); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	for w.Metrics().FencingEvents == 0 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("worker did not report fencing")
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) != 1 || records[0].Kind != journal.Started {
		t.Fatalf("fenced worker changed journal: records=%+v err=%v", records, err)
	}
	current, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(ctx, key, jetstream.LastRevision(current.Revision())); err != nil {
		t.Fatal(err)
	}
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m := w.Metrics()
	if m.FencingEvents != 1 || m.LeaseAcquisitions < 2 || m.Redeliveries < 1 || calls.Load() < 2 {
		t.Fatalf("fencing recovery metrics=%+v calls=%d", m, calls.Load())
	}
}
