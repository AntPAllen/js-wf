package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type heldWorkerRenewJS struct {
	jetstream.JetStream
	lease jetstream.KeyValue
}

func (j heldWorkerRenewJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	if bucket == "WF_LEASE" {
		return j.lease, nil
	}
	return j.JetStream.KeyValue(ctx, bucket)
}

func TestWorkerLostRenewAckHandsOffUnfinishedStep(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	kv, err := proxied.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	// Update 1 initializes the lease epoch; updates 2 and 3 guard the
	// Started and StepRequested appends. Update 4 is the held heartbeat.
	wrapped := heldWorkerRenewJS{JetStream: proxied, lease: &holdLeaseRenewKV{KeyValue: kv, proxy: proxy, held: held, triggerAt: 4}}
	const typ, id = "heartbeat", "lost-renew-handoff"
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
	first, err := worker.New(ctx, wrapped, "lost-renew-first", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
	c := client.New(all[1])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case err := <-firstDone:
		t.Fatalf("first worker exited before effect: %v metrics=%+v", err, first.Metrics())
	case <-time.After(12 * time.Second):
		t.Fatalf("first effect did not enter: metrics=%+v", first.Metrics())
	}
	observer, err := all[1].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := observer.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("worker did not attempt lease renewal")
	}
	for ctx.Err() == nil {
		committed, err := observer.Get(ctx, identity.Key(typ, id))
		if err == nil && committed.Revision() > initial.Revision() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("held renewal did not commit")
	}
	proxy.Block()
	select {
	case <-effectStopped:
	case <-ctx.Done():
		t.Fatal("uncertain renewal did not cancel effect")
	}
	proxy.Heal()
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("first worker did not stop")
	}
	partial, _, err := journal.New(all[1]).Read(ctx, typ, id)
	if err != nil || len(partial) != 2 || partial[1].Kind != journal.StepRequested {
		t.Fatalf("partial journal=%+v err=%v", partial, err)
	}
	second, err := worker.New(ctx, all[2], "lost-renew-second", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, partition) }()
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "42" {
		t.Fatalf("successor result=%s err=%v", result, err)
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
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
	records, _, err := journal.New(all[1]).Read(ctx, typ, id)
	if err != nil || len(records) != 4 || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed || records[2].Epoch <= records[1].Epoch || effects.Load() != 2 || first.Metrics().FencingEvents == 0 {
		t.Fatalf("handoff records=%+v effects=%d metrics=%+v err=%v", records, effects.Load(), first.Metrics(), err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	}
}
