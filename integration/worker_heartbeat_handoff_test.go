package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
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
	runWorkerLostRenewAckHandoff(t, false)
}

func TestWorkerLostRenewAckHandsOffIgnoringCancellationEffect(t *testing.T) {
	if os.Getenv("WF_LEASE_LOSS_BLOCKED_EFFECT_ROOT") == "" {
		t.Skip("opt-in retained native lease-loss blocked effect proof")
	}
	runWorkerLostRenewAckHandoff(t, true)
}

func runWorkerLostRenewAckHandoff(t *testing.T, ignoresCancellation bool) {
	t.Helper()
	var all []jetstream.JetStream
	var cluster *testcluster.Cluster
	root := ""
	proof := map[string]any{"ignores_cancellation": ignoresCancellation}
	var fenceObserved time.Time
	if ignoresCancellation {
		base := os.Getenv("WF_LEASE_LOSS_BLOCKED_EFFECT_ROOT")
		if !filepath.IsAbs(base) {
			t.Fatal("absolute retained proof root required")
		}
		root = filepath.Join(base, t.Name())
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		defer func() {
			proof["native_test_failed"] = t.Failed()
			data, err := json.MarshalIndent(proof, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, "blocked-effect-proof.json"), append(data, '\n'), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		var err error
		cluster, err = testcluster.Start(filepath.Join(root, "cluster"), 3)
		if err != nil {
			t.Fatal(err)
		}
		all, cluster = setupCluster(t, cluster)
	} else {
		all, cluster = setup(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if ignoresCancellation {
		if err := proxy.EnableTrafficTrace(4 << 20); err != nil {
			t.Fatal(err)
		}
	}
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
	effectContexts := make(chan context.Context, 1)
	releaseIgnoredEffect, ignoredEffectReturned := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseEffect := func() { releaseOnce.Do(func() { close(releaseIgnoredEffect) }) }
	defer releaseEffect()
	var effects atomic.Int32
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "held", 0, func(effectCtx context.Context) (int, error) {
			if effects.Add(1) == 1 {
				close(entered)
				if ignoresCancellation {
					effectContexts <- effectCtx
					<-releaseIgnoredEffect
					close(ignoredEffectReturned)
					return 7, nil
				}
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
	var oldEffectCtx context.Context
	if ignoresCancellation {
		select {
		case oldEffectCtx = <-effectContexts:
		case <-ctx.Done():
			t.Fatal("ignored effect context unavailable")
		}
	}
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("worker did not attempt lease renewal")
	}
	var committedLease jetstream.KeyValueEntry
	for ctx.Err() == nil {
		committed, err := observer.Get(ctx, identity.Key(typ, id))
		if err == nil && committed.Revision() > initial.Revision() {
			committedLease = committed
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("held renewal did not commit")
	}
	proxy.Block()
	if ignoresCancellation {
		select {
		case <-oldEffectCtx.Done():
		case <-ctx.Done():
			t.Fatal("uncertain renewal did not cancel abandoned effect context")
		}
		fenceObserved = time.Now().UTC()
		proof["fence_observed"] = fenceObserved
		proof["initial_lease_revision"] = initial.Revision()
		proof["committed_lease_revision"] = committedLease.Revision()
		proof["committed_lease_value"] = json.RawMessage(committedLease.Value())
		select {
		case <-ignoredEffectReturned:
			t.Fatal("ignoring effect returned before explicit release")
		default:
		}
	} else {
		select {
		case <-effectStopped:
		case <-ctx.Done():
			t.Fatal("uncertain renewal did not cancel effect")
		}
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
	proof["first_worker_stopped"] = time.Now().UTC()
	partial, _, err := journal.New(all[1]).Read(ctx, typ, id)
	if err != nil || len(partial) != 2 || partial[1].Kind != journal.StepRequested {
		t.Fatalf("partial journal=%+v err=%v", partial, err)
	}
	proof["partial_journal"] = partial
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
	proof["successor_terminal_observed"] = time.Now().UTC()
	if ignoresCancellation {
		recovery := time.Since(fenceObserved)
		proof["recovery_from_observed_fence_ns"] = recovery.Nanoseconds()
		if recovery >= 30*time.Second {
			t.Fatalf("lease-loss recovery exceeded original30s gate: %s", recovery)
		}
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
	if ignoresCancellation {
		select {
		case <-ignoredEffectReturned:
			t.Fatal("abandoned effect did not remain blocked through successor terminal")
		default:
		}
		proof["abandoned_effect_blocked_through_successor_terminal"] = true
		proof["successor_journal"] = records
		proof["effect_released"] = time.Now().UTC()
		releaseEffect()
		select {
		case <-ignoredEffectReturned:
		case <-ctx.Done():
			t.Fatal("abandoned effect did not return after explicit release")
		}
		proof["abandoned_effect_returned"] = time.Now().UTC()
		for i, js := range all {
			after, _, err := journal.New(js).Read(ctx, typ, id)
			if err != nil || !reflect.DeepEqual(after, records) {
				t.Fatalf("peer%d journal changed after stale effect: %+v err=%v", i, after, err)
			}
			value, err := client.New(js).Await(ctx, typ, id)
			if err != nil || string(value) != "42" {
				t.Fatalf("peer%d stale effect changed result: %s err=%v", i, value, err)
			}
		}
		proof["all_three_peer_journals_and_results_unchanged_after_stale_result"] = true
		proof["effects"] = effects.Load()
		proof["first_worker_metrics"] = first.Metrics()
		leaseStream, err := all[0].Stream(ctx, "KV_WF_LEASE")
		if err != nil {
			t.Fatal(err)
		}
		leaseInfo, err := leaseStream.Info(ctx)
		if err != nil || leaseInfo.Config.MaxAge != 12*time.Second || leaseInfo.Config.Replicas != 3 {
			t.Fatalf("lease profile=%+v err=%v", leaseInfo, err)
		}
		proof["lease_stream_info"] = leaseInfo
		consumer, err := run.Consumer(ctx, fmt.Sprintf("WF_P_%02d", partition))
		if err != nil {
			t.Fatal(err)
		}
		consumerInfo, err := consumer.Info(ctx)
		if err != nil || consumerInfo.Config.AckWait != worker.DefaultAckWait {
			t.Fatalf("dispatch profile=%+v err=%v", consumerInfo, err)
		}
		proof["dispatch_consumer_info"] = consumerInfo
		data, err := json.MarshalIndent(proxy.TrafficTrace(), "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "proxy-traffic.json"), append(data, '\n'), 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if report, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatal(err)
	} else {
		proof["integrity_report"] = report
	}
}
