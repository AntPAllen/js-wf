package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/lease"
)

func TestHeartbeatReusesRecentLeaseRenewal(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	store, err := lease.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	kv, err := all[0].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.Acquire(ctx, "test", "heartbeat-reuse", "owner")
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the initialized revision on this replica before comparing reads.
	var revision uint64
	for revision <= owner.Epoch() {
		entry, err := kv.Get(ctx, "test.heartbeat-reuse")
		if err != nil {
			t.Fatal(err)
		}
		revision = entry.Revision()
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	updated, err := owner.RenewIfIdle(ctx, time.Second)
	if err != nil || updated {
		t.Fatalf("recent renewal: updated=%v err=%v", updated, err)
	}
	entry, err := kv.Get(ctx, "test.heartbeat-reuse")
	if err != nil || entry.Revision() != revision {
		t.Fatalf("heartbeat changed revision: entry=%v before=%d err=%v", entry, revision, err)
	}
	// Append-facing renewal must write even at the same instant.
	if err := owner.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	updated, err = owner.RenewIfIdle(ctx, time.Second)
	if err != nil || !updated {
		t.Fatalf("idle heartbeat: updated=%v err=%v", updated, err)
	}
	if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
	}
	updated, err = owner.RenewIfIdle(ctx, time.Second)
	if updated || !errors.Is(err, lease.ErrLost) {
		t.Fatalf("released heartbeat: updated=%v err=%v", updated, err)
	}
}

func TestFirstWorkerHeartbeatUsesAcknowledgedRenewalFreshness(t *testing.T) {
	for _, idle := range []bool{false, true} {
		name := "recent"
		if idle {
			name = "overdue"
		}
		t.Run(name, func(t *testing.T) {
			all, _ := setup(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			ticks := make(chan time.Time)
			beats := make(chan worker.OperationEvent, 4)
			var appends atomic.Int64
			observe := func(e worker.OperationEvent) {
				if e.Operation == "lease_renew_append" && e.LeaseUpdateAttempted {
					appends.Add(1)
				}
				if e.Operation == "lease_heartbeat_recent" || e.Operation == "lease_renew_heartbeat" {
					beats <- e
				}
			}
			w, err := worker.New(ctx, all[0], "first-heartbeat", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				value, err := wf.Run(c, "held", 0, func(effectCtx context.Context) (int, error) {
					close(entered)
					select {
					case <-release:
						return 42, nil
					case <-effectCtx.Done():
						return 0, effectCtx.Err()
					}
				})
				if err != nil {
					return nil, err
				}
				return json.Marshal(value)
			}}, worker.WithHeartbeatTicks(ticks), worker.WithOperationObserver(observe))
			if err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- w.RunPartition(runCtx, identity.Partition("test", name, provision.Partitions)) }()
			c := client.New(all[2])
			if _, err := c.Start(ctx, "test", name, []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if idle {
				select {
				case <-time.After(3100 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case ticks <- time.Now():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var beat worker.OperationEvent
			select {
			case beat = <-beats:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if beat.Error != "" || beat.LeaseUpdateAttempted != idle {
				t.Fatalf("first heartbeat idle=%t event=%+v", idle, beat)
			}
			close(release)
			result, err := c.Await(ctx, "test", name)
			if err != nil || string(result) != "42" {
				t.Fatalf("result=%s err=%v", result, err)
			}
			stop()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			records, _, err := journal.New(all[1]).Read(ctx, "test", name)
			if err != nil || len(records) != 4 || records[3].Kind != journal.Completed || appends.Load() != 4 {
				t.Fatalf("journal entries=%d unconditional append renewals=%d err=%v", len(records), appends.Load(), err)
			}
		})
	}
}
