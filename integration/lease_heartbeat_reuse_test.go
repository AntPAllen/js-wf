package integration_test

import (
	"context"
	"errors"
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
