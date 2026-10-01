package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type missingSnapshotReply struct {
	journal.SnapshotWritePort
	calls    atomic.Int64
	entered  chan error
	expired  chan struct{}
	deadline time.Time
}

func (p *missingSnapshotReply) GetManifestRevision(ctx context.Context, key string) (journal.SnapshotManifestValue, error) {
	if p.calls.Add(1) == 1 {
		deadline, ok := ctx.Deadline()
		p.deadline = deadline
		if !ok || time.Until(deadline) > 15*time.Second {
			err := fmt.Errorf("snapshot metadata request has delivery lifetime, not a 15s budget")
			p.entered <- err
			return journal.SnapshotManifestValue{}, err
		}
		p.entered <- nil
		<-ctx.Done()
		close(p.expired)
		return journal.SnapshotManifestValue{}, ctx.Err()
	}
	return p.SnapshotWritePort.GetManifestRevision(ctx, key)
}

func TestWorkerSnapshotMissingReplyReleasesSuspendedOwner(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 55*time.Second)
	defer stop()
	const typ, id = "snapshot-budget", "suspended"
	port := &missingSnapshotReply{SnapshotWritePort: journal.NewSnapshotPort(all[1]), entered: make(chan error, 1), expired: make(chan struct{})}
	store := journal.NewWithJetStreamSnapshotPort(all[1], port)
	var effects, calls, heartbeats, timeouts atomic.Int64
	w, err := worker.New(ctx, all[1], "snapshot-budget-owner", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		_, err := wf.Run(c, "once", 42, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
		if err != nil {
			return nil, err
		}
		if _, err := wf.AwaitSignal(c, "continue"); err != nil {
			return nil, err
		}
		return json.RawMessage(`42`), nil
	}}, worker.WithJournalStore(store), worker.WithPartitionConcurrency(2), worker.WithOperationObserver(func(e worker.OperationEvent) {
		if e.Operation == "lease_renew_heartbeat" && e.Error == "" {
			heartbeats.Add(1)
		}
		if e.Operation == "journal_snapshot" && e.Error == context.DeadlineExceeded.Error() {
			timeouts.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	runCtx, stopRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(runCtx, identity.Partition(typ, id, provision.Partitions)) }()
	defer func() {
		stopRun()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	select {
	case err := <-port.entered:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	prefix, tail, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || len(prefix) == 0 || prefix[len(prefix)-1].Kind != journal.Suspended {
		t.Fatalf("suspended prefix tail=%d err=%v", tail, err)
	}
	state, err := all[2].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("premature terminal state: %v", err)
	}
	if _, err := c.Signal(ctx, typ, id, "continue", []byte(`true`), "resume"); err != nil {
		t.Fatal(err)
	}
	enabling := time.Now()
	// The actual heartbeat must keep the owner fenced past its twelve-second TTL
	// while metadata has no reply; the bounded call then permits release/retry.
	timer := time.NewTimer(time.Until(port.deadline.Add(-2 * time.Second)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	leasing, err := lease.New(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	if unexpected, err := leasing.Acquire(ctx, typ, id, "competitor"); !errors.Is(err, lease.ErrHeld) {
		if err == nil {
			_ = unexpected.Release(ctx)
		}
		t.Fatalf("owner was not fenced during missing reply: %v", err)
	}
	// The recent-renewal optimization can skip alternate ticker boundaries;
	// two actual updates plus ErrHeld after the original TTL prove maintenance.
	if heartbeats.Load() < 2 {
		t.Fatalf("missing real heartbeat renewals: %d", heartbeats.Load())
	}
	select {
	case <-port.expired:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result, err := c.Await(ctx, typ, id)
	if err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	recovery := time.Since(enabling)
	if recovery >= 30*time.Second {
		t.Fatalf("snapshot missing-reply signal-to-terminal=%s exceeds 30s", recovery)
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("queue did not drain: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	records, _, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || len(records) < len(prefix) || !reflect.DeepEqual(prefix, records[:len(prefix)]) {
		t.Fatalf("suspended prefix changed: %v", err)
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); !errors.Is(err, client.ErrAlreadyStarted) {
		t.Fatalf("duplicate start=%v", err)
	}
	after, err := c.Await(ctx, typ, id)
	if err != nil || string(after) != "42" {
		t.Fatalf("immutable result=%s err=%v", after, err)
	}
	leases, err := all[2].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leases.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("snapshot timeout retained owner: %v", err)
	}
	if calls.Load() != 2 || effects.Load() != 1 || timeouts.Load() != 1 {
		t.Fatalf("calls=%d effects=%d timeouts=%d", calls.Load(), effects.Load(), timeouts.Load())
	}
	report, err := integrity.Check(ctx, all[2])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("retained=%+v err=%v", report, err)
	}
	t.Logf("missing snapshot reply recovered in %s signal_to_terminal=%s; inv_seq=%d calls=%d effects=%d heartbeat_renewals=%d timeout=%d integrity=%+v", time.Since(started), recovery, handle.InvSeq, calls.Load(), effects.Load(), heartbeats.Load(), timeouts.Load(), report)
}
