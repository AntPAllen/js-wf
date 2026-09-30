package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
)

// This reproduces the lease-bound part of the five-container SIGKILL and
// SIGSTOP results. A stopped owner cannot release its lease, and its stale
// journal append must be rejected after a successor has completed.
func runWorkerKillLeaseExpiry(replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(42)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("worker_kill_lease_expiry"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, provision.LeaseTTL))
	transport := NewJournalTransport(schedule)
	store := journal.NewWithPorts(transport, transport)
	old, err := leasing.Acquire(ctx, "test", "killed", "old")
	if err != nil {
		return trace, err
	}
	first, err := store.Append(ctx, "test", "killed", journal.Entry{Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		return trace, err
	}
	second, err := store.Append(ctx, "test", "killed", journal.Entry{Epoch: old.Epoch(), Index: 1, Kind: journal.StepRequested, WorkerID: "old"}, first)
	if err != nil {
		return trace, err
	}
	if err := schedule.AdvanceMillis(provision.LeaseTTL.Milliseconds() - 1); err != nil {
		return trace, err
	}
	if _, err := leasing.Acquire(ctx, "test", "killed", "replacement"); !errors.Is(err, lease.ErrHeld) {
		return trace, fmt.Errorf("replacement acquired before %s TTL: %v", provision.LeaseTTL, err)
	}
	if err := schedule.AdvanceMillis(1); err != nil {
		return trace, err
	}
	next, err := leasing.Acquire(ctx, "test", "killed", "replacement")
	if err != nil || next.Epoch() <= old.Epoch() {
		return trace, fmt.Errorf("replacement after TTL: old=%d next=%v err=%v", old.Epoch(), next, err)
	}
	third, err := store.Append(ctx, "test", "killed", journal.Entry{Epoch: next.Epoch(), Index: 2, Kind: journal.StepCompleted, WorkerID: "replacement"}, second)
	if err != nil {
		return trace, err
	}
	fourth, err := store.Append(ctx, "test", "killed", journal.Entry{Epoch: next.Epoch(), Index: 3, Kind: journal.Completed, WorkerID: "replacement"}, third)
	if err != nil {
		return trace, err
	}
	records, tail, err := store.Read(ctx, "test", "killed")
	if err != nil || len(records) != 4 || tail != fourth || schedule.NowMillis() != provision.LeaseTTL.Milliseconds() {
		return trace, fmt.Errorf("recovery at %dms: entries=%d tail=%d want=%d err=%v", schedule.NowMillis(), len(records), tail, fourth, err)
	}
	if err := schedule.AdvanceMillis((45 * time.Second).Milliseconds() - provision.LeaseTTL.Milliseconds()); err != nil {
		return trace, err
	}
	if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("paused owner renewed after successor acquired the lease: %v", err)
	}
	if _, err := store.Append(ctx, "test", "killed", journal.Entry{Epoch: old.Epoch(), Index: 2, Kind: journal.StepCompleted, WorkerID: "old"}, second); !errors.Is(err, journal.ErrStale) {
		return trace, fmt.Errorf("paused owner appended after successor completed: %v", err)
	}
	afterResume, resumedTail, err := store.Read(ctx, "test", "killed")
	if err != nil || !reflect.DeepEqual(afterResume, records) || resumedTail != fourth || schedule.NowMillis() != 45_000 {
		return trace, fmt.Errorf("journal changed after paused owner resumed at %dms: entries=%v tail=%d err=%v", schedule.NowMillis(), afterResume, resumedTail, err)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestWorkerKillAndPauseLeaseExpiryAndFencing(t *testing.T) {
	generated, err := runWorkerKillLeaseExpiry(nil)
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("SIM_WORKER_PAUSE_OUT"); path != "" {
		if err := generated.Save(path); err != nil {
			t.Fatal(err)
		}
	}
	replayed, err := replayTrace(generated)
	if err != nil || !reflect.DeepEqual(replayed, generated) {
		t.Fatalf("worker kill lease replay: %v", err)
	}
}

// A worker canceled by a transport fault can fail both its first release and
// its cleanup read. Fresh run messages then encounter the still-live lease
// until the last successful renewal expires. This is the lease-bound portion
// of the mixed seed 55 latency failure; the model does not assert why the real
// release failed or how quickly a later run message is delivered.
func runCanceledWorkerRetainedLease(replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(55)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("worker_canceled_retained_lease"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	transport := NewKVTransport(schedule, provision.LeaseTTL)
	leasing := lease.NewWithKVPort(transport)
	journalTransport := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journalTransport, journalTransport)
	old, err := leasing.Acquire(ctx, "test", "canceled", "old")
	if err != nil {
		return trace, err
	}
	first, err := store.Append(ctx, "test", "canceled", journal.Entry{Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		return trace, err
	}
	second, err := store.Append(ctx, "test", "canceled", journal.Entry{Epoch: old.Epoch(), Index: 1, Kind: journal.StepRequested, WorkerID: "old"}, first)
	if err != nil {
		return trace, err
	}
	if err := schedule.AdvanceMillis(9_000); err != nil {
		return trace, err
	}
	if err := old.Renew(ctx); err != nil {
		return trace, err
	}
	if err := transport.QueueFault(KVFault{Operation: "delete", Kind: KVDropBeforeCommit}); err != nil {
		return trace, err
	}
	if err := old.Release(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("failed release: %v", err)
	}
	if err := transport.QueueFault(KVFault{Operation: "get", Kind: KVGetTransportLost}); err != nil {
		return trace, err
	}
	if err := old.Cleanup(ctx); !errors.Is(err, ErrTransportLost) {
		return trace, fmt.Errorf("failed cleanup read: %v", err)
	}
	if err := schedule.AdvanceMillis(provision.LeaseTTL.Milliseconds() - 1); err != nil {
		return trace, err
	}
	if _, err := leasing.Acquire(ctx, "test", "canceled", "replacement"); !errors.Is(err, lease.ErrHeld) {
		return trace, fmt.Errorf("replacement acquired before renewed lease expiry: %v", err)
	}
	if err := schedule.AdvanceMillis(1); err != nil {
		return trace, err
	}
	next, err := leasing.Acquire(ctx, "test", "canceled", "replacement")
	if err != nil || next.Epoch() <= old.Epoch() {
		return trace, fmt.Errorf("replacement after expiry: old=%d next=%v err=%v", old.Epoch(), next, err)
	}
	third, err := store.Append(ctx, "test", "canceled", journal.Entry{Epoch: next.Epoch(), Index: 2, Kind: journal.StepCompleted, WorkerID: "replacement"}, second)
	if err != nil {
		return trace, err
	}
	fourth, err := store.Append(ctx, "test", "canceled", journal.Entry{Epoch: next.Epoch(), Index: 3, Kind: journal.Completed, WorkerID: "replacement"}, third)
	if err != nil {
		return trace, err
	}
	if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("stale cleanup after replacement: %v", err)
	}
	if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("stale renewal after replacement: %v", err)
	}
	if _, err := store.Append(ctx, "test", "canceled", journal.Entry{Epoch: old.Epoch(), Index: 2, Kind: journal.StepCompleted, WorkerID: "old"}, second); !errors.Is(err, journal.ErrStale) {
		return trace, fmt.Errorf("stale owner append after replacement: %v", err)
	}
	records, tail, err := store.Read(ctx, "test", "canceled")
	if err != nil || len(records) != 4 || tail != fourth {
		return trace, fmt.Errorf("retained terminal after handoff: entries=%d tail=%d want=%d err=%v", len(records), tail, fourth, err)
	}
	if schedule.NowMillis() != 9_000+provision.LeaseTTL.Milliseconds() {
		return trace, fmt.Errorf("replacement acquired at %dms", schedule.NowMillis())
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCanceledWorkerRetainedLeaseExpiresAndFences(t *testing.T) {
	generated, err := runCanceledWorkerRetainedLease(nil)
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("SIM_RETAINED_LEASE_OUT"); path != "" {
		if err := generated.Save(path); err != nil {
			t.Fatal(err)
		}
	}
	replayed, err := replayTrace(generated)
	if err != nil || !reflect.DeepEqual(replayed, generated) {
		t.Fatalf("retained canceled lease replay: %v", err)
	}
}
