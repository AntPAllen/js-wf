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
