package sim

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/lease"
)

// This reproduces the lease-bound part of the five-container SIGKILL result.
// Without a live owner to release the lease, even an immediately available
// replacement cannot append a terminal entry before the configured 30 s TTL.
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
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
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
	if err := schedule.AdvanceMillis(29_999); err != nil {
		return trace, err
	}
	if _, err := leasing.Acquire(ctx, "test", "killed", "replacement"); !errors.Is(err, lease.ErrHeld) {
		return trace, fmt.Errorf("replacement acquired before 30s TTL: %v", err)
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
	if err != nil || len(records) != 4 || tail != fourth || schedule.NowMillis() != 30_000 {
		return trace, fmt.Errorf("recovery at %dms: entries=%d tail=%d want=%d err=%v", schedule.NowMillis(), len(records), tail, fourth, err)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestWorkerKillLeaseExpiryExplainsLatency(t *testing.T) {
	generated, err := runWorkerKillLeaseExpiry(nil)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := replayTrace(generated)
	if err != nil || !reflect.DeepEqual(replayed, generated) {
		t.Fatalf("worker kill lease replay: %v", err)
	}
}
