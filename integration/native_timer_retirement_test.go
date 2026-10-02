package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/worker"
)

func TestNativeTimerRetirementAfterTerminalJournal(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 45*time.Second)
	defer stop()
	if _, err := client.New(all[0]).Start(ctx, "test", "retirement", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	input, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject("test", "retirement"))
	if err != nil {
		t.Fatal(err)
	}
	store := journal.New(all[0])
	sequence, err := store.Append(ctx, "test", "retirement", journal.Entry{Kind: journal.Started, Epoch: 1, Index: 0, Payload: []byte(`null`)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	schedule := worker.NewTimerSchedulePort(all[1])
	due := time.Now().Add(time.Second)
	publish := func(generation, step uint64) {
		t.Helper()
		if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, schedule, true, "test", "retirement", generation, step, worker.TimerDeadline{FireAt: due, ClockDomain: "utc-quorum-v1", ScheduleAt: due.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
	}
	publish(input.Sequence, 1)
	scanner := reconcile.NewSuspendedScan(all[2])
	if result, err := scanner.Scan(ctx, input.Sequence, 1, false); err != nil || result.Removed != 0 {
		t.Fatalf("active: %+v %v", result, err)
	}
	if _, err := store.Append(ctx, "test", "retirement", journal.Entry{Kind: journal.Completed, Epoch: 1, Index: 1, Payload: []byte(`{"result":42}`)}, sequence); err != nil {
		t.Fatal(err)
	}
	publish(input.Sequence+1, 2)
	if result, err := scanner.Scan(ctx, input.Sequence, 1, true); err != nil || result.Removed != 1 {
		t.Fatalf("dry: %+v %v", result, err)
	}
	run, err := all[1].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.GetLastMsgForSubject(ctx, "wf.schedule.test.retirement.1"); err != nil {
		t.Fatal("dry run removed hint", err)
	}
	if result, err := scanner.Scan(ctx, input.Sequence, 1, false); err != nil || result.Removed != 1 {
		t.Fatalf("retire: %+v %v", result, err)
	}
	if _, err := run.GetLastMsgForSubject(ctx, "wf.schedule.test.retirement.1"); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatal("terminal hint remains", err)
	}
	if _, err := run.GetLastMsgForSubject(ctx, "wf.schedule.test.retirement.2"); err != nil {
		t.Fatal("other generation removed", err)
	}
	if result, err := scanner.Scan(ctx, input.Sequence, 1, false); err != nil || result.Removed != 0 {
		t.Fatalf("retry: %+v %v", result, err)
	}
	t.Log("NATIVE_RETIREMENT active_preserved=true terminal_removed=1 new_generation_preserved=true dry_run_preserved=true retry_idempotent=true")
}
