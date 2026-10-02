package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/worker"
)

func runDeliveredNativeSourceRetirement(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("delivered_native_source_retirement"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"normal", "drop_delete", "lost_delete_reply"})
	if err != nil {
		return trace, err
	}
	scannerKind, err := schedule.Choose([]string{"timer", "suspended"})
	if err != nil {
		return trace, err
	}
	retention, err := schedule.Choose([]string{"retained_on_delivery", "restored_after_delivery"})
	if err != nil {
		return trace, err
	}
	terminalKind, err := schedule.Choose([]string{"Completed", "Failed"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	base := time.Unix(1700000000, 0).UTC()
	inv := NewSignalTransport(schedule)
	timers := NewTimerScheduleTransport(schedule, base)
	generation, err := inv.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "one"), Data: []byte(`null`)})
	if err != nil {
		return trace, err
	}
	// Source retention is independent of its acknowledged target delivery.
	due := base.Add(2 * time.Second)
	if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, timers, true, "test", "one", generation, 1, worker.TimerDeadline{FireAt: due, ClockDomain: "utc-quorum-v1", ScheduleAt: due}); err != nil {
		return trace, err
	}
	var delivered []string
	timers.OnNativeDelivery(func(message *nats.Msg, _ time.Time) {
		delivered = append(delivered, message.Header.Get(identity.TimerInvSeqHeader))
	})
	if retention == "retained_on_delivery" {
		timers.RetainDeliveredNativeSources()
	}
	if err := timers.Advance(3 * time.Second); err != nil {
		return trace, err
	}
	if len(delivered) != 1 || delivered[0] != fmt.Sprint(generation) {
		return trace, fmt.Errorf("missing durable target delivery: %v", delivered)
	}
	if retention == "restored_after_delivery" {
		if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 0 {
			return trace, fmt.Errorf("pre-restore source retained")
		}
		timers.RestoreDeliveredNativeSources()
	}
	inv.SetJournal("test", "one", []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}})
	port := nativeRetirementScanPort{inv, timers}
	scan := reconcile.NewTimerScanWithPort(port).Scan
	if scannerKind == "suspended" {
		scan = reconcile.NewSuspendedScanWithPort(port).Scan
	}
	// A live journal must not retire the future hint.
	if result, err := scan(ctx, 1, 1, false); err != nil || result.Removed != 0 {
		return trace, fmt.Errorf("active cleanup %+v %v", result, err)
	}
	if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 1 {
		return trace, fmt.Errorf("lost active timer")
	}
	terminal := journal.Completed
	if terminalKind == "Failed" {
		terminal = journal.Failed
	}
	inv.SetJournal("test", "one", []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: terminal}}})
	if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, timers, true, "test", "one", generation+1, 3, worker.TimerDeadline{FireAt: due.Add(time.Minute), ClockDomain: "utc-quorum-v1", ScheduleAt: due.Add(time.Minute)}); err != nil {
		return trace, err
	}
	// Dry run reports eligibility without deletion.
	if result, err := scan(ctx, 1, 1, true); err != nil || result.Removed != 1 {
		return trace, fmt.Errorf("dry run %+v %v", result, err)
	}
	if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 2 {
		return trace, fmt.Errorf("dry run deleted timer")
	}
	if mode == "drop_delete" {
		timers.QueueDeleteFault(DropBeforeCommit)
	}
	if mode == "lost_delete_reply" {
		timers.QueueDeleteFault(LoseAckAfterCommit)
	}
	first, err := scan(ctx, 1, 1, false)
	if mode == "drop_delete" || mode == "lost_delete_reply" {
		if err == nil {
			return trace, fmt.Errorf("ambiguous deletion accepted")
		}
	} else if err != nil || first.Removed != 1 {
		return trace, fmt.Errorf("terminal cleanup %+v %v", first, err)
	}
	if _, err := scan(ctx, 1, 1, false); err != nil {
		return trace, err
	}
	if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 1 || subjects[0] != "wf.schedule.test.one.3" {
		return trace, fmt.Errorf("terminal hint retained or new generation deleted")
	}
	// Restore is not target replay; repeated scans cannot execute the old timer.

	if result, err := scan(ctx, 1, 1, false); err != nil || result.Removed != 0 {
		return trace, fmt.Errorf("new generation cleanup %+v %v", result, err)
	}
	if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 1 {
		return trace, fmt.Errorf("new generation lost")
	}
	if err := timers.Advance(2 * time.Minute); err != nil {
		return trace, err
	}
	if len(delivered) != 2 || delivered[1] != fmt.Sprint(generation+1) {
		return trace, fmt.Errorf("retired source delivered again: %v", delivered)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_delivered_native_retirement", Outcome: mode + "/" + scannerKind + "/" + retention + "/" + terminalKind})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededDeliveredNativeSourceRetirement(t *testing.T) {
	if path := os.Getenv("SIM_DELIVERED_NATIVE_SOURCE_OUT"); path != "" {
		trace, err := runDeliveredNativeSourceRetirement(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	coverage := map[string]bool{}
	for seed := range seededSchedules(t) {
		trace, err := runDeliveredNativeSourceRetirement(seed, nil)
		if err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		coverage[trace.Decisions[0].Chosen+"/"+trace.Decisions[1].Chosen+"/"+trace.Decisions[2].Chosen+"/"+trace.Decisions[3].Chosen] = true
		if seed <= 10 {
			replayed, err := runDeliveredNativeSourceRetirement(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("replay seed=%d: %v", seed, err)
			}
		}
	}
	if len(coverage) != 24 {
		t.Fatal("missing retirement modes", coverage)
	}
	// JSON serialization is also the cross-process replay format.
	trace, err := runDeliveredNativeSourceRetirement(42, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	var saved Trace
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	replayed, err := runDeliveredNativeSourceRetirement(42, &saved)
	if err != nil || !reflect.DeepEqual(trace, replayed) {
		t.Fatal("serialized replay", err)
	}
}
