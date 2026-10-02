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

type nativeRetirementScanPort struct {
	*SignalTransport
	reconcile.NativeTimerRetirePort
}

func runNativeTimerRetirement(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("native_timer_terminal_retirement"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"normal", "drop_delete", "lost_delete_reply", "failed_terminal"})
	if err != nil {
		return trace, err
	}
	scannerKind, err := schedule.Choose([]string{"timer", "suspended"})
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
	// A translated native hint is sixty seconds later than the durable deadline.
	due := base.Add(2 * time.Second)
	if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, timers, true, "test", "one", generation, 1, worker.TimerDeadline{FireAt: due, ClockDomain: "utc-quorum-v1", ScheduleAt: due.Add(time.Minute)}); err != nil {
		return trace, err
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
	if mode == "failed_terminal" {
		terminal = journal.Failed
	}
	inv.SetJournal("test", "one", []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: terminal}}})
	// Dry run reports eligibility without deletion.
	if result, err := scan(ctx, 1, 1, true); err != nil || result.Removed != 1 {
		return trace, fmt.Errorf("dry run %+v %v", result, err)
	}
	if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 1 {
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
	if subjects, _ := timers.NativeTimerSubjects(ctx, "test", "one"); len(subjects) != 0 {
		return trace, fmt.Errorf("terminal hint retained")
	}
	// A late old publication is retired on the next scan. A new generation on
	// another step remains, even while the reader still sees the old terminal.
	for _, item := range []struct{ gen, step uint64 }{{generation, 2}, {generation + 1, 3}} {
		if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, timers, true, "test", "one", item.gen, item.step, worker.TimerDeadline{FireAt: due, ClockDomain: "utc-quorum-v1", ScheduleAt: due.Add(time.Minute)}); err != nil {
			return trace, err
		}
	}
	if result, err := scan(ctx, 1, 1, false); err != nil || result.Removed != 1 {
		return trace, fmt.Errorf("late cleanup %+v %v", result, err)
	}
	subjects, err := timers.NativeTimerSubjects(ctx, "test", "one")
	if err != nil || len(subjects) != 1 || subjects[0] != "wf.schedule.test.one.3" {
		return trace, fmt.Errorf("new generation lost: %v %v", subjects, err)
	}
	var delivered []string
	timers.OnNativeDelivery(func(message *nats.Msg, _ time.Time) {
		delivered = append(delivered, message.Header.Get(identity.TimerInvSeqHeader))
	})
	if err := timers.Advance(2 * time.Minute); err != nil {
		return trace, err
	}
	if len(delivered) != 1 || delivered[0] != fmt.Sprint(generation+1) {
		return trace, fmt.Errorf("retired timer delivered: %v", delivered)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_native_retirement", Outcome: mode})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededNativeTimerTerminalRetirement(t *testing.T) {
	if path := os.Getenv("SIM_NATIVE_RETIREMENT_OUT"); path != "" {
		trace, err := runNativeTimerRetirement(42, nil)
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
		trace, err := runNativeTimerRetirement(seed, nil)
		if err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		coverage[trace.Decisions[0].Chosen+"/"+trace.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runNativeTimerRetirement(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("replay seed=%d: %v", seed, err)
			}
		}
	}
	if len(coverage) != 8 {
		t.Fatal("missing retirement modes", coverage)
	}
	// JSON serialization is also the cross-process replay format.
	trace, err := runNativeTimerRetirement(42, nil)
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
	replayed, err := runNativeTimerRetirement(42, &saved)
	if err != nil || !reflect.DeepEqual(trace, replayed) {
		t.Fatal("serialized replay", err)
	}
}
