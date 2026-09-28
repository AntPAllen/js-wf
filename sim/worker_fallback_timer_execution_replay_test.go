package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func runSeededWorkerFallbackTimerExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("worker_fallback_timer_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "timer_publish_drop", "timer_publish_ack_lost", "wakeup_drop", "wakeup_ack_lost", "delete_drop", "delete_ack_lost", "await_completion_drop", "await_completion_ack_lost", "timer_run_ack_lost", "consumer_leader_changed"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	base := time.Unix(1_700_000_000, 0).UTC()
	transport := NewWorkerTransport(schedule, 3*time.Second)
	timers := NewTimerScheduleTransport(schedule, base)
	timers.OnFallbackWakeup(func(msg *nats.Msg, timestamp time.Time) {
		transport.Dispatch.PublishRunMessage(msg.Subject, msg.Data, msg.Header, timestamp)
	})
	journals := NewJournalTransport(schedule)
	appendPort := &faultingExecutionJournal{JournalTransport: journals}
	if mode == "await_completion_drop" || mode == "await_completion_ack_lost" {
		appendPort.subject = identity.JournalSubject(typ, id)
		appendPort.fault = DropBeforeCommit
		if mode == "await_completion_ack_lost" {
			appendPort.fault = LoseAckAfterCommit
		}
	}
	store := journal.NewWithPorts(appendPort, journals)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var handlerCalls int
	w, err := worker.NewWithPorts("modeled-timer-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		handlerCalls++
		timer, err := c.Timer("wake", 2*time.Second)
		if err != nil {
			return nil, err
		}
		if err := timer.Await(); err != nil {
			return nil, err
		}
		return json.RawMessage(`42`), nil
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Timer: timers, TimerNow: func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
	}, NativeTimer: false, Client: c})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	if mode == "timer_publish_drop" || mode == "timer_publish_ack_lost" {
		fault := DropBeforeCommit
		if mode == "timer_publish_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := timers.QueueFault(fault); err != nil {
			return trace, err
		}
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopFirst)
	if err := w.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d first timer delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	firstRecords, _, err := store.Read(ctx, typ, id)
	if err != nil || len(firstRecords) != 5 || firstRecords[4].Kind != journal.Suspended || len(timers.RetainedFallbackRecords()) != 1 {
		return trace, fmt.Errorf("seed %d initial timer journal=%d sources=%d err=%v", seed, len(firstRecords), len(timers.RetainedFallbackRecords()), err)
	}
	var scheduled struct {
		FireAt time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(timers.RetainedFallbackRecords()[0].Data, &scheduled); err != nil || scheduled.FireAt.IsZero() {
		return trace, fmt.Errorf("seed %d invalid fallback timer: fire_at=%s err=%v", seed, scheduled.FireAt, err)
	}
	scan := reconcile.NewFallbackTimerScanWithPort(timers, func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
	})
	remaining := scheduled.FireAt.Sub(base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond))
	if remaining > time.Millisecond {
		if err := timers.Advance(remaining - time.Millisecond); err != nil {
			return trace, err
		}
		if transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d timer fired early", seed)
		}
		if result, scanErr := scan.Scan(ctx, 1, 1, false); scanErr != nil || result.Reenqueued != 0 || transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d early fallback scan=%+v pending=%d err=%v", seed, result, transport.Dispatch.Pending(), scanErr)
		}
		remaining = time.Millisecond
	}
	if err := timers.Advance(remaining); err != nil {
		return trace, err
	}
	if mode == "wakeup_drop" || mode == "wakeup_ack_lost" {
		fault := DropBeforeCommit
		if mode == "wakeup_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := timers.QueueWakeupFault(fault); err != nil {
			return trace, err
		}
	}
	if mode == "delete_drop" || mode == "delete_ack_lost" {
		fault := DropBeforeCommit
		if mode == "delete_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := timers.QueueDeleteFault(fault); err != nil {
			return trace, err
		}
	}
	firstScan, scanErr := scan.Scan(ctx, 1, 1, false)
	if mode == "wakeup_drop" || mode == "wakeup_ack_lost" || mode == "delete_drop" || mode == "delete_ack_lost" {
		if !errors.Is(scanErr, ErrTransportLost) || firstScan.Reenqueued != 1 {
			return trace, fmt.Errorf("seed %d faulted fallback scan=%+v err=%v", seed, firstScan, scanErr)
		}
		if _, err := scan.Scan(ctx, 1, 1, false); err != nil {
			return trace, fmt.Errorf("seed %d repair fallback scan: %w", seed, err)
		}
	} else if scanErr != nil || firstScan.Reenqueued != 1 {
		return trace, fmt.Errorf("seed %d due fallback scan=%+v err=%v", seed, firstScan, scanErr)
	}
	if transport.Dispatch.Pending() != 1 || len(timers.Runs()) != 1 || len(timers.RetainedFallbackRecords()) != 0 {
		return trace, fmt.Errorf("seed %d repaired timer pending=%d runs=%d retained=%d", seed, transport.Dispatch.Pending(), len(timers.Runs()), len(timers.RetainedFallbackRecords()))
	}
	if mode == "timer_run_ack_lost" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_leader_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopSecond)
	if err := w.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d resumed timer delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 7 || records[5].Kind != journal.StepCompleted || records[6].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d terminal timer entries=%d err=%v", seed, len(records), err)
	}
	wantCalls := 2
	if mode == "timer_publish_drop" || mode == "timer_publish_ack_lost" || mode == "await_completion_drop" || mode == "await_completion_ack_lost" {
		wantCalls++
	}
	if handlerCalls != wantCalls {
		return trace, fmt.Errorf("seed %d mode=%s handler calls=%d want=%d", seed, mode, handlerCalls, wantCalls)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 7, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d timer retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_fallback_timer_execution", Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerFallbackTimerExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_FALLBACK_TIMER_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerFallbackTimerExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_FALLBACK_TIMER_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededWorkerFallbackTimerExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-fallback-timer-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-fallback-timer.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerFallbackTimerExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker fallback timer replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-fallback-timer-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerFallbackTimerExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_FALLBACK_TIMER_HELPER=1", "SIM_WORKER_FALLBACK_TIMER_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("worker fallback timer trace changed across processes")
	}
}
