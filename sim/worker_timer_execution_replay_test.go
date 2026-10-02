package sim

import (
	"bytes"
	"context"
	"encoding/json"
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
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func runSeededWorkerTimerExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	workload := "worker_timer_execution_v2"
	modes := []string{"clean", "timer_publish_drop", "timer_publish_ack_lost", "await_completion_drop", "await_completion_ack_lost", "timer_run_ack_lost", "consumer_leader_changed", "route_quorum_lost"}
	if replay != nil && replay.Workload == "worker_timer_execution" {
		workload = "worker_timer_execution"
		modes = modes[:len(modes)-1]
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(modes)
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
	timers.OnNativeDelivery(func(msg *nats.Msg, timestamp time.Time) {
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
	}, NativeTimer: true, Client: c})
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
	if err != nil || len(firstRecords) != 5 || firstRecords[4].Kind != journal.Suspended || len(timers.NativeSources()) != 1 {
		return trace, fmt.Errorf("seed %d initial timer journal=%d sources=%d err=%v", seed, len(firstRecords), len(timers.NativeSources()), err)
	}
	source := timers.NativeSources()[0]
	due, err := time.Parse(time.RFC3339Nano, source.Header.Get(jetstream.ScheduleHeader)[4:])
	if err != nil {
		return trace, err
	}
	remaining := due.Sub(base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond))
	if mode == "route_quorum_lost" {
		timers.SetScheduleQuorum(false)
	}
	if remaining > time.Millisecond {
		if err := timers.Advance(remaining - time.Millisecond); err != nil {
			return trace, err
		}
		if transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d timer fired early", seed)
		}
		remaining = time.Millisecond
	}
	if err := timers.Advance(remaining); err != nil {
		return trace, err
	}
	var healedAtMillis int64
	if mode == "route_quorum_lost" {
		if transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d native timer delivered without scheduling quorum", seed)
		}
		if err := timers.Advance(12 * time.Second); err != nil {
			return trace, err
		}
		if transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d overdue native timer delivered before route heal", seed)
		}
		healedAtMillis = schedule.NowMillis()
		choice, err := schedule.Choose([]string{"0", "1000", "5000", "20000"})
		if err != nil {
			return trace, err
		}
		delayMillis, err := strconv.ParseInt(choice, 10, 64)
		if err != nil {
			return trace, err
		}
		if err := timers.SetHealDelay(time.Duration(delayMillis) * time.Millisecond); err != nil {
			return trace, err
		}
		timers.SetScheduleQuorum(true)
		if delayMillis != 0 {
			if transport.Dispatch.Pending() != 0 {
				return trace, fmt.Errorf("seed %d native timer delivered before recovery delay", seed)
			}
			if err := timers.Advance(time.Duration(delayMillis-1) * time.Millisecond); err != nil || transport.Dispatch.Pending() != 0 {
				return trace, fmt.Errorf("seed %d native timer delivered early after heal: pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
			}
			if err := timers.Advance(time.Millisecond); err != nil {
				return trace, err
			}
		}
	}
	if transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("seed %d due timer pending=%d", seed, transport.Dispatch.Pending())
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
	if mode == "route_quorum_lost" && schedule.NowMillis()-healedAtMillis >= 30_000 {
		return trace, fmt.Errorf("seed %d timer resume after route heal took %d virtual ms", seed, schedule.NowMillis()-healedAtMillis)
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
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_timer_execution", Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerTimerExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_TIMER_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerTimerExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_TIMER_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededWorkerTimerExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-timer-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-timer.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerTimerExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker timer replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-timer-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerTimerExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_TIMER_HELPER=1", "SIM_WORKER_TIMER_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker timer trace changed across processes")
	}
}
