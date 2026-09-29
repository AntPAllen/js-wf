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
	"github.com/nats-io/nats.go/jetstream"
)

func runSeededWorkerSelectExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_select_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"timer_only", "signal_early", "both_ready_signal", "signal_publish_ack_lost", "signal_enqueue_drop", "signal_enqueue_ack_lost", "timer_publish_ack_lost", "consumer_leader_changed"})
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
	store := journal.NewWithPorts(journals, journals)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var calls int
	w, err := worker.NewWithPorts("modeled-select-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls++
		timer, err := c.Timer("deadline", 2*time.Second)
		if err != nil {
			return nil, err
		}
		choice, _, err := timer.SelectSignal("ready")
		if err != nil {
			return nil, err
		}
		if choice == wf.SignalSelected {
			if err := timer.Cancel(); err != nil {
				return nil, err
			}
		}
		return json.Marshal(choice)
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Timer: timers, TimerNow: func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
	}, NativeTimer: true, Client: c})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	if mode == "timer_publish_ack_lost" {
		if err := timers.QueueFault(LoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopFirst)
	if err := w.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d first select delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	first, _, err := store.Read(ctx, typ, id)
	if err != nil || len(first) != 5 || first[4].Kind != journal.Suspended || len(timers.NativeSources()) != 1 {
		return trace, fmt.Errorf("seed %d initial select entries=%d sources=%d err=%v", seed, len(first), len(timers.NativeSources()), err)
	}
	source := timers.NativeSources()[0]
	due, err := time.Parse(time.RFC3339Nano, source.Header.Get(jetstream.ScheduleHeader)[4:])
	if err != nil {
		return trace, err
	}
	publishSignal := mode != "timer_only" && mode != "timer_publish_ack_lost" && mode != "consumer_leader_changed"
	var sequence uint64
	if publishSignal {
		if mode == "signal_publish_ack_lost" {
			if err := transport.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
				return trace, err
			}
		}
		if mode == "signal_enqueue_drop" || mode == "signal_enqueue_ack_lost" {
			kind := "drop_before_commit"
			if mode == "signal_enqueue_ack_lost" {
				kind = "lose_ack_after_commit"
			}
			if err := transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
				return trace, err
			}
		}
		sequence, err = c.Signal(ctx, typ, id, "ready", []byte(`true`), "select-key")
		if mode == "signal_publish_ack_lost" {
			if !errors.Is(err, client.ErrSignalUnknown) {
				return trace, fmt.Errorf("seed %d signal publish err=%v", seed, err)
			}
			stored := transport.SignalFor(typ, id, "ready")
			if len(stored) != 1 {
				return trace, fmt.Errorf("seed %d retained signals=%d", seed, len(stored))
			}
			sequence = stored[0].Sequence
		} else if mode == "signal_enqueue_drop" || mode == "signal_enqueue_ack_lost" {
			if !errors.Is(err, client.ErrEnqueueUnknown) {
				return trace, fmt.Errorf("seed %d signal enqueue err=%v", seed, err)
			}
		} else if err != nil {
			return trace, err
		}
		if mode == "signal_publish_ack_lost" || mode == "signal_enqueue_drop" || mode == "signal_enqueue_ack_lost" {
			if repaired, scanErr := reconcile.NewSignalScanWithPort(transport.SignalTransport).Scan(ctx, sequence, 1, false); scanErr != nil || repaired.Reenqueued != 1 {
				return trace, fmt.Errorf("seed %d select signal repair=%+v err=%v", seed, repaired, scanErr)
			}
		}
		if transport.Dispatch.Pending() != 1 {
			return trace, fmt.Errorf("seed %d signal select pending=%d", seed, transport.Dispatch.Pending())
		}
	}
	// In the early case, consume the signal before the timer reaches its due time.
	if mode == "signal_early" {
		secondCtx, stopSecond := context.WithCancel(ctx)
		transport.Dispatch.StopWhenDrained(stopSecond)
		if err := w.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d early signal delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
		}
	}
	remaining := due.Sub(base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond))
	if remaining > time.Millisecond {
		if err := timers.Advance(remaining - time.Millisecond); err != nil {
			return trace, err
		}
		if transport.Dispatch.Pending() != 0 && mode == "signal_early" {
			return trace, fmt.Errorf("seed %d early timer wakeup", seed)
		}
		remaining = time.Millisecond
	}
	if err := timers.Advance(remaining); err != nil {
		return trace, err
	}
	if mode == "consumer_leader_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	finalCtx, stopFinal := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopFinal)
	if err := w.RunPartitionWithTransport(finalCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d final select delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	records, _, err := store.Read(ctx, typ, id)
	want := wf.TimerSelected
	wantEntries := 7
	if publishSignal {
		want = wf.SignalSelected
		wantEntries = 10
	}
	if err != nil || len(records) != wantEntries || records[len(records)-1].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d mode=%s final select entries=%d want=%d err=%v", seed, mode, len(records), wantEntries, err)
	}
	if publishSignal {
		if records[5].Kind != journal.SignalConsumed || records[6].Kind != journal.StepCompleted {
			return trace, fmt.Errorf("seed %d selected signal journal kinds=%s,%s", seed, records[5].Kind, records[6].Kind)
		}
	} else if records[5].Kind != journal.StepCompleted {
		return trace, fmt.Errorf("seed %d timer completion kind=%s", seed, records[5].Kind)
	}
	terminal := records[len(records)-1]
	var outcome wf.Outcome
	if err := json.Unmarshal(terminal.Payload, &outcome); err != nil || string(outcome.Result) != `"`+string(want)+`"` {
		return trace, fmt.Errorf("seed %d selected outcome=%+v err=%v", seed, outcome, err)
	}
	retained, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(retained.Value, terminal.Payload) {
		return trace, fmt.Errorf("seed %d selected KV result mismatch: %v", seed, err)
	}
	metrics := w.Metrics()
	wantNoOps := uint64(0)
	if publishSignal {
		wantNoOps = 1
	}
	if metrics.CancelledTimerNoOps != wantNoOps || metrics.TimersFired != 1-wantNoOps {
		return trace, fmt.Errorf("seed %d selected metrics=%+v", seed, metrics)
	}
	if calls != 2 {
		if mode != "timer_publish_ack_lost" || calls != 3 {
			return trace, fmt.Errorf("seed %d mode=%s handler calls=%d", seed, mode, calls)
		}
	}
	transport.SetJournal(typ, id, records)
	if report, err := CheckSignalWakeupLiveness(transport.SignalTransport); err != nil || report.Enabled != 0 {
		return trace, fmt.Errorf("seed %d select signal liveness=%+v err=%v", seed, report, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: wantEntries, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d select retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_select_execution", Outcome: mode + ":" + string(want), Sequence: sequence, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerSelectExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_SELECT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerSelectExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_SELECT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerSelectExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-select-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-select.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerSelectExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker select replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-select-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerSelectExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_SELECT_HELPER=1", "SIM_WORKER_SELECT_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker select trace changed across processes")
	}
}
