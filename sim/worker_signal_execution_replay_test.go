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
)

func runSeededWorkerSignalExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_signal_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "signal_publish_ack_lost", "signal_enqueue_drop", "signal_enqueue_ack_lost", "completion_drop", "completion_ack_lost", "run_ack_lost", "consumer_leader_changed"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journals := NewJournalTransport(schedule)
	appendPort := &faultingExecutionJournal{JournalTransport: journals}
	if mode == "completion_drop" || mode == "completion_ack_lost" {
		appendPort.subject = identity.JournalSubject(typ, id)
		appendPort.fault = DropBeforeCommit
		if mode == "completion_ack_lost" {
			appendPort.fault = LoseAckAfterCommit
		}
	}
	store := journal.NewWithPorts(appendPort, journals)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var handlerCalls int
	w, err := worker.NewWithPorts("modeled-signal-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		handlerCalls++
		value, err := wf.AwaitSignal(c, "go")
		if err != nil {
			return nil, err
		}
		return json.RawMessage(value), nil
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c})
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil || handle.InvSeq == 0 || transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("seed %d start handle=%+v pending=%d err=%v", seed, handle, transport.Dispatch.Pending(), err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopFirst)
	if err := w.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d first delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	firstRecords, _, err := store.Read(ctx, typ, id)
	if err != nil || len(firstRecords) != 3 || firstRecords[2].Kind != journal.Suspended {
		return trace, fmt.Errorf("seed %d suspension entries=%d err=%v", seed, len(firstRecords), err)
	}
	payload := []byte(`{"answer":42}`)
	switch mode {
	case "signal_publish_ack_lost":
		err = transport.QueueSignalFault(SignalLoseAckAfterCommit)
	case "signal_enqueue_drop", "signal_enqueue_ack_lost":
		kind := "drop_before_commit"
		if mode == "signal_enqueue_ack_lost" {
			kind = "lose_ack_after_commit"
		}
		err = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind})
	}
	if err != nil {
		return trace, err
	}
	sequence, signalErr := c.Signal(ctx, typ, id, "go", payload, "key")
	if mode == "signal_publish_ack_lost" {
		if !errors.Is(signalErr, client.ErrSignalUnknown) {
			return trace, fmt.Errorf("seed %d lost signal publish: %v", seed, signalErr)
		}
		stored := transport.SignalFor(typ, id, "go")
		if len(stored) != 1 {
			return trace, fmt.Errorf("seed %d lost signal retained %d", seed, len(stored))
		}
		sequence = stored[0].Sequence
	} else if mode == "signal_enqueue_drop" || mode == "signal_enqueue_ack_lost" {
		if !errors.Is(signalErr, client.ErrEnqueueUnknown) || sequence == 0 {
			return trace, fmt.Errorf("seed %d lost signal wakeup seq=%d err=%v", seed, sequence, signalErr)
		}
	} else if signalErr != nil || sequence == 0 {
		return trace, fmt.Errorf("seed %d signal seq=%d err=%v", seed, sequence, signalErr)
	}
	if mode == "signal_publish_ack_lost" || mode == "signal_enqueue_drop" || mode == "signal_enqueue_ack_lost" {
		if result, err := reconcile.NewSignalScanWithPort(transport.SignalTransport).Scan(ctx, sequence, 1, false); err != nil || result.Reenqueued != 1 {
			return trace, fmt.Errorf("seed %d signal repair=%+v err=%v", seed, result, err)
		}
	}
	if transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("seed %d signal wakeup pending=%d", seed, transport.Dispatch.Pending())
	}
	if mode == "run_ack_lost" {
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
		return trace, fmt.Errorf("seed %d resumed delivery pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 6 || records[3].Kind != journal.SignalConsumed || records[4].Kind != journal.StepCompleted || records[5].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d terminal entries=%d err=%v", seed, len(records), err)
	}
	wantCalls := 2
	if mode == "completion_drop" || mode == "completion_ack_lost" {
		wantCalls++
	}
	if handlerCalls != wantCalls {
		return trace, fmt.Errorf("seed %d mode=%s handler calls=%d want=%d", seed, mode, handlerCalls, wantCalls)
	}
	transport.SetJournal(typ, id, records)
	if report, err := CheckSignalWakeupLiveness(transport.SignalTransport); err != nil || report.Enabled != 0 {
		return trace, fmt.Errorf("seed %d signal liveness enabled=%d missing=%v err=%v", seed, report.Enabled, report.Missing, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 6, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_signal_execution", Sequence: sequence, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerSignalExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_SIGNAL_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerSignalExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_SIGNAL_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededWorkerSignalExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-signal-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-signal.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerSignalExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker signal replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-signal-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerSignalExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_SIGNAL_HELPER=1", "SIM_WORKER_SIGNAL_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker signal trace changed across processes")
	}
}
