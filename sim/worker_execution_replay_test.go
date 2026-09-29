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
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

type faultingExecutionJournal struct {
	*JournalTransport
	subject string
	fault   AppendFault
}

func (p *faultingExecutionJournal) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	if subject == p.subject && p.fault != "" {
		var entry journal.Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			return 0, err
		}
		if entry.Kind == journal.StepCompleted {
			if err := p.QueueFault(Fault{Kind: p.fault}); err != nil {
				return 0, err
			}
			p.fault = ""
		}
	}
	return p.JournalTransport.Publish(ctx, subject, data, expected)
}

func integratedWorkerIDs(count int) []string {
	ids := make([]string, 0, count)
	for candidate := 0; len(ids) < count; candidate++ {
		id := fmt.Sprintf("integrated-%03d", candidate)
		if identity.Partition("test", id, provision.Partitions) == 0 {
			ids = append(ids, id)
		}
	}
	return ids
}

func runSeededWorkerExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_execution_5"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mode, err := schedule.Choose([]string{"clean", "start_run_drop", "start_run_ack_lost", "step_drop", "step_ack_lost", "result_ack_lost", "run_ack_lost", "consumer_leader_changed"})
	if err != nil {
		return trace, err
	}
	const count = 5
	ids := integratedWorkerIDs(count)
	transport := NewWorkerTransport(schedule, 3*time.Second)
	signals := transport.SignalTransport
	journals := NewJournalTransport(schedule)
	appendPort := &faultingExecutionJournal{JournalTransport: journals}
	if mode == "step_drop" || mode == "step_ack_lost" {
		appendPort.subject = identity.JournalSubject("test", ids[2])
		appendPort.fault = DropBeforeCommit
		if mode == "step_ack_lost" {
			appendPort.fault = LoseAckAfterCommit
		}
	}
	store := journal.NewWithPorts(appendPort, journals)
	leaseState := NewKVTransport(schedule, 30*time.Second)
	leasing := lease.NewWithKVPort(leaseState)
	outcomes := NewKVTransport(schedule, 0)
	dispatch := transport.Dispatch
	effects := map[string]int{}
	starter := client.NewWithStartPort(signals.StartTransport)
	for index, id := range ids {
		if index == 0 && (mode == "start_run_drop" || mode == "start_run_ack_lost") {
			kind := "drop_before_commit"
			if mode == "start_run_ack_lost" {
				kind = "lose_ack_after_commit"
			}
			if err := signals.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
				return trace, err
			}
		}
		handle, err := starter.Start(ctx, "test", id, []byte(fmt.Sprintf(`{"id":%q}`, id)))
		if index == 0 && (mode == "start_run_drop" || mode == "start_run_ack_lost") {
			if !errors.Is(err, client.ErrEnqueueUnknown) || handle.InvSeq == 0 {
				return trace, fmt.Errorf("seed %d start %s fault=%s handle=%+v err=%v", seed, id, mode, handle, err)
			}
			if result, scanErr := reconcile.NewStartScanWithPort(signals.StartTransport).Scan(ctx, handle.InvSeq, 1, false); scanErr != nil || result.Reenqueued != 1 || dispatch.Pending() != 1 {
				return trace, fmt.Errorf("seed %d start %s repaired=%+v pending=%d err=%v", seed, id, result, dispatch.Pending(), scanErr)
			}
		} else if err != nil || handle.InvSeq == 0 {
			return trace, fmt.Errorf("seed %d start %s: handle=%+v err=%v", seed, id, handle, err)
		}
	}
	if mode == "result_ack_lost" {
		if err := outcomes.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	if mode == "run_ack_lost" {
		if err := dispatch.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_leader_changed" {
		if err := dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var args struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(input, &args); err != nil {
			return nil, err
		}
		id := args.ID
		value, err := wf.Run(c, "effect", 1, func(context.Context) (int, error) {
			effects[id]++
			return 42, nil
		})
		if err != nil {
			return nil, err
		}
		return json.RawMessage(strconv.Itoa(value)), nil
	}
	w, err := worker.NewWithPorts("modeled-worker", map[string]worker.Handler{"test": handler}, worker.ModeledWorkerPorts{
		Journal: store, Leases: leasing, Outcome: outcomes, Invocation: signals, Signals: signals,
		Client: client.NewWithSignalPorts(signals, signals),
	})
	if err != nil {
		return trace, err
	}
	dispatch.StopWhenDrained(cancel)
	if err := w.RunPartitionWithTransport(ctx, 0, dispatch); err != nil {
		return trace, fmt.Errorf("seed %d worker dispatch: %w", seed, err)
	}
	if ctx.Err() == context.DeadlineExceeded || dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d worker did not drain: pending=%d context=%v", seed, dispatch.Pending(), ctx.Err())
	}
	for _, id := range ids {
		wantEffects := 1
		if mode == "step_drop" && id == ids[2] {
			wantEffects = 2
		}
		if effects[id] != wantEffects {
			return trace, fmt.Errorf("seed %d %s mode=%s effects=%d want=%d", seed, id, mode, effects[id], wantEffects)
		}
		read, _, err := store.Read(context.Background(), "test", id)
		if err != nil || len(read) != 4 || read[3].Kind != journal.Completed {
			return trace, fmt.Errorf("seed %d %s journal entries=%d err=%v", seed, id, len(read), err)
		}
	}
	snapshot, err := retainedModelSnapshot(signals.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: count, Journals: count, Entries: 4 * count, Terminal: count}) {
		return trace, fmt.Errorf("seed %d worker retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_execution", Sequence: uint64(report.Terminal), Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_EXECUTION_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_EXECUTION_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-execution-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-execution.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker execution replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-execution-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_EXECUTION_HELPER=1", "SIM_WORKER_EXECUTION_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker execution trace changed across processes")
	}
}
