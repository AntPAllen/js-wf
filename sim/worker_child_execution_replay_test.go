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
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func runSeededWorkerChildExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_child_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "notify_signal_drop", "notify_signal_ack_lost", "notify_enqueue_drop", "notify_enqueue_ack_lost", "child_outcome_ack_lost", "child_step_drop", "child_step_ack_lost", "consumer_leader_changed"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const parentType, parentID, childType = "parent", "fanout", "child"
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journals := NewJournalTransport(schedule)
	appendPort := &faultingExecutionJournal{JournalTransport: journals}
	store := journal.NewWithPorts(appendPort, journals)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var parentCalls, childCalls, effects int
	w, err := worker.NewWithPorts("modeled-child-worker", map[string]worker.Handler{
		parentType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			parentCalls++
			promise, err := wf.CallAsync(c, childType, []byte(`21`))
			if err != nil {
				return nil, err
			}
			first, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			want := bytes.Clone(first)
			if len(first) != 0 {
				first[0] ^= 0xff
			}
			repeated, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(repeated, want) {
				return nil, fmt.Errorf("repeated promise result changed")
			}
			return repeated, nil
		},
		childType: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			childCalls++
			var n int
			if err := json.Unmarshal(input, &n); err != nil {
				return nil, err
			}
			value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { effects++; return n * 2, nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
	}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, parentType, parentID, []byte(`null`)); err != nil {
		return trace, err
	}
	runOne := func(part uint32) error {
		runCtx, stopRun := context.WithCancel(ctx)
		transport.Dispatch.StopAfterNextAck(stopRun)
		return w.RunPartitionWithTransport(runCtx, part, transport.Dispatch)
	}
	parentPart := identity.Partition(parentType, parentID, provision.Partitions)
	if err := runOne(parentPart); err != nil {
		return trace, fmt.Errorf("seed %d parent first run: %w", seed, err)
	}
	parentFirst, _, err := store.Read(ctx, parentType, parentID)
	if err != nil || len(parentFirst) != 5 || parentFirst[4].Kind != journal.Suspended {
		return trace, fmt.Errorf("seed %d parent suspension entries=%d err=%v", seed, len(parentFirst), err)
	}
	var request struct {
		Kind    string `json:"kind"`
		ChildID string `json:"child_id"`
	}
	if err := json.Unmarshal(parentFirst[1].Payload, &request); err != nil || request.Kind != "call_async" || request.ChildID == "" {
		return trace, fmt.Errorf("seed %d child request=%+v err=%v", seed, request, err)
	}
	childID := request.ChildID
	if transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("seed %d child run pending=%d", seed, transport.Dispatch.Pending())
	}
	if mode == "child_step_drop" || mode == "child_step_ack_lost" {
		appendPort.subject = identity.JournalSubject(childType, childID)
		appendPort.fault = DropBeforeCommit
		if mode == "child_step_ack_lost" {
			appendPort.fault = LoseAckAfterCommit
		}
	}
	if mode == "child_outcome_ack_lost" {
		if err := outcomes.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	if mode == "notify_signal_drop" || mode == "notify_signal_ack_lost" {
		fault := SignalDropBeforeCommit
		if mode == "notify_signal_ack_lost" {
			fault = SignalLoseAckAfterCommit
		}
		if err := transport.QueueSignalFault(fault); err != nil {
			return trace, err
		}
	}
	if mode == "notify_enqueue_drop" || mode == "notify_enqueue_ack_lost" {
		kind := "drop_before_commit"
		if mode == "notify_enqueue_ack_lost" {
			kind = "lose_ack_after_commit"
		}
		if err := transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_leader_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	childPart := identity.Partition(childType, childID, provision.Partitions)
	if err := runOne(childPart); err != nil {
		return trace, fmt.Errorf("seed %d child run: %w", seed, err)
	}
	childRecords, _, err := store.Read(ctx, childType, childID)
	if err != nil || len(childRecords) != 4 || childRecords[3].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d child terminal entries=%d err=%v", seed, len(childRecords), err)
	}
	if transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("seed %d parent wakeup pending=%d", seed, transport.Dispatch.Pending())
	}
	if err := runOne(parentPart); err != nil {
		return trace, fmt.Errorf("seed %d parent resume: %w", seed, err)
	}
	if transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d final pending=%d", seed, transport.Dispatch.Pending())
	}
	parentRecords, _, err := store.Read(ctx, parentType, parentID)
	if err != nil || len(parentRecords) != 8 || parentRecords[5].Kind != journal.SignalConsumed || parentRecords[6].Kind != journal.StepCompleted || parentRecords[7].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d parent final entries=%d err=%v", seed, len(parentRecords), err)
	}
	wantChildCalls := 1
	if mode == "child_step_drop" || mode == "child_step_ack_lost" {
		wantChildCalls = 2
	}
	if parentCalls != 2 || childCalls != wantChildCalls {
		return trace, fmt.Errorf("seed %d handler calls parent=%d child=%d", seed, parentCalls, childCalls)
	}
	wantEffects := 1
	if mode == "child_step_drop" {
		wantEffects = 2
	}
	if effects != wantEffects {
		return trace, fmt.Errorf("seed %d mode=%s effects=%d", seed, mode, effects)
	}
	var parentOut, childOut wf.Outcome
	if err := json.Unmarshal(parentRecords[7].Payload, &parentOut); err != nil {
		return trace, err
	}
	if err := json.Unmarshal(childRecords[3].Payload, &childOut); err != nil {
		return trace, err
	}
	if string(parentOut.Result) != "42" || string(childOut.Result) != "42" {
		return trace, fmt.Errorf("seed %d results parent=%s child=%s", seed, parentOut.Result, childOut.Result)
	}
	signals := transport.SignalFor(parentType, parentID, "child_0")
	if len(signals) != 1 {
		return trace, fmt.Errorf("seed %d child notifications=%d", seed, len(signals))
	}
	transport.SetJournal(parentType, parentID, parentRecords)
	if report, err := CheckSignalWakeupLiveness(transport.SignalTransport); err != nil || report.Enabled != 0 {
		return trace, fmt.Errorf("seed %d child signal liveness=%+v err=%v", seed, report, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 2, Journals: 2, Entries: 12, Terminal: 2}) {
		return trace, fmt.Errorf("seed %d child retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_child_execution", Outcome: mode, Sequence: signals[0].Sequence, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerChildExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_CHILD_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerChildExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_CHILD_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerChildExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-child-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-child.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerChildExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker child replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-child-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerChildExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_CHILD_HELPER=1", "SIM_WORKER_CHILD_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker child trace changed across processes")
	}
}
