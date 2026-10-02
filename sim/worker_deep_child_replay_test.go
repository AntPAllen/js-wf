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
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

func runSeededWorkerDeepChild(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_deep_child_1"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "grandchild_step_drop", "grandchild_step_ack_lost", "notify_signal_ack_lost", "notify_enqueue_ack_lost", "consumer_leader_changed"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const parentType, parentID, childType, grandType = "parent", "deep", "child", "grandchild"
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journalTransport := NewJournalTransport(schedule)
	appendPort := &faultingExecutionJournal{JournalTransport: journalTransport}
	store := journal.NewWithPorts(appendPort, journalTransport)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var parentCalls, childCalls int
	var effects [2]int
	w, err := worker.NewWithPorts("modeled-deep-worker", map[string]worker.Handler{
		parentType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			parentCalls++
			promise, err := wf.CallAsync(c, childType, []byte(`null`))
			if err != nil {
				return nil, err
			}
			return wf.AwaitPromise(c, promise)
		},
		childType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			childCalls++
			promises := make([]wf.Promise, 2)
			for i := range promises {
				promise, err := wf.CallAsync(c, grandType, json.RawMessage(strconv.Itoa(i+3)))
				if err != nil {
					return nil, err
				}
				promises[i] = promise
			}
			var sum int
			for _, promise := range promises {
				value, err := wf.AwaitPromise(c, promise)
				if err != nil {
					return nil, err
				}
				var n int
				if err := json.Unmarshal(value, &n); err != nil {
					return nil, err
				}
				sum += n
			}
			return json.Marshal(sum)
		},
		grandType: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			var n int
			if err := json.Unmarshal(input, &n); err != nil || n < 3 || n > 4 {
				return nil, fmt.Errorf("invalid leaf input %s: %v", input, err)
			}
			value, err := wf.Run(c, "leaf", n, func(context.Context) (int, error) {
				effects[n-3]++
				return n, nil
			})
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
	runOne := func(subject string) error {
		part, err := strconv.ParseUint(strings.TrimPrefix(subject, "wf.run."), 10, 32)
		if err != nil || !strings.HasPrefix(subject, "wf.run.") {
			return fmt.Errorf("invalid run subject %q: %v", subject, err)
		}
		runCtx, stopRun := context.WithCancel(ctx)
		transport.Dispatch.StopAfterNextAck(stopRun)
		return w.RunPartitionWithTransport(runCtx, uint32(part), transport.Dispatch)
	}
	if subjects := transport.Dispatch.PendingSubjects(); len(subjects) != 1 {
		return trace, fmt.Errorf("seed %d initial run subjects=%v", seed, subjects)
	} else if err := runOne(subjects[0]); err != nil {
		return trace, fmt.Errorf("seed %d parent first run: %w", seed, err)
	}
	parentFirst, _, err := store.Read(ctx, parentType, parentID)
	if err != nil || len(parentFirst) != 5 || parentFirst[4].Kind != journal.Suspended {
		return trace, fmt.Errorf("seed %d parent suspension entries=%d err=%v", seed, len(parentFirst), err)
	}
	var childRequest struct {
		Kind    string `json:"kind"`
		ChildID string `json:"child_id"`
	}
	if err := json.Unmarshal(parentFirst[1].Payload, &childRequest); err != nil || childRequest.Kind != "call_async" || childRequest.ChildID == "" {
		return trace, fmt.Errorf("seed %d child request=%+v err=%v", seed, childRequest, err)
	}
	childID := childRequest.ChildID
	if subjects := transport.Dispatch.PendingSubjects(); len(subjects) != 1 {
		return trace, fmt.Errorf("seed %d child run subjects=%v", seed, subjects)
	} else if err := runOne(subjects[0]); err != nil {
		return trace, fmt.Errorf("seed %d child first run: %w", seed, err)
	}
	childFirst, _, err := store.Read(ctx, childType, childID)
	if err != nil || len(childFirst) != 7 || childFirst[6].Kind != journal.Suspended {
		return trace, fmt.Errorf("seed %d child suspension entries=%d err=%v", seed, len(childFirst), err)
	}
	var grandchildIDs [2]string
	for i, index := range []int{1, 3} {
		var request struct {
			Kind    string `json:"kind"`
			ChildID string `json:"child_id"`
		}
		if err := json.Unmarshal(childFirst[index].Payload, &request); err != nil || request.Kind != "call_async" || request.ChildID == "" {
			return trace, fmt.Errorf("seed %d grandchild %d request=%+v err=%v", seed, i, request, err)
		}
		grandchildIDs[i] = request.ChildID
	}
	if grandchildIDs[0] == grandchildIDs[1] || transport.Dispatch.Pending() != 2 {
		return trace, fmt.Errorf("seed %d grandchildren=%v pending=%d", seed, grandchildIDs, transport.Dispatch.Pending())
	}
	if mode == "grandchild_step_drop" || mode == "grandchild_step_ack_lost" {
		appendPort.subject = identity.JournalSubject(grandType, grandchildIDs[0])
		appendPort.fault = DropBeforeCommit
		if mode == "grandchild_step_ack_lost" {
			appendPort.fault = LoseAckAfterCommit
		}
	}
	if mode == "notify_signal_ack_lost" {
		if err := transport.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	if mode == "notify_enqueue_ack_lost" {
		if err := transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_leader_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	for turn := 0; transport.Dispatch.Pending() != 0 && turn < 32; turn++ {
		subjects := transport.Dispatch.PendingSubjects()
		subject, err := schedule.Choose(subjects)
		if err != nil {
			return trace, err
		}
		if err := runOne(subject); err != nil {
			return trace, fmt.Errorf("seed %d turn %d subject %s: %w", seed, turn, subject, err)
		}
	}
	if transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d undrained runs=%d", seed, transport.Dispatch.Pending())
	}
	parentRecords, _, err := store.Read(ctx, parentType, parentID)
	if err != nil || len(parentRecords) == 0 || parentRecords[len(parentRecords)-1].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d parent terminal entries=%d err=%v", seed, len(parentRecords), err)
	}
	childRecords, _, err := store.Read(ctx, childType, childID)
	if err != nil || len(childRecords) == 0 || childRecords[len(childRecords)-1].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d child terminal entries=%d err=%v", seed, len(childRecords), err)
	}
	var parentOut, childOut wf.Outcome
	if err := json.Unmarshal(parentRecords[len(parentRecords)-1].Payload, &parentOut); err != nil {
		return trace, err
	}
	if err := json.Unmarshal(childRecords[len(childRecords)-1].Payload, &childOut); err != nil {
		return trace, err
	}
	if string(parentOut.Result) != "7" || string(childOut.Result) != "7" || parentCalls < 2 || childCalls < 2 {
		return trace, fmt.Errorf("seed %d parent=%s child=%s calls=%d/%d", seed, parentOut.Result, childOut.Result, parentCalls, childCalls)
	}
	for i, grandchildID := range grandchildIDs {
		records, _, err := store.Read(ctx, grandType, grandchildID)
		if err != nil || len(records) != 4 || records[3].Kind != journal.Completed {
			return trace, fmt.Errorf("seed %d grandchild %d entries=%d err=%v", seed, i, len(records), err)
		}
		var outcome wf.Outcome
		if err := json.Unmarshal(records[3].Payload, &outcome); err != nil || string(outcome.Result) != strconv.Itoa(i+3) {
			return trace, fmt.Errorf("seed %d grandchild %d result=%s err=%v", seed, i, outcome.Result, err)
		}
	}
	wantEffects := [2]int{1, 1}
	if mode == "grandchild_step_drop" {
		wantEffects[0] = 2
	}
	if effects != wantEffects {
		return trace, fmt.Errorf("seed %d mode=%s effects=%v want=%v", seed, mode, effects, wantEffects)
	}
	transport.SetJournal(parentType, parentID, parentRecords)
	transport.SetJournal(childType, childID, childRecords)
	if report, err := CheckSignalWakeupLiveness(transport.SignalTransport); err != nil || report.Enabled != 0 {
		return trace, fmt.Errorf("seed %d child wakeup liveness=%+v err=%v", seed, report, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journalTransport, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	wantEntries := len(parentRecords) + len(childRecords) + 8
	if err != nil || report != (integrity.Report{Invocations: 4, Journals: 4, Entries: wantEntries, Terminal: 4}) {
		return trace, fmt.Errorf("seed %d retained check=%+v want_entries=%d err=%v", seed, report, wantEntries, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_deep_child", Outcome: mode, Sequence: uint64(report.Terminal), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerDeepChildReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_DEEP_CHILD_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerDeepChild(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_DEEP_CHILD_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededWorkerDeepChild(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-deep-child-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-deep-child.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerDeepChild(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d deep child replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-deep-child-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerDeepChildReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_DEEP_CHILD_HELPER=1", "SIM_WORKER_DEEP_CHILD_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker deep child trace changed across processes")
	}
}
