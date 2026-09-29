package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func runSeededWorkerLargeResult(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_large_result"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "step_blob_drop", "step_blob_ack_lost", "step_blob_read_unavailable", "terminal_blob_drop", "terminal_blob_ack_lost", "step_completion_ack_lost", "outcome_ack_lost", "consumer_leader_changed"})
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
	if mode == "step_completion_ack_lost" || mode == "step_blob_read_unavailable" {
		appendPort.subject = identity.JournalSubject(typ, id)
		appendPort.fault = LoseAckAfterCommit
	}
	store := journal.NewWithPorts(appendPort, journals)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	blobs := NewResultBlobTransport(schedule)
	if mode == "step_blob_read_unavailable" {
		if err := blobs.QueueReadFault("step-result-"); err != nil {
			return trace, err
		}
	}
	if mode == "step_blob_drop" || mode == "step_blob_ack_lost" {
		fault := DropBeforeCommit
		if mode == "step_blob_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := blobs.QueueNamedFault("step-result-", fault); err != nil {
			return trace, err
		}
	}
	if mode == "terminal_blob_drop" || mode == "terminal_blob_ack_lost" {
		fault := DropBeforeCommit
		if mode == "terminal_blob_ack_lost" {
			fault = LoseAckAfterCommit
		}
		if err := blobs.QueueNamedFault("terminal-result-", fault); err != nil {
			return trace, err
		}
	}
	if mode == "outcome_ack_lost" {
		if err := outcomes.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_leader_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	value := strings.Repeat("x", wf.MaxInlineResult)
	result, _ := json.Marshal(value)
	var calls, effects int
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	w, err := worker.NewWithPorts("modeled-large-result-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls++
		got, err := wf.Run(c, "large", 1, func(context.Context) (string, error) { effects++; return value, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(got)
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: blobs, Client: c})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	runCtx, stopRun := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopRun)
	if err := w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d large worker pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 4 || records[3].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d large journal entries=%d err=%v", seed, len(records), err)
	}
	wantEffects := 1
	if mode == "step_blob_drop" || mode == "step_blob_ack_lost" {
		wantEffects = 2
	}
	if effects != wantEffects {
		return trace, fmt.Errorf("seed %d mode=%s effects=%d want=%d", seed, mode, effects, wantEffects)
	}
	wantCalls := 1
	if mode == "step_blob_drop" || mode == "step_blob_ack_lost" || mode == "terminal_blob_drop" || mode == "terminal_blob_ack_lost" || mode == "step_completion_ack_lost" {
		wantCalls = 2
	}
	if mode == "step_blob_read_unavailable" {
		wantCalls = 3
	}
	if calls != wantCalls {
		return trace, fmt.Errorf("seed %d mode=%s calls=%d want=%d", seed, mode, calls, wantCalls)
	}
	var completed struct {
		ResultRef  string `json:"result_ref"`
		ResultHash string `json:"result_hash"`
	}
	if err := json.Unmarshal(records[2].Payload, &completed); err != nil || completed.ResultRef == "" || completed.ResultHash == "" {
		return trace, fmt.Errorf("seed %d step blob completion=%+v err=%v", seed, completed, err)
	}
	var out wf.Outcome
	if err := json.Unmarshal(records[3].Payload, &out); err != nil || out.ResultRef == "" || out.ResultHash == "" || len(out.Result) != 0 {
		return trace, fmt.Errorf("seed %d terminal blob outcome=%+v err=%v", seed, out, err)
	}
	if blobs.Count() != 2 || !strings.HasPrefix(completed.ResultRef, "step-result-") || !strings.HasPrefix(out.ResultRef, "terminal-result-") {
		return trace, fmt.Errorf("seed %d blob count=%d refs=%s,%s", seed, blobs.Count(), completed.ResultRef, out.ResultRef)
	}
	digest := sha256.Sum256(result)
	wantHash := hex.EncodeToString(digest[:])
	if completed.ResultHash != wantHash || out.ResultHash != wantHash {
		return trace, fmt.Errorf("seed %d blob hashes step=%s terminal=%s want=%s", seed, completed.ResultHash, out.ResultHash, wantHash)
	}
	stepData, err := blobs.GetBytes(ctx, completed.ResultRef)
	if err != nil || !bytes.Equal(stepData, result) {
		return trace, fmt.Errorf("seed %d step blob mismatch: %v", seed, err)
	}
	finalData, err := out.ResultBytes(ctx, blobs.GetBytes)
	if err != nil || !bytes.Equal(finalData, result) {
		return trace, fmt.Errorf("seed %d terminal blob mismatch: %v", seed, err)
	}
	retained, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(retained.Value, records[3].Payload) {
		return trace, fmt.Errorf("seed %d terminal KV mismatch: %v", seed, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d large retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_large_result", Outcome: mode, Sequence: uint64(blobs.Count()), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerLargeResultReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_LARGE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerLargeResult(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_LARGE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerLargeResult(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-large-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-large.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerLargeResult(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker large replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-large-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerLargeResultReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_LARGE_HELPER=1", "SIM_WORKER_LARGE_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker large trace changed across processes")
	}
}
