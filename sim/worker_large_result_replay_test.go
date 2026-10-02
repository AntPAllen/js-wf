package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	"github.com/nats-io/nats.go/jetstream"
)

type resultBudgetPort struct {
	*ResultBlobTransport
	mode        string
	fired       bool
	budgetError error
}

func (p *resultBudgetPort) missing(ctx context.Context, name string, commit func() error) error {
	p.fired = true
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 15*time.Second {
		p.budgetError = fmt.Errorf("result object request inherits delivery lifetime instead of a 15s budget")
		return fmt.Errorf("%w: %w", worker.ErrResultBlobUnknown, p.budgetError)
	}
	if strings.HasSuffix(p.mode, "ack_lost") {
		if err := commit(); err != nil {
			return err
		}
	}
	if err := p.schedule.AdvanceMillis(15000); err != nil {
		return err
	}
	p.schedule.RecordTransport(TransportEvent{Operation: "result_missing_reply", Subject: name, Outcome: p.mode})
	// Return the raw error deliberately: worker policy must preserve retryability.
	return context.DeadlineExceeded
}
func (p *resultBudgetPort) PutBytes(ctx context.Context, name string, data []byte) error {
	if !p.fired && (strings.HasPrefix(p.mode, "step_put_") && strings.HasPrefix(name, "step-result-") || strings.HasPrefix(p.mode, "terminal_put_") && strings.HasPrefix(name, "terminal-result-")) {
		return p.missing(ctx, name, func() error { return p.ResultBlobTransport.PutBytes(ctx, name, data) })
	}
	return p.ResultBlobTransport.PutBytes(ctx, name, data)
}
func (p *resultBudgetPort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	if !p.fired && p.mode == "step_get_drop" {
		return nil, p.missing(ctx, name, func() error { return nil })
	}
	return p.ResultBlobTransport.GetBytes(ctx, name)
}
func runSeededWorkerResultBudget(seed int64, replay *Trace) (Trace, error) {
	return runSeededWorkerLargeResultMode(seed, replay, true)
}
func runSeededWorkerLargeResult(seed int64, replay *Trace) (Trace, error) {
	return runSeededWorkerLargeResultMode(seed, replay, false)
}

func runSeededWorkerLargeResultMode(seed int64, replay *Trace, budget bool) (trace Trace, runErr error) {
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
	workload := "worker_large_result"
	if budget {
		workload = "worker_result_response_budget"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	modes := []string{"clean", "step_blob_drop", "step_blob_ack_lost", "step_blob_read_unavailable", "terminal_blob_drop", "terminal_blob_ack_lost", "step_completion_ack_lost", "outcome_ack_lost", "consumer_leader_changed"}
	if budget {
		modes = []string{"step_put_drop", "step_put_ack_lost", "step_get_drop", "terminal_put_drop", "terminal_put_ack_lost"}
	}
	mode, err := schedule.Choose(modes)
	if err != nil {
		return trace, err
	}
	lifetime := 5 * time.Second
	if budget {
		lifetime = 5 * time.Minute
	}
	ctx, stop := context.WithTimeout(context.Background(), lifetime)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journals := NewJournalTransport(schedule)
	appendPort := &faultingExecutionJournal{JournalTransport: journals}
	if mode == "step_completion_ack_lost" || mode == "step_blob_read_unavailable" || mode == "step_get_drop" {
		appendPort.subject = identity.JournalSubject(typ, id)
		appendPort.fault = LoseAckAfterCommit
	}
	store := journal.NewWithPorts(appendPort, journals)
	leaseKV := NewKVTransport(schedule, 30*time.Second)
	leasing := lease.NewWithKVPort(leaseKV)
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
	blobPort := worker.ResultBlobPort(blobs)
	boundedBlobs := &resultBudgetPort{ResultBlobTransport: blobs, mode: mode}
	if budget {
		blobPort = boundedBlobs
	}
	var calls, effects int
	var effectKeys []string
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls++
		var got string
		var err error
		if budget {
			got, err = wf.RunOnce(c, "large", 1, func(_ context.Context, key string) (string, error) {
				effects++
				effectKeys = append(effectKeys, key)
				return value, nil
			})
		} else {
			got, err = wf.Run(c, "large", 1, func(context.Context) (string, error) { effects++; return value, nil })
		}
		if err != nil {
			return nil, err
		}
		return json.Marshal(got)
	}}
	w, err := worker.NewWithPorts("modeled-large-result-worker", handlers, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: blobPort, Client: c})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	var prefixAtTimeout []journal.Record
	if budget {
		if mode == "step_get_drop" {
			initial, stopInitial := context.WithCancel(ctx)
			transport.Dispatch.StopAfterNextNak(stopInitial)
			if err := w.RunPartitionWithTransport(initial, 0, transport.Dispatch); err != nil {
				return trace, err
			}
			if boundedBlobs.fired || effects != 1 {
				return trace, fmt.Errorf("result read cut not prepared")
			}
			if err := schedule.AdvanceMillis(1000); err != nil {
				return trace, err
			}
		}
		attempt, stopAttempt := context.WithCancel(ctx)
		transport.Dispatch.StopAfterNextNak(stopAttempt)
		if err := w.RunPartitionWithTransport(attempt, 0, transport.Dispatch); err != nil {
			return trace, err
		}
		if boundedBlobs.budgetError != nil {
			return trace, boundedBlobs.budgetError
		}
		wantTime := int64(15000)
		if mode == "step_get_drop" {
			wantTime += 1000
		}
		if !boundedBlobs.fired || schedule.NowMillis() != wantTime || transport.Dispatch.Pending() == 0 {
			return trace, fmt.Errorf("result cut fired=%v time=%d pending=%d", boundedBlobs.fired, schedule.NowMillis(), transport.Dispatch.Pending())
		}
		if _, err := leaseKV.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("result timeout retained lease: %v", err)
		}
		if _, err := outcomes.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("result timeout produced terminal outcome: %v", err)
		}
		prefixAtTimeout, _, err = store.Read(ctx, typ, id)
		if err != nil {
			return trace, err
		}
		if err := schedule.AdvanceMillis(1000); err != nil {
			return trace, err
		}
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
	if mode == "step_blob_drop" || mode == "step_blob_ack_lost" || strings.HasPrefix(mode, "step_put_") {
		wantEffects = 2
	}
	if budget {
		for _, key := range effectKeys {
			if key == "" || key != effectKeys[0] {
				return trace, fmt.Errorf("RunOnce key changed across result timeout")
			}
		}
	}
	if effects != wantEffects {
		return trace, fmt.Errorf("seed %d mode=%s effects=%d want=%d", seed, mode, effects, wantEffects)
	}
	wantCalls := 1
	if mode == "step_blob_drop" || mode == "step_blob_ack_lost" || mode == "terminal_blob_drop" || mode == "terminal_blob_ack_lost" || mode == "step_completion_ack_lost" || strings.HasPrefix(mode, "step_put_") || strings.HasPrefix(mode, "terminal_put_") {
		wantCalls = 2
	}
	if mode == "step_blob_read_unavailable" || mode == "step_get_drop" {
		wantCalls = 3
	}
	if calls != wantCalls {
		return trace, fmt.Errorf("seed %d mode=%s calls=%d want=%d", seed, mode, calls, wantCalls)
	}
	if budget && (len(records) < len(prefixAtTimeout) || !reflect.DeepEqual(prefixAtTimeout, records[:len(prefixAtTimeout)])) {
		return trace, fmt.Errorf("recorded result prefix changed")
	}
	if budget && records[len(records)-1].Epoch <= records[0].Epoch {
		return trace, fmt.Errorf("result retry did not advance fencing epoch")
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
	if budget {
		journalBytes, err := json.Marshal(records)
		if err != nil {
			return trace, err
		}
		objects := map[string][]byte{completed.ResultRef: bytes.Clone(result), out.ResultRef: bytes.Clone(result)}
		beforeEffects, beforeCalls := effects, calls
		replayed, err := wf.Replay(journalBytes, func(c *wf.Context) (json.RawMessage, error) { return handlers[typ](c, nil) }, wf.ReplayOptions{Type: typ, ID: id, InvSeq: out.InvSeq, Objects: objects})
		if err != nil || !bytes.Equal(replayed, result) || effects != beforeEffects || calls != beforeCalls+1 {
			return trace, fmt.Errorf("offline result replay effects=%d result_bytes=%d err=%v", effects, len(replayed), err)
		}
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
	for seed := range seededSchedules(t) {
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
