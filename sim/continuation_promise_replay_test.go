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
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

type continuationPromiseSnapshots struct {
	*SnapshotReadTransport
	blobs *ResultBlobTransport
}

func (p *continuationPromiseSnapshots) GetObject(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "step-result-") || strings.HasPrefix(name, "terminal-result-") {
		return p.blobs.GetBytes(ctx, name)
	}
	return p.SnapshotReadTransport.GetObject(ctx, name)
}

type continuationPromiseGuard struct {
	journal.SnapshotWritePort
	frames, archives int
}

func (p *continuationPromiseGuard) GetObject(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "snapshot-") {
		p.archives++
		return nil, fmt.Errorf("forbidden archive read")
	}
	if strings.HasPrefix(name, "step-result-") {
		p.frames++
	}
	return p.SnapshotWritePort.GetObject(ctx, name)
}

type continuationPromiseResults struct {
	*ResultBlobTransport
	childObject        string
	reads, unavailable int
}

func (p *continuationPromiseResults) GetBytes(ctx context.Context, name string) ([]byte, error) {
	if name == p.childObject {
		p.reads++
		if p.unavailable > 0 {
			p.unavailable--
			if err := p.QueueReadFault(name); err != nil {
				return nil, err
			}
		}
	}
	return p.ResultBlobTransport.GetBytes(ctx, name)
}

func runSeededContinuationPromise(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("continuation_promise"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "resume_read_once", "resume_read_twice", "corrupt_child", "terminal_drop", "terminal_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ, id, childType = "parent", "checkpoint-promise", "child"
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	blobs := NewResultBlobTransport(schedule)
	snapshots := &continuationPromiseSnapshots{SnapshotReadTransport: NewSnapshotReadTransport(schedule), blobs: blobs}
	snapshots.BindJournal(live)
	snapshots.BindSignals(transport.SignalTransport)
	store := journal.NewWithSnapshotPort(live, live, snapshots)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	payload, _ := json.Marshal(strings.Repeat("x", wf.MaxInlineTerminal))
	var initialCalls, childCalls int
	var promise wf.Promise
	handlers := map[string]worker.Handler{
		typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			initialCalls++
			var err error
			promise, err = wf.CallAsync(c, childType, []byte(`null`))
			if err != nil {
				return nil, err
			}
			value, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(value, payload) {
				return nil, fmt.Errorf("initial promise changed")
			}
			return nil, wf.Continue(c, "finish_v1", promise)
		},
		childType: func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) { childCalls++; return payload, nil },
	}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		var saved wf.Promise
		if err := json.Unmarshal(locals, &saved); err != nil {
			return nil, err
		}
		if saved != promise {
			return nil, fmt.Errorf("promise identity changed")
		}
		value, err := wf.AwaitPromise(c, saved)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(value, payload) {
			return nil, fmt.Errorf("restored promise changed")
		}
		value[0] = '!'
		again, err := wf.AwaitPromise(c, saved)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(again, payload) {
			return nil, fmt.Errorf("cached promise alias")
		}
		return json.RawMessage(strconv.Itoa(len(again))), nil
	}}
	ports := worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: blobs, Client: c}
	first, err := worker.NewWithPorts("promise-first", handlers, ports, worker.WithContinuations(typ, stages))
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	runOne := func(part uint32) error {
		runCtx, stopRun := context.WithCancel(ctx)
		defer stopRun()
		transport.Dispatch.StopAfterNextAck(stopRun)
		return first.RunPartitionWithTransport(runCtx, part, transport.Dispatch)
	}
	parentPart := identity.Partition(typ, id, provision.Partitions)
	if err := runOne(parentPart); err != nil {
		return trace, err
	}
	if promise.ChildID == "" {
		return trace, fmt.Errorf("missing child")
	}
	if strings.HasPrefix(mode, "terminal_") {
		kind := DropBeforeCommit
		if mode == "terminal_ack_lost" {
			kind = LoseAckAfterCommit
		}
		if err := blobs.QueueNamedFault("terminal-result-", kind); err != nil {
			return trace, err
		}
	}
	if err := runOne(identity.Partition(childType, promise.ChildID, provision.Partitions)); err != nil {
		return trace, err
	}
	if err := runOne(parentPart); err != nil {
		return trace, err
	}
	if err := runOne(parentPart); err != nil {
		return trace, err
	}
	before, _, err := store.Read(ctx, typ, id)
	if err != nil || len(before) == 0 || before[len(before)-1].Kind != journal.Suspended || string(before[len(before)-1].Payload) != `{"waiting_on":"signal:gate"}` {
		return trace, fmt.Errorf("before=%v err=%v", before, err)
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		return trace, fmt.Errorf("frame=%+v err=%v", view, err)
	}
	var frame struct {
		PromiseOutcomes map[string]json.RawMessage `json:"promise_outcomes"`
	}
	if err := json.Unmarshal(view.Frame, &frame); err != nil {
		return trace, err
	}
	var childOutcome wf.Outcome
	if err := json.Unmarshal(frame.PromiseOutcomes[promise.SignalName], &childOutcome); err != nil || childOutcome.ResultRef == "" || childOutcome.ResultHash == "" || len(childOutcome.Result) != 0 || len(view.Frame) >= len(payload) {
		return trace, fmt.Errorf("frame promise=%+v framebytes=%d err=%v", childOutcome, len(view.Frame), err)
	}
	guard := &continuationPromiseGuard{SnapshotWritePort: snapshots}
	results := &continuationPromiseResults{ResultBlobTransport: blobs, childObject: childOutcome.ResultRef}
	if mode == "resume_read_once" {
		results.unavailable = 1
	}
	if mode == "resume_read_twice" {
		results.unavailable = 2
	}
	wantReads := results.unavailable + 1
	if mode == "corrupt_child" {
		blobs.mu.Lock()
		blobs.objects[childOutcome.ResultRef] = []byte(`"bad"`)
		blobs.mu.Unlock()
		schedule.RecordTransport(TransportEvent{Operation: "corrupt_promise_blob", Subject: childOutcome.ResultRef, DataSHA256: digest([]byte(`"bad"`)), Outcome: "corrupt", AtMillis: schedule.NowMillis()})
	}
	ports.Journal = journal.NewWithSnapshotPort(live, live, guard)
	ports.ResultBlobs = results
	second, err := worker.NewWithPorts("promise-second", handlers, ports, worker.WithContinuations(typ, stages))
	if err != nil {
		return trace, err
	}
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		return trace, err
	}
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	transport.Dispatch.StopWhenDrained(stopRun)
	if err := second.RunPartitionWithTransport(runCtx, parentPart, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(records) == 0 {
		return trace, fmt.Errorf("records=%v err=%v", records, err)
	}
	last := records[len(records)-1]
	var outcome wf.Outcome
	if err := json.Unmarshal(last.Payload, &outcome); err != nil {
		return trace, err
	}
	if mode == "corrupt_child" {
		if last.Kind != journal.Failed || outcome.Error != wf.ErrCorruptJournal.Error() {
			return trace, fmt.Errorf("corrupt outcome=%+v kind=%s", outcome, last.Kind)
		}
	} else if last.Kind != journal.Completed || outcome.Error != "" || string(outcome.Result) != strconv.Itoa(len(payload)) {
		return trace, fmt.Errorf("outcome=%+v kind=%s", outcome, last.Kind)
	}
	wantChild := 1
	if strings.HasPrefix(mode, "terminal_") {
		wantChild = 2
	}
	if initialCalls != 2 || childCalls != wantChild || guard.archives != 0 || guard.frames == 0 || results.reads != wantReads || results.unavailable != 0 {
		return trace, fmt.Errorf("mode=%s calls=%d/%d reads=%d/%d child_reads=%d want=%d unavailable=%d", mode, initialCalls, childCalls, guard.archives, guard.frames, results.reads, wantReads, results.unavailable)
	}
	calls, consumed, awaits := 0, 0, 0
	for _, r := range records {
		if r.Kind == journal.SignalConsumed {
			var data struct{ Name string }
			if err := json.Unmarshal(r.Payload, &data); err != nil {
				return trace, err
			}
			if data.Name == promise.SignalName {
				consumed++
			}
		}
		if r.Kind == journal.StepRequested {
			var req struct{ Kind, Name string }
			if err := json.Unmarshal(r.Payload, &req); err != nil {
				return trace, err
			}
			if req.Kind == "call_async" {
				calls++
			}
			if req.Name == promise.SignalName {
				awaits++
			}
		}
	}
	if calls != 1 || consumed != 1 || awaits != 2 {
		return trace, fmt.Errorf("duplicate promise steps: call=%d consumed=%d named=%d", calls, consumed, awaits)
	}
	raw, err := retainedModelSnapshot(transport.StartTransport, live, outcomes)
	if err != nil {
		return trace, err
	}
	raw.Journals[identity.JournalSubject(typ, id)] = records
	report, err := integrity.CheckSnapshot(raw)
	if err != nil || report.Invocations != 2 || report.Terminal != 2 {
		return trace, fmt.Errorf("audit=%+v err=%v", report, err)
	}
	blobs.mu.Lock()
	remaining := blobs.namedFault != "" || blobs.readFaultPrefix != ""
	blobs.mu.Unlock()
	if remaining {
		return trace, fmt.Errorf("unconsumed blob fault")
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_continuation_promise", Subject: identity.JournalSubject(typ, id), Sequence: tail, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededContinuationPromiseReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_PROMISE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededContinuationPromise(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_PROMISE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]bool{}
	firstSeeds := map[string]int64{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededContinuationPromise(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-continuation-promise-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-continuation-promise.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		mode := generated.Decisions[0].Chosen
		if !observed[mode] {
			firstSeeds[mode] = seed
		}
		observed[mode] = true
		if seed <= 10 {
			replayed, err := runSeededContinuationPromise(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d continuation promise replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 6 {
		t.Fatalf("covered %d/6 continuation promise modes", len(observed))
	}
	for _, mode := range []string{"clean", "resume_read_once", "resume_read_twice", "corrupt_child", "terminal_drop", "terminal_ack_lost"} {
		t.Logf("mode=%s first_seed=%d", mode, firstSeeds[mode])
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("continuation-promise-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationPromiseReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_PROMISE_HELPER=1", "SIM_CONTINUATION_PROMISE_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("continuation promise trace changed across processes")
	}
}
