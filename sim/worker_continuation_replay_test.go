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

type continuationWorkerJournal struct {
	*JournalTransport
	mode  string
	fired bool
}

func (p *continuationWorkerJournal) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	var entry journal.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return 0, err
	}
	var request struct {
		Kind string `json:"kind"`
	}
	_ = json.Unmarshal(entry.Payload, &request)
	var done struct {
		Ref string `json:"result_ref"`
	}
	_ = json.Unmarshal(entry.Payload, &done)
	if !p.fired && ((strings.HasPrefix(p.mode, "request_") && entry.Kind == journal.StepRequested && request.Kind == "checkpoint") || (strings.HasPrefix(p.mode, "completion_") && entry.Kind == journal.StepCompleted && done.Ref != "")) {
		kind := DropBeforeCommit
		if strings.HasSuffix(p.mode, "ack_lost") {
			kind = LoseAckAfterCommit
		}
		if err := p.QueueFault(Fault{Kind: kind}); err != nil {
			return 0, err
		}
		p.fired = true
	}
	return p.JournalTransport.Publish(ctx, subject, data, expected)
}

type continuationWorkerSnapshots struct {
	*SnapshotReadTransport
	mode  string
	fired bool
}

func (p *continuationWorkerSnapshots) PutObject(ctx context.Context, name string, raw []byte) error {
	if !p.fired && ((strings.HasPrefix(p.mode, "frame_") && strings.HasPrefix(name, "step-result-")) || (strings.HasPrefix(p.mode, "archive_") && strings.HasPrefix(name, "snapshot-"))) {
		kind := DropBeforeCommit
		if strings.HasSuffix(p.mode, "ack_lost") {
			kind = LoseAckAfterCommit
		}
		if err := p.QueueWriteFault(SnapshotFault{"put_object", kind}); err != nil {
			return err
		}
		p.fired = true
	}
	return p.SnapshotReadTransport.PutObject(ctx, name, raw)
}
func (p *continuationWorkerSnapshots) PutBytes(ctx context.Context, name string, raw []byte) error {
	return p.PutObject(ctx, name, raw)
}
func (p *continuationWorkerSnapshots) GetBytes(ctx context.Context, name string) ([]byte, error) {
	return p.GetObject(ctx, name)
}

func runSeededWorkerContinuation(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_continuation"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "request_drop", "request_ack_lost", "completion_drop", "completion_ack_lost", "frame_drop", "frame_ack_lost", "archive_drop", "archive_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost", "signal_purge_drop", "signal_purge_ack_lost", "handoff_drop", "handoff_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	appendPort := &continuationWorkerJournal{JournalTransport: live, mode: mode}
	snapshots := &continuationWorkerSnapshots{SnapshotReadTransport: NewSnapshotReadTransport(schedule), mode: mode}
	snapshots.BindJournal(live)
	snapshots.BindSignals(transport.SignalTransport)
	store := journal.NewWithSnapshotPort(appendPort, live, snapshots)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	handle, err := c.Start(ctx, typ, id, []byte(`23`))
	if err != nil {
		return trace, err
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Signal(ctx, typ, id, "buffered", []byte(strconv.Itoa(i+1)), fmt.Sprintf("signal-%d", i)); err != nil {
			return trace, err
		}
	}
	switch mode {
	case "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost", "signal_purge_drop", "signal_purge_ack_lost":
		operation := "create_manifest"
		if strings.HasPrefix(mode, "purge_") {
			operation = "purge_journal"
		}
		if strings.HasPrefix(mode, "signal_purge_") {
			operation = "purge_signals"
		}
		kind := DropBeforeCommit
		if strings.HasSuffix(mode, "ack_lost") {
			kind = LoseAckAfterCommit
		}
		if err := snapshots.QueueWriteFault(SnapshotFault{operation, kind}); err != nil {
			return trace, err
		}
	case "handoff_drop", "handoff_ack_lost":
		kind := "drop_before_commit"
		if strings.HasSuffix(mode, "ack_lost") {
			kind = "lose_ack_after_commit"
		}
		if err := transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
			return trace, err
		}
	}
	var effects, initialCalls, stageCalls int
	var prefixKey, suffixKey string
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls++
		if err := c.SetState("total", 23); err != nil {
			return nil, err
		}
		if _, err := wf.RunOnce(c, "prefix", 23, func(_ context.Context, key string) (int, error) { effects++; prefixKey = key; return 46, nil }); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "next_v1", 45)
	}}
	stages := map[string]worker.ContinuationHandler{"next_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
		stageCalls++
		if string(input) != "23" || string(locals) != "45" {
			return nil, fmt.Errorf("continuation inputs")
		}
		var total int
		if found, err := c.GetState("total", &total); err != nil || !found || total != 23 {
			return nil, fmt.Errorf("continuation state %d: %v", total, err)
		}
		for i := 0; i < 2; i++ {
			value, err := wf.AwaitSignal(c, "buffered")
			if err != nil {
				return nil, err
			}
			if string(value) != strconv.Itoa(i+1) {
				return nil, fmt.Errorf("buffered signal %d=%q", i, value)
			}
		}
		if _, err := wf.RunOnce(c, "suffix", 23, func(_ context.Context, key string) (int, error) { effects++; suffixKey = key; return 46, nil }); err != nil {
			return nil, err
		}
		return json.RawMessage(`23`), nil
	}}
	w, err := worker.NewWithPorts("modeled-continuation", handlers, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: snapshots, Client: c}, worker.WithContinuations(typ, stages))
	if err != nil {
		return trace, err
	}
	runCtx, stopRun := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopRun)
	if err := w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("worker pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
		return trace, fmt.Errorf("records=%d err=%v", len(records), err)
	}
	if effects != 2 || stageCalls != 1 || prefixKey == "" || prefixKey == suffixKey {
		return trace, fmt.Errorf("mode=%s initial=%d stage=%d effects=%d keys=%s/%s", mode, initialCalls, stageCalls, effects, prefixKey, suffixKey)
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil || view.Snapshot.Runtime.Stage != "next_v1" {
		return trace, fmt.Errorf("checkpoint=%+v err=%v", view, err)
	}
	raw, err := retainedModelSnapshot(transport.StartTransport, live, outcomes)
	if err != nil {
		return trace, err
	}
	raw.Journals[identity.JournalSubject(typ, id)] = records
	report, err := integrity.CheckSnapshot(raw)
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		return trace, fmt.Errorf("integrity=%+v err=%v", report, err)
	}
	objects := make(map[string][]byte)
	snapshots.mu.Lock()
	for name, value := range snapshots.objects {
		objects[name] = bytes.Clone(value)
	}
	snapshots.mu.Unlock()
	journalBytes, err := json.Marshal(records)
	if err != nil {
		return trace, err
	}
	initialBefore, stageBefore := initialCalls, stageCalls
	var replayObservation wf.ReplayObservation
	replayed, err := wf.ReplayWithContinuations(journalBytes, func(ctx *wf.Context) (json.RawMessage, error) { return handlers[typ](ctx, json.RawMessage(`23`)) }, map[string]wf.ReplayContinuation[json.RawMessage]{"next_v1": func(ctx *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
		return stages["next_v1"](ctx, json.RawMessage(`23`), locals)
	}}, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: objects, Observation: &replayObservation})
	if err != nil || string(replayed) != "23" || effects != 2 || initialCalls != initialBefore+1 || stageCalls != stageBefore+1 || replayObservation.Continuations != 1 || replayObservation.PlayedSteps != replayObservation.RecordedSteps {
		return trace, fmt.Errorf("offline continuation result=%s effects=%d observation=%+v err=%v", replayed, effects, replayObservation, err)
	}
	if (strings.HasPrefix(mode, "request_") || strings.HasPrefix(mode, "completion_")) && !appendPort.fired {
		return trace, fmt.Errorf("journal fault was not exercised: %s", mode)
	}
	if (strings.HasPrefix(mode, "frame_") || strings.HasPrefix(mode, "archive_")) && !snapshots.fired {
		return trace, fmt.Errorf("object fault was not exercised: %s", mode)
	}
	snapshots.mu.Lock()
	remainingSnapshotFaults := len(snapshots.writeFaults)
	snapshots.mu.Unlock()
	transport.StartTransport.mu.Lock()
	remainingStartFaults := len(transport.StartTransport.faults)
	transport.StartTransport.mu.Unlock()
	if remainingSnapshotFaults != 0 || remainingStartFaults != 0 {
		return trace, fmt.Errorf("unused faults snapshot=%d handoff=%d", remainingSnapshotFaults, remainingStartFaults)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_continuation", Subject: identity.JournalSubject(typ, id), Sequence: tail, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerContinuationReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_CONTINUATION_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerContinuation(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_CONTINUATION_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerContinuation(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-continuation-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-continuation.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runSeededWorkerContinuation(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker continuation replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 17 {
		t.Fatalf("covered %d/17 worker continuation modes", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-continuation-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerContinuationReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_CONTINUATION_HELPER=1", "SIM_WORKER_CONTINUATION_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker continuation trace changed across processes")
	}
}
