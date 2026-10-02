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

type panicBudgetJournal struct {
	*JournalTransport
	mode         string
	armed, fired bool
}

func (p *panicBudgetJournal) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	var entry journal.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return 0, err
	}
	target := journal.Attempt
	if strings.HasPrefix(p.mode, "failed_") {
		target = journal.Failed
	}
	if p.armed && !p.fired && entry.Kind == target && (strings.HasPrefix(p.mode, "attempt_") || strings.HasPrefix(p.mode, "failed_")) {
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

type panicBudgetFrameReader struct {
	*continuationWorkerSnapshots
	frames, archives int
}

func (p *panicBudgetFrameReader) GetObject(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "snapshot-") {
		p.archives++
		return nil, fmt.Errorf("archive read forbidden: %s", name)
	}
	if strings.HasPrefix(name, "step-result-") {
		p.frames++
	}
	return p.continuationWorkerSnapshots.GetObject(ctx, name)
}
func (p *panicBudgetFrameReader) GetBytes(ctx context.Context, name string) ([]byte, error) {
	return p.GetObject(ctx, name)
}

func runSeededContinuationPanic(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("continuation_panic"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "attempt_drop", "attempt_ack_lost", "failed_drop", "failed_ack_lost", "frame_drop", "frame_ack_lost", "archive_drop", "archive_ack_lost"})
	if err != nil {
		return trace, err
	}
	prefixChoice, err := schedule.Choose([]string{"1", "2"})
	if err != nil {
		return trace, err
	}
	middleChoice, err := schedule.Choose([]string{"1", "2"})
	if err != nil {
		return trace, err
	}
	prefixPanics, _ := strconv.Atoi(prefixChoice)
	middlePanics, _ := strconv.Atoi(middleChoice)
	budget := prefixPanics + middlePanics + 1
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	appendPort := &panicBudgetJournal{JournalTransport: live, mode: mode}
	snapshots := &continuationWorkerSnapshots{SnapshotReadTransport: NewSnapshotReadTransport(schedule), mode: mode}
	snapshots.BindJournal(live)
	snapshots.BindSignals(transport.SignalTransport)
	store := journal.NewWithSnapshotPort(appendPort, live, snapshots)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	var initialCalls, middleCalls, finishCalls int
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls++
		if initialCalls <= prefixPanics {
			panic("prefix poison")
		}
		if err := c.SetState("value", 10); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", 10)
	}}
	stages := map[string]worker.ContinuationHandler{
		"middle_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			middleCalls++
			var value int
			found, err := c.GetState("value", &value)
			if err != nil {
				return nil, err
			}
			if !found || value != 10 || string(locals) != "10" {
				return nil, fmt.Errorf("bad middle state")
			}
			if middleCalls <= middlePanics {
				panic("middle poison")
			}
			if err := c.SetState("value", 20); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", 20)
		},
		"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			finishCalls++
			var value int
			found, err := c.GetState("value", &value)
			if err != nil {
				return nil, err
			}
			if !found || value != 20 || string(locals) != "20" {
				return nil, fmt.Errorf("bad finish state")
			}
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			panic("final poison")
		},
	}
	ports := worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: snapshots, Client: c}
	first, err := worker.NewWithPorts("panic-first", handlers, ports, worker.WithContinuations(typ, stages), worker.WithMaxPanicAttempts(budget))
	if err != nil {
		return trace, err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	transport.Dispatch.StopWhenDrained(stopFirst)
	if err := first.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("first pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Suspended || string(records[len(records)-1].Payload) != `{"waiting_on":"signal:gate"}` {
		return trace, fmt.Errorf("before restart records=%v err=%v", records, err)
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		return trace, fmt.Errorf("checkpoint=%+v err=%v", view, err)
	}
	runtime := view.Snapshot.Runtime
	_, info, err := wf.NewCheckpointContext(ctx, nil, nil, view.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: handle.InvSeq, Index: runtime.Index, Epoch: runtime.Epoch, Hash: runtime.SHA256})
	if err != nil || info.Stage != "finish_v1" || int(info.PanicAttempts) != budget-1 {
		return trace, fmt.Errorf("baseline=%+v want=%d err=%v", info, budget-1, err)
	}
	initialBefore, middleBefore, finishBefore := initialCalls, middleCalls, finishCalls
	guard := &panicBudgetFrameReader{continuationWorkerSnapshots: snapshots}
	ports.Journal = journal.NewWithSnapshotPort(appendPort, live, guard)
	ports.ResultBlobs = guard
	second, err := worker.NewWithPorts("panic-second", handlers, ports, worker.WithContinuations(typ, stages), worker.WithMaxPanicAttempts(budget))
	if err != nil {
		return trace, err
	}
	appendPort.armed = true
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		return trace, err
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	transport.Dispatch.StopWhenDrained(stopSecond)
	if err := second.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("second pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Failed {
		return trace, fmt.Errorf("terminal records=%v err=%v", records, err)
	}
	attempts := 0
	for _, r := range records {
		if r.Kind == journal.Attempt {
			attempts++
			a, err := journal.DecodeAttempt(r.Payload)
			if err != nil || a.Count != attempts {
				return trace, fmt.Errorf("attempt=%+v want=%d err=%v", a, attempts, err)
			}
		}
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[len(records)-1].Payload, &outcome); err != nil || outcome.InvSeq != handle.InvSeq || outcome.Error != "workflow panic: final poison" {
		return trace, fmt.Errorf("outcome=%+v err=%v", outcome, err)
	}
	wantFinish := finishBefore + 1
	if mode == "attempt_drop" {
		wantFinish++
	}
	if attempts != budget || initialCalls != initialBefore || middleCalls != middleBefore || finishCalls != wantFinish || guard.archives != 0 || guard.frames == 0 {
		return trace, fmt.Errorf("mode=%s budget=%d attempts=%d calls=%d/%d/%d prior=%d/%d/%d reads=%d/%d", mode, budget, attempts, initialCalls, middleCalls, finishCalls, initialBefore, middleBefore, finishBefore, guard.archives, guard.frames)
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
	if (strings.HasPrefix(mode, "attempt_") || strings.HasPrefix(mode, "failed_")) && !appendPort.fired {
		return trace, fmt.Errorf("unused journal fault %s", mode)
	}
	if (strings.HasPrefix(mode, "frame_") || strings.HasPrefix(mode, "archive_")) && !snapshots.fired {
		return trace, fmt.Errorf("unused object fault %s", mode)
	}
	snapshots.mu.Lock()
	remaining := len(snapshots.writeFaults)
	snapshots.mu.Unlock()
	if remaining != 0 {
		return trace, fmt.Errorf("unused object faults=%d", remaining)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_continuation_panic", Subject: identity.JournalSubject(typ, id), Sequence: tail, Outcome: fmt.Sprintf("%s:budget=%d", mode, budget), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededContinuationPanicReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_PANIC_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededContinuationPanic(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_PANIC_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]bool{}
	budgets := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runSeededContinuationPanic(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-continuation-panic-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-continuation-panic.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen] = true
		budgets[generated.Decisions[0].Chosen+":"+generated.Decisions[1].Chosen+":"+generated.Decisions[2].Chosen] = true
		if seed <= 10 {
			replayed, err := runSeededContinuationPanic(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d continuation panic replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 9 {
		t.Fatalf("covered %d/9 continuation panic modes", len(observed))
	}
	if len(budgets) != 36 {
		t.Fatalf("covered %d/36 fault and panic-count combinations", len(budgets))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("continuation-panic-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationPanicReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_PANIC_HELPER=1", "SIM_CONTINUATION_PANIC_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("continuation panic trace changed across processes")
	}
}
