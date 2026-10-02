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

type continuationLimitJournal struct {
	*JournalTransport
	mode         string
	armed, fired bool
}

func (p *continuationLimitJournal) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	var entry journal.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return 0, err
	}
	target := journal.SignalConsumed
	if strings.HasPrefix(p.mode, "completed_") {
		target = journal.StepCompleted
	}
	if strings.HasPrefix(p.mode, "failed_") {
		target = journal.Failed
	}
	if p.armed && !p.fired && entry.Kind == target && p.mode != "clean" {
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

func runSeededContinuationLimit(seed int64, replay *Trace) (Trace, error) {
	return runContinuationLimitScenario(seed, replay, false)
}

func runContinuationLimitScenario(seed int64, replay *Trace, killed bool) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	workload := "continuation_limit"
	if killed {
		workload = "continuation_limit_held_takeover"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	modes := []string{"clean", "signal_drop", "signal_ack_lost", "completed_drop", "completed_ack_lost", "failed_drop", "failed_ack_lost"}
	if killed {
		modes = []string{"after_signal", "after_completion", "after_failed"}
	}
	mode, err := schedule.Choose(modes)
	if err != nil {
		return trace, err
	}
	budgetChoice, err := schedule.Choose([]string{"16", "18", "20"})
	if err != nil {
		return trace, err
	}
	budget, _ := strconv.Atoi(budgetChoice)
	padding := (budget - 16) / 2
	healMillis := int64(0)
	if killed {
		choice, err := schedule.Choose([]string{"0", "500", "2000"})
		if err != nil {
			return trace, err
		}
		healMillis, _ = strconv.ParseInt(choice, 10, 64)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	journalMode := mode
	if killed {
		journalMode = "clean"
	}
	appendPort := &continuationLimitJournal{JournalTransport: live, mode: journalMode}
	snapshots := &continuationWorkerSnapshots{SnapshotReadTransport: NewSnapshotReadTransport(schedule), mode: mode}
	snapshots.BindJournal(live)
	snapshots.BindSignals(transport.SignalTransport)
	store := journal.NewWithSnapshotPort(appendPort, live, snapshots)
	ttl := 30 * time.Second
	if killed {
		ttl = provision.LeaseTTL
	}
	leaseKV := NewKVTransport(schedule, ttl)
	actorKV := &continuationKilledActorKV{KVTransport: leaseKV}
	leasing := lease.NewWithKVPort(actorKV)
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	var initialCalls, middleCalls, finishCalls, effects int
	pad := func(c *wf.Context, from, to int) error {
		for i := from; i < to; i++ {
			if err := c.SetState("padding", i); err != nil {
				return err
			}
		}
		return nil
	}
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		initialCalls++
		if err := c.SetState("value", 10); err != nil {
			return nil, err
		}
		if err := pad(c, 0, padding/2); err != nil {
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
			if err := pad(c, padding/2, padding); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", 10)
		},
		"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			finishCalls++
			if string(locals) != "10" {
				return nil, fmt.Errorf("bad finish locals")
			}
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			_, err := wf.Run(c, "must_not_run", 10, func(context.Context) (int, error) { effects++; return 20, nil })
			return json.RawMessage(`20`), err
		},
	}
	ports := worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, ResultBlobs: snapshots, Client: c, JournalEntryLimit: uint64(budget)}
	first, err := worker.NewWithPorts("panic-first", handlers, ports, worker.WithContinuations(typ, stages))
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
	if len(records) != budget-3 || view.Anchor.Index != uint64(budget-7) || view.Snapshot.Runtime.StepPosition != uint64(budget-8) {
		return trace, fmt.Errorf("pre-resume budget=%d records=%d anchor=%d offset=%d", budget, len(records), view.Anchor.Index, view.Snapshot.Runtime.StepPosition)
	}
	initialBefore, middleBefore, finishBefore := initialCalls, middleCalls, finishCalls
	guard := &panicBudgetFrameReader{continuationWorkerSnapshots: snapshots}
	ports.Journal = journal.NewWithSnapshotPort(appendPort, live, guard)
	ports.ResultBlobs = guard
	second, err := worker.NewWithPorts("panic-second", handlers, ports, worker.WithContinuations(typ, stages))
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
	var cutPrefix []journal.Record
	var cutEpoch uint64
	if killed {
		// Stop only after the selected production append has committed. Cleanup
		// from the stopped actor is suppressed at the model boundary; no successful
		// server delete reply is being hypothesized.
		cut := &continuationTakeoverCut{continuationLimitJournal: appendPort, kind: journal.SignalConsumed, stop: stopSecond, actor: actorKV}
		if mode == "after_completion" {
			cut.kind = journal.StepCompleted
		}
		if mode == "after_failed" {
			cut.kind = journal.Failed
		}
		ports.Journal = journal.NewWithSnapshotPort(cut, live, guard)
		second, err = worker.NewWithPorts("cut-owner", handlers, ports, worker.WithContinuations(typ, stages))
		if err != nil {
			return trace, err
		}
		if err := second.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil || !cut.fired || transport.Dispatch.Pending() != 1 {
			return trace, fmt.Errorf("cut owner pending=%d fired=%t err=%v", transport.Dispatch.Pending(), cut.fired, err)
		}
		cutPrefix, _, err = store.Read(ctx, typ, id)
		want := budget - 2
		if mode == "after_completion" {
			want++
		}
		if mode == "after_failed" {
			want += 2
		}
		if err != nil || len(cutPrefix) != want || cutPrefix[len(cutPrefix)-1].Kind != cut.kind {
			return trace, fmt.Errorf("cut prefix=%v want=%d err=%v", cutPrefix, want, err)
		}
		held, err := leaseKV.Get(ctx, identity.Key(typ, id))
		if err != nil {
			return trace, err
		}
		var oldOwner lease.Value
		if err := json.Unmarshal(held.Value, &oldOwner); err != nil || oldOwner.Worker != "cut-owner" {
			return trace, fmt.Errorf("held owner=%+v err=%v", oldOwner, err)
		}
		cutEpoch = oldOwner.Epoch
		actorKV.dead = false
		schedule.RecordTransport(TransportEvent{Operation: "continuation_cluster_outage", Outcome: "committed_state_retained", AtMillis: schedule.NowMillis()})
		if err := schedule.AdvanceMillis(healMillis); err != nil {
			return trace, err
		}
		// Fresh adapters and a fresh worker recover the retained model state.
		leasing = lease.NewWithKVPort(leaseKV)
		ports.Leases = leasing
		ports.Journal = journal.NewWithSnapshotPort(live, live, guard)
		schedule.RecordTransport(TransportEvent{Operation: "continuation_cluster_healed", Outcome: mode, AtMillis: schedule.NowMillis()})
		expiry := held.Created.UnixMilli() + provision.LeaseTTL.Milliseconds()
		if err := schedule.AdvanceMillis(expiry - 1 - schedule.NowMillis()); err != nil {
			return trace, err
		}
		if _, err := leasing.Acquire(ctx, typ, id, "rival"); !errors.Is(err, lease.ErrHeld) {
			return trace, fmt.Errorf("near-cap takeover before TTL: %v", err)
		}
		if err := schedule.AdvanceMillis(1); err != nil {
			return trace, err
		}
		replacement, err := worker.NewWithPorts("replacement", handlers, ports, worker.WithContinuations(typ, stages))
		if err != nil {
			return trace, err
		}
		replacementCtx, stopReplacement := context.WithCancel(ctx)
		transport.Dispatch.StopWhenDrained(stopReplacement)
		err = replacement.RunPartitionWithTransport(replacementCtx, 0, transport.Dispatch)
		stopReplacement()
		if err != nil || transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("takeover pending=%d err=%v", transport.Dispatch.Pending(), err)
		}
		if schedule.NowMillis()-held.Created.UnixMilli() >= 30000 {
			return trace, fmt.Errorf("modeled cap recovery exceeded 30s")
		}
		if _, err := leaseKV.Update(ctx, identity.Key(typ, id), held.Value, held.Revision); err == nil {
			return trace, fmt.Errorf("dead actor updated old lease revision")
		}
	} else if err := second.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("second pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if killed && effects != 0 {
		return trace, fmt.Errorf("forbidden effect after held takeover: %d", effects)
	}
	if killed && mode != "after_failed" && len(records) > 0 && records[len(records)-1].Epoch <= cutEpoch {
		return trace, fmt.Errorf("replacement epoch did not advance")
	}
	if killed && (len(records) < len(cutPrefix) || !reflect.DeepEqual(records[:len(cutPrefix)], cutPrefix)) {
		return trace, fmt.Errorf("committed cut prefix changed")
	}
	if err != nil || len(records) != budget || records[budget-1].Index != uint64(budget-1) || records[budget-1].Kind != journal.Failed {
		return trace, fmt.Errorf("terminal budget=%d records=%d err=%v", budget, len(records), err)
	}
	if records[budget-3].Kind != journal.SignalConsumed || records[budget-2].Kind != journal.StepCompleted {
		return trace, fmt.Errorf("signal completion lost before reserved failure")
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[budget-1].Payload, &outcome); err != nil || outcome.InvSeq != handle.InvSeq || outcome.Error != journal.ErrTooLong.Error() || outcome.LimitEntry != nil {
		return trace, fmt.Errorf("outcome=%+v err=%v", outcome, err)
	}
	var rejected struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if err := json.Unmarshal(outcome.LimitRequest, &rejected); err != nil || rejected.Kind != "run" || rejected.Name != "must_not_run" || rejected.InputHash == "" {
		return trace, fmt.Errorf("rejected=%+v err=%v", rejected, err)
	}
	if effects != 0 || initialCalls != initialBefore || middleCalls != middleBefore || finishCalls <= finishBefore || guard.archives != 0 || guard.frames == 0 {
		return trace, fmt.Errorf("effect/prefix drift: effects=%d calls=%d/%d/%d reads=%d/%d", effects, initialCalls, middleCalls, finishCalls, guard.archives, guard.frames)
	}
	state, err := outcomes.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(state.Value, records[budget-1].Payload) {
		return trace, fmt.Errorf("terminal state differs: %v", err)
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
	if !killed && mode != "clean" && !appendPort.fired {
		return trace, fmt.Errorf("unused journal fault %s", mode)
	}
	objects := make(map[string][]byte)
	snapshots.mu.Lock()
	for name, value := range snapshots.objects {
		objects[name] = bytes.Clone(value)
	}
	snapshots.mu.Unlock()
	// Audit the original Failed terminal and its retained rejected declaration.
	replayRecords := append([]journal.Record(nil), records...)
	replayLimit := func(history []journal.Record) (wf.ReplayObservation, error) {
		raw, _ := json.Marshal(history)
		var observed wf.ReplayObservation
		_, err := wf.ReplayWithContinuations(raw, func(c *wf.Context) (json.RawMessage, error) { return handlers[typ](c, json.RawMessage(`null`)) }, map[string]wf.ReplayContinuation[json.RawMessage]{
			"middle_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
				return stages["middle_v1"](c, nil, locals)
			},
			"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
				return stages["finish_v1"](c, nil, locals)
			},
		}, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: objects, Observation: &observed})
		return observed, err
	}
	observed, err := replayLimit(replayRecords)
	if !errors.Is(err, wf.ErrReplayPendingStep) || observed.Continuations != 2 || observed.PlayedSteps != budget-5 || observed.PlayedSteps != observed.RecordedSteps || effects != 0 {
		return trace, fmt.Errorf("limit audit=%+v err=%v effects=%d", observed, err, effects)
	}
	changed := append([]journal.Record(nil), replayRecords...)
	changedOutcome := outcome
	changedOutcome.LimitRequest = json.RawMessage(`{"kind":"run","name":"changed","input_hash":"changed"}`)
	changed[budget-1].Payload, _ = json.Marshal(changedOutcome)
	if _, err := replayLimit(changed); !errors.Is(err, wf.ErrNonDeterministic) || effects != 0 {
		return trace, fmt.Errorf("changed limit request accepted: err=%v effects=%d", err, effects)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_continuation_limit", Subject: identity.JournalSubject(typ, id), Sequence: tail, Outcome: fmt.Sprintf("%s:budget=%d", mode, budget), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededContinuationLimitReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_LIMIT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededContinuationLimit(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_LIMIT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]bool{}
	budgets := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runSeededContinuationLimit(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-continuation-limit-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-continuation-limit.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen] = true
		budgets[generated.Decisions[0].Chosen+":"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runSeededContinuationLimit(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d continuation limit replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 7 {
		t.Fatalf("covered %d/7 continuation limit modes", len(observed))
	}
	if len(budgets) != 21 {
		t.Fatalf("covered %d/21 fault and entry-budget combinations", len(budgets))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("continuation-limit-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededContinuationLimitReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_LIMIT_HELPER=1", "SIM_CONTINUATION_LIMIT_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("continuation limit trace changed across processes")
	}
}

func TestModeledJournalEntryLimitBounds(t *testing.T) {
	for _, budget := range []uint64{0, 3, 4, 16, journal.MaxEntries, journal.MaxEntries + 1} {
		t.Run(strconv.FormatUint(budget, 10), func(t *testing.T) {
			schedule := NewScheduler(42)
			transport := NewWorkerTransport(schedule, 3*time.Second)
			live := NewJournalTransport(schedule)
			ports := worker.ModeledWorkerPorts{Journal: journal.NewWithPorts(live, live), Leases: lease.NewWithKVPort(NewKVTransport(schedule, time.Minute)), Outcome: NewKVTransport(schedule, 0), Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport), JournalEntryLimit: budget}
			instance, err := worker.NewWithPorts("limit-bounds", nil, ports)
			invalid := budget != 0 && (budget < 4 || budget > journal.MaxEntries)
			if invalid {
				if instance != nil || err == nil || !strings.Contains(err.Error(), "invalid modeled journal entry limit") {
					t.Fatalf("invalid budget accepted: budget=%d worker=%v err=%v", budget, instance, err)
				}
			} else if instance == nil || err != nil {
				t.Fatalf("valid budget rejected: budget=%d err=%v", budget, err)
			}
		})
	}
}
