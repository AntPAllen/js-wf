package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type heldOutcomeProbe struct {
	*KVTransport
	override []byte
	lost     bool
}

func (p *heldOutcomeProbe) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	if p.lost {
		p.lost = false
		p.event(TransportEvent{Operation: "terminal_probe_read", Subject: key, Outcome: "lost"})
		return lease.KVEntry{}, ErrTransportLost
	}
	value, err := p.KVTransport.Get(ctx, key)
	if p.override != nil && err == nil {
		value.Value = bytes.Clone(p.override)
		p.event(TransportEvent{Operation: "terminal_probe_override", Subject: key, DataSHA256: digest(p.override), Outcome: "ok"})
	}
	return value, err
}

func runSeededTerminalHeld(seed int64, replay *Trace) (Trace, error) {
	return runSeededTerminalDelivery(seed, replay, false, 0)
}

func runSeededTerminalOwned(seed int64, replay *Trace) (Trace, error) {
	return runSeededTerminalDelivery(seed, replay, true, 32)
}

func runSeededTerminalDelivery(seed int64, replay *Trace, owned bool, ownedCount int) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	workload := "worker_terminal_held"
	if owned {
		workload = "worker_terminal_owned"
		if ownedCount == 500 {
			workload += "_500"
		}
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"completed", "failed", "state_read_lost", "wrong_generation", "tombstone", "malformed", "missing_state", "child_notify", "child_notify_drop", "child_notify_ack_lost", "ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journals := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journals, journals)
	var cost *terminalReadCost
	if owned {
		cost = &terminalReadCost{JournalTransport: journals, schedule: schedule}
		store = journal.NewWithPorts(journals, cost)
	}
	kv := NewKVTransport(schedule, 30*time.Second)
	leasing := lease.NewWithKVPort(kv)
	outcomes := &heldOutcomeProbe{KVTransport: NewKVTransport(schedule, 0)}
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	handlers, effects := 0, 0
	var rejectedProbeStop context.CancelFunc
	var ownedStop context.CancelFunc
	ownedAcks := 0
	w, err := worker.NewWithPorts("terminal-probe", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		handlers++
		if mode == "failed" {
			return nil, errors.New("expected failure")
		}
		_, err := wf.Run(c, "once", 42, func(context.Context) (int, error) { effects++; return 42, nil })
		return json.RawMessage(`42`), err
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c}, worker.WithDispatchObserver(func(e worker.DispatchEvent) {
		if e.Stage == "ack" && e.Error == "" && ownedStop != nil {
			ownedAcks++
			if ownedAcks == ownedCount {
				ownedStop()
			}
		}
		if e.Stage == "ack" && rejectedProbeStop != nil {
			rejectedProbeStop()
		}
	}))
	if err != nil {
		return trace, err
	}
	parentPending := 0
	var handle client.Handle
	if strings.HasPrefix(mode, "child_notify") {
		parentID := "parent-0"
		for identity.Partition("parent", parentID, 64) == 0 {
			parentID += "x"
		}
		parent, err := c.Start(ctx, "parent", parentID, []byte(`null`))
		if err != nil {
			return trace, err
		}
		handle, err = c.StartChild(ctx, typ, id, []byte(`null`), "parent", parentID, parent.InvSeq, "result")
		if err != nil {
			return trace, err
		}
		parentPending = 2
	} else {
		handle, err = c.Start(ctx, typ, id, []byte(`null`))
		if err != nil {
			return trace, err
		}
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	transport.Dispatch.StopAfterNextAck(stopFirst)
	if err := w.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != parentPending {
		return trace, fmt.Errorf("initial pending=%d err=%v", transport.Dispatch.Pending(), err)
	}
	prefix, tail, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	state, err := outcomes.KVTransport.Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	originalState := bytes.Clone(state.Value)
	var owner *lease.Lease
	var before lease.KVEntry
	if !owned {
		owner, err = leasing.Acquire(ctx, typ, id, "healthy-owner")
		if err != nil {
			return trace, err
		}
		before, err = kv.Get(ctx, identity.Key(typ, id))
		if err != nil {
			return trace, err
		}
	} else {
		cost.enabled = true
	}
	switch mode {
	case "state_read_lost":
		outcomes.lost = true
	case "wrong_generation":
		outcomes.override, _ = json.Marshal(wf.Outcome{InvSeq: handle.InvSeq + 100, Result: []byte(`42`)})
	case "tombstone":
		outcomes.override = []byte(fmt.Sprintf(`{"tombstone":true,"inv_seq":%d}`, handle.InvSeq))
	case "malformed":
		outcomes.override = []byte(`corrupt`)
	case "missing_state":
		if err := outcomes.KVTransport.Delete(ctx, identity.Key(typ, id), state.Revision); err != nil {
			return trace, err
		}
	}
	if mode == "child_notify_drop" || mode == "child_notify_ack_lost" {
		fault := SignalDropBeforeCommit
		if mode == "child_notify_ack_lost" {
			fault = SignalLoseAckAfterCommit
		}
		if err := transport.QueueSignalFault(fault); err != nil {
			return trace, err
		}
	}
	negative := mode == "state_read_lost" || mode == "wrong_generation" || mode == "tombstone" || mode == "malformed" || mode == "missing_state" || mode == "child_notify_drop" || mode == "child_notify_ack_lost"
	count := 16
	if owned {
		count = ownedCount
		negative = mode == "wrong_generation" || mode == "tombstone" || mode == "malformed"
	}
	for i := 0; i < count; i++ {
		transport.Dispatch.PublishRun("wf.run.0", []byte(identity.Key(typ, id)))
	}
	if negative {
		attempt, stop := context.WithCancel(ctx)
		rejectedProbeStop = stop
		transport.Dispatch.StopAfterNextNak(stop)
		if err := w.RunPartitionWithTransport(attempt, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != count+parentPending {
			return trace, fmt.Errorf("unsafe terminal probe acknowledged pending=%d err=%v", transport.Dispatch.Pending(), err)
		}
		rejectedProbeStop = nil
		outcomes.override = nil
		if mode == "missing_state" {
			if _, err := outcomes.KVTransport.Create(ctx, identity.Key(typ, id), originalState); err != nil {
				return trace, err
			}
		}
		if err := schedule.AdvanceMillis(5000); err != nil {
			return trace, err
		}
	}
	if mode == "ack_lost" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	started := schedule.NowMillis()
	secondCtx, stopSecond := context.WithCancel(ctx)
	if parentPending == 0 {
		transport.Dispatch.StopWhenDrained(stopSecond)
	} else if owned {
		ownedStop = stopSecond
	} else {
		acked := 0
		// The already-published parent start/result wakeups remain on another partition.
		// Stop after the final child duplicate is removed; parent notifications dedup.
		w2, err := worker.NewWithPorts("terminal-child-probe", w.Handlers, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c}, worker.WithDispatchObserver(func(e worker.DispatchEvent) {
			if e.Stage == "ack" && e.Error == "" {
				acked++
				if acked == count {
					stopSecond()
				}
			}
		}))
		if err != nil {
			return trace, err
		}
		w = w2
	}
	expectedWait := int64(0)
	if owned && (mode == "state_read_lost" || mode == "missing_state" || mode == "child_notify_drop" || mode == "child_notify_ack_lost") {
		expectedWait = 100
	}
	if err := w.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != parentPending || schedule.NowMillis()-started != expectedWait {
		return trace, fmt.Errorf("terminal wakeup contention: pending=%d virtual_wait=%d err=%v", transport.Dispatch.Pending(), schedule.NowMillis()-started, err)
	}
	after, err := kv.Get(ctx, identity.Key(typ, id))
	if !owned {
		if err != nil || after.Revision != before.Revision || !bytes.Equal(after.Value, before.Value) {
			return trace, fmt.Errorf("healthy lease changed: before=%+v after=%+v err=%v", before, after, err)
		}
		if err := owner.Renew(ctx); err != nil {
			return trace, fmt.Errorf("owner lost lease: %v", err)
		}
	} else {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("terminal duplicate left a lease: %v", err)
		}
		expectedReads := 0
		if negative || expectedWait != 0 {
			expectedReads = 1
		}
		if cost.reads != expectedReads {
			return trace, fmt.Errorf("terminal full history reads=%d want=%d", cost.reads, expectedReads)
		}
		cost.enabled = false
	}
	records, newTail, err := store.Read(ctx, typ, id)
	if err != nil || newTail != tail || !reflect.DeepEqual(records, prefix) || handlers != 1 || mode != "failed" && effects != 1 {
		return trace, fmt.Errorf("terminal history/effect changed: handlers=%d effects=%d err=%v", handlers, effects, err)
	}
	state, err = outcomes.KVTransport.Get(ctx, identity.Key(typ, id))
	if err != nil || !bytes.Equal(state.Value, originalState) {
		return trace, fmt.Errorf("terminal state changed: %v", err)
	}
	if _, err := integrity.CheckSnapshot(integrity.Snapshot{Invocations: []string{identity.InvocationSubject(typ, id)}, Journals: map[string][]journal.Record{identity.JournalSubject(typ, id): records}, TerminalState: map[string][]byte{identity.Key(typ, id): originalState}}); err != nil {
		return trace, err
	}
	if strings.HasPrefix(mode, "child_notify") {
		transport.SignalTransport.mu.Lock()
		signals := len(transport.SignalTransport.signals)
		transport.SignalTransport.mu.Unlock()
		if signals != 1 {
			return trace, fmt.Errorf("parent notifications=%d", signals)
		}
	}
	checkOperation := "check_terminal_held"
	if owned {
		checkOperation = "check_" + workload
	}
	schedule.RecordTransport(TransportEvent{Operation: checkOperation, Subject: identity.Key(typ, id), Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededTerminalHeldReplay(t *testing.T) {
	if os.Getenv("SIM_TERMINAL_HELD_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededTerminalHeld(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TERMINAL_HELD_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededTerminalHeld(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "terminal-held-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededTerminalHeld(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 11 {
		t.Fatalf("covered %d/11 terminal modes", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("terminal-held-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededTerminalHeldReplay$")
		cmd.Env = append(os.Environ(), "SIM_TERMINAL_HELD_HELPER=1", "SIM_TERMINAL_HELD_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("purge/blob trace changed across processes")
	}
}
