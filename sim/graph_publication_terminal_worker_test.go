package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"
)

type graphTerminalStateProbe struct {
	*heldOutcomeProbe
	reads      int
	afterFirst func() error
}

func (p *graphTerminalStateProbe) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	p.reads++
	if p.reads == 2 && p.afterFirst != nil {
		if err := p.afterFirst(); err != nil {
			return lease.KVEntry{}, err
		}
	}
	return p.heldOutcomeProbe.Get(ctx, key)
}

var graphTerminalWorkerModes = []string{"completed", "failed", "pending", "cancelled_timer", "purge_after_notify", "missing_state", "forged_state", "state_lost", "purge_marker", "wrong_graph_generation", "malformed_terminal", "unowned_result", "unknown_graph_root", "unknown_reader_pin", "parent_notify", "parent_notify_drop", "parent_notify_lost", "ack_lost"}

func runGraphTerminalWorker(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("graph_worker_terminal_delivery"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphTerminalWorkerModes)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	m := NewGraphPublicationTransport(schedule)
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(schedule, 3*time.Second)
	legacy := NewJournalTransport(schedule)
	store := journal.NewWithPorts(legacy, legacy)
	kv := NewKVTransport(schedule, 30*time.Second)
	leasing := lease.NewWithKVPort(kv)
	state := &heldOutcomeProbe{KVTransport: NewKVTransport(schedule, 0)}
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	var handle client.Handle
	parentID := "parent-0"
	if strings.HasPrefix(mode, "parent_notify") {
		for identity.Partition("parent", parentID, 64) == 0 {
			parentID += "x"
		}
		parent, e := c.Start(ctx, "parent", parentID, []byte(`null`))
		if e != nil {
			return trace, e
		}
		handle, err = c.StartChild(ctx, typ, id, []byte(`null`), "parent", parentID, parent.InvSeq, "result")
	} else {
		handle, err = c.Start(ctx, typ, id, []byte(`null`))
	}
	if err != nil {
		return trace, err
	}
	tail, err := graph.Begin(ctx, typ, id, handle.InvSeq)
	if err != nil {
		return trace, err
	}
	tail, err = graph.Append(ctx, typ, id, handle.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		return trace, err
	}
	outcome := wf.Outcome{InvSeq: handle.InvSeq, Result: []byte(`42`)}
	kind := journal.Completed
	if mode == "failed" {
		kind = journal.Failed
		outcome = wf.Outcome{InvSeq: handle.InvSeq, Error: "graph failure"}
	}
	if mode == "wrong_graph_generation" {
		outcome.InvSeq++
	}
	if mode == "malformed_terminal" {
		outcome.Error = "invalid completion"
	}
	if mode == "unowned_result" {
		outcome.Result = nil
		outcome.ResultRef = "terminal-result-unowned"
		outcome.ResultHash = strings.Repeat("a", 64)
	}
	body, _ := json.Marshal(outcome)
	terminalIndex := uint64(1)
	if mode == "cancelled_timer" {
		for _, entry := range []journal.Entry{{Kind: journal.StepRequested, Index: 1, Payload: json.RawMessage(`{"kind":"timer_cancel","timer_step":7}`)}, {Kind: journal.StepCompleted, Index: 2, Payload: json.RawMessage(`{"cancelled":true}`)}} {
			tail, err = graph.Append(ctx, typ, id, handle.InvSeq, entry, tail, nil, nil)
			if err != nil {
				return trace, err
			}
		}
		terminalIndex = 3
		transport.Dispatch.PublishRunMessage("wf.run.0", []byte(identity.Key(typ, id)), nats.Header{identity.TimerInvSeqHeader: []string{fmt.Sprint(handle.InvSeq)}, identity.TimerStepHeader: []string{"7"}}, now())
	}
	finish := func() error {
		var e error
		tail, e = graph.Append(ctx, typ, id, handle.InvSeq, journal.Entry{Kind: kind, Index: terminalIndex, Payload: body}, tail, nil, nil)
		return e
	}
	if mode != "pending" {
		if err = finish(); err != nil {
			return trace, err
		}
	}
	if mode != "missing_state" {
		if _, err = state.Create(ctx, identity.Key(typ, id), body); err != nil {
			return trace, err
		}
	}
	if mode == "forged_state" {
		state.override = []byte(fmt.Sprintf(`{"inv_seq":%d,"error":"forged mirror"}`, handle.InvSeq+100))
	}
	if mode == "state_lost" {
		state.lost = true
	}
	if mode == "purge_marker" {
		state.override, _ = json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: handle.InvSeq, PurgedAt: now(), ExpiresAt: now().Add(time.Hour)})
	}
	stateProbe := &graphTerminalStateProbe{heldOutcomeProbe: state}
	if mode == "purge_after_notify" {
		stateProbe.afterFirst = func() error {
			entry, e := state.KVTransport.Get(ctx, identity.Key(typ, id))
			if e != nil {
				return e
			}
			marker, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: handle.InvSeq, PurgedAt: now(), ExpiresAt: now().Add(time.Hour)})
			_, e = state.KVTransport.Update(ctx, identity.Key(typ, id), marker, entry.Revision)
			return e
		}
	}
	owner, err := leasing.Acquire(ctx, typ, id, "healthy-owner")
	if err != nil {
		return trace, err
	}
	before, err := kv.Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	if mode == "unknown_graph_root" {
		if err = m.QueueFault("read_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	if mode == "unknown_reader_pin" {
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
	}
	if mode == "parent_notify_drop" || mode == "parent_notify_lost" {
		fault := SignalDropBeforeCommit
		if mode == "parent_notify_lost" {
			fault = SignalLoseAckAfterCommit
		}
		if err = transport.QueueSignalFault(fault); err != nil {
			return trace, err
		}
	}
	if mode == "ack_lost" {
		if err = transport.Dispatch.QueueFault(DispatchFault{Operation: "ack", Kind: "lose_ack_after_commit"}); err != nil {
			return trace, err
		}
	}
	negative := mode == "pending" || mode == "purge_after_notify" || mode == "state_lost" || mode == "purge_marker" || mode == "wrong_graph_generation" || mode == "malformed_terminal" || mode == "unowned_result" || mode == "unknown_graph_root" || mode == "unknown_reader_pin" || mode == "parent_notify_drop" || mode == "parent_notify_lost"
	attempts, handlers, effects, acks, naks := 0, 0, 0, 0, 0
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := worker.NewWithPorts("graph-terminal-probe", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		handlers++
		_, e := wf.Run(c, "unexpected", 1, func(context.Context) (int, error) { effects++; return 1, nil })
		return []byte(`1`), e
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: stateProbe, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now}, worker.WithGraphJournal(graph), worker.WithDispatchObserver(func(e worker.DispatchEvent) {
		if e.Stage == "lease_held" {
			attempts++
		}
		if e.Stage == "ack" {
			acks++
			if mode != "cancelled_timer" || acks == 2 {
				cancel()
			}
		}
		if e.Stage == "nak" {
			naks++
			cancel()
		}
	}))
	if err != nil {
		return trace, err
	}
	if err = w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	if handlers != 0 || effects != 0 || attempts != 1 && (mode != "cancelled_timer" || attempts != 2) {
		return trace, fmt.Errorf("terminal probe executed handler/effect or bypassed held lease: %d/%d/%d", handlers, effects, attempts)
	}
	wantAcks := 1
	if mode == "cancelled_timer" {
		wantAcks = 2
		if w.Metrics().CancelledTimerNoOps != 1 {
			return trace, fmt.Errorf("canceled timer metric differs: %+v", w.Metrics())
		}
	}
	if negative && (acks != 0 || naks != 1) || !negative && (acks != wantAcks || naks != 0) {
		return trace, fmt.Errorf("unsafe ACK/NAK decision %s: %d/%d", mode, acks, naks)
	}
	after, err := kv.Get(ctx, identity.Key(typ, id))
	if err != nil || before.Revision != after.Revision || !bytes.Equal(before.Value, after.Value) {
		return trace, fmt.Errorf("healthy foreign lease changed: %v", err)
	}
	if err = owner.Renew(ctx); err != nil {
		return trace, err
	}
	if err = owner.Release(ctx); err != nil {
		return trace, err
	}
	records, currentTail, err := graph.Read(ctx, typ, id, handle.InvSeq)
	wantRecords := int(terminalIndex) + 1
	if mode == "pending" {
		wantRecords = 1
	}
	if err != nil || currentTail != tail || len(records) != wantRecords || mode != "pending" && !bytes.Equal(records[len(records)-1].Payload, body) {
		return trace, fmt.Errorf("terminal graph changed: %v", err)
	}
	if mode == "parent_notify" {
		signal, err := transport.GetSignalAfter(ctx, "wf.sig.parent."+parentID+".result", 1)
		if err != nil || signal == nil || !bytes.Equal(signal.Data, body) {
			return trace, fmt.Errorf("canonical parent notification differs: %v", err)
		}
	}
	if mode == "pending" {
		if err = finish(); err != nil {
			return trace, err
		}
	}
	state.override = nil
	if old, e := state.KVTransport.Get(ctx, identity.Key(typ, id)); e == nil {
		if err = state.KVTransport.Delete(ctx, identity.Key(typ, id), old.Revision); err != nil {
			return trace, err
		}
	}
	transport.PurgeInvocation(identity.InvocationSubject(typ, id))
	if err = graph.Retire(ctx, typ, id, handle.InvSeq, tail); err != nil {
		return trace, err
	}
	if err = schedule.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("terminal graph did not drain: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_worker_terminal_delivery", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}
func TestSeededGraphTerminalWorkerReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-terminal-worker-failure-")
			if e != nil {
				t.Fatal(e)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, e := runGraphTerminalWorker(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphTerminalWorker(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("terminal worker replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_TERMINAL_WORKER_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphTerminalWorkerModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph terminal worker: modes=%v; canonical ACK/NAK, no handlers/effects, unchanged foreign lease, graph parent bytes and graph drain", observed)
}
