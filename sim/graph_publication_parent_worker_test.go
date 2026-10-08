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

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphParentWorkerModes = []string{"parent_notify_active", "parent_notify_uninitialized", "parent_notify_forged_mirror", "parent_notify_purging", "parent_notify_retired", "parent_notify_retired_missing_inv", "parent_notify_replaced", "parent_notify_unknown", "parent_notify_consumed_inline", "parent_notify_consumed_external", "parent_notify_consumed_corrupt", "parent_notify_missing_unconfirmed"}

type graphParentSignalPort struct {
	*SignalTransport
	graph   *GraphPublicationTransport
	subject string
	fail    bool
}

func (p *graphParentSignalPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	msg, err := p.SignalTransport.LastInvocation(ctx, subject)
	if err == nil && subject == p.subject && p.fail {
		p.fail = false
		if err = p.graph.QueueFault("read_root", DropBeforeCommit); err != nil {
			return nil, err
		}
	}
	return msg, err
}

func runGraphParentWorker(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("graph_worker_parent_notification"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphParentWorkerModes)
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
	parentPort := &graphParentSignalPort{SignalTransport: transport.SignalTransport, graph: m}
	c := client.NewWithSignalPorts(transport.SignalTransport, parentPort)
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	var handle, parent client.Handle
	parentID := "parent-0"
	if strings.HasPrefix(mode, "parent_notify") {
		for identity.Partition("parent", parentID, 64) == 0 {
			parentID += "x"
		}
		var e error
		parent, e = c.Start(ctx, "parent", parentID, []byte(`null`))
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
	if mode == "parent_notify_consumed_external" {
		outcome.Result = []byte(`"` + strings.Repeat("r", client.MaxInlineSignal+1) + `"`)
	}
	kind := journal.Completed

	body, _ := json.Marshal(outcome)

	terminalIndex := uint64(1)

	finish := func() error {
		var e error
		tail, e = graph.Append(ctx, typ, id, handle.InvSeq, journal.Entry{Kind: kind, Index: terminalIndex, Payload: body}, tail, nil, nil)
		return e
	}
	if err = finish(); err != nil {
		return trace, err
	}
	parentPort.subject = identity.InvocationSubject("parent", parentID)
	var parentTail, parentIndex uint64
	parentAppend := func(kind journal.Kind, payload []byte, owned [][]byte) error {
		next, e := graph.Append(ctx, "parent", parentID, parent.InvSeq, journal.Entry{Kind: kind, Index: parentIndex, Payload: payload}, parentTail, owned, nil)
		if e == nil {
			parentTail = next
			parentIndex++
		}
		return e
	}
	parentFinish := func() error {
		payload, _ := json.Marshal(wf.Outcome{InvSeq: parent.InvSeq, Result: []byte(`42`)})
		return parentAppend(journal.Completed, payload, nil)
	}
	if mode != "parent_notify_uninitialized" {
		parentTail, err = graph.Begin(ctx, "parent", parentID, parent.InvSeq)
		if err != nil {
			return trace, err
		}
		if err = parentAppend(journal.Started, nil, nil); err != nil {
			return trace, err
		}
	}
	if mode == "parent_notify_forged_mirror" {
		transport.SetState(identity.Key("parent", parentID), []byte(`{"tombstone":true,"inv_seq":999,"purged_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-02T00:00:00Z"}`))
		transport.SetJournal("parent", parentID, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}})
	}
	if mode == "parent_notify_purging" || mode == "parent_notify_retired" || mode == "parent_notify_retired_missing_inv" || mode == "parent_notify_replaced" {
		if err = parentFinish(); err != nil {
			return trace, err
		}
		if mode == "parent_notify_purging" {
			err = graph.FencePurge(ctx, "parent", parentID, parent.InvSeq, parentTail)
		} else {
			err = graph.Retire(ctx, "parent", parentID, parent.InvSeq, parentTail)
		}
		if err != nil {
			return trace, err
		}
	}
	if mode == "parent_notify_retired_missing_inv" || mode == "parent_notify_missing_unconfirmed" || mode == "parent_notify_replaced" {
		transport.PurgeInvocation(identity.InvocationSubject("parent", parentID))
	}
	if mode == "parent_notify_replaced" {
		if _, err = c.Start(ctx, "parent", parentID, []byte(`43`)); err != nil {
			return trace, err
		}
	}
	if strings.HasPrefix(mode, "parent_notify_consumed_") {
		request, _ := json.Marshal(map[string]any{"kind": "call_async", "name": "result", "child_type": typ, "child_id": id})
		if err = parentAppend(journal.StepRequested, request, nil); err != nil {
			return trace, err
		}
		bound, e := c.WithGraphJournal(graph)
		if e != nil {
			return trace, e
		}
		childInput, e := transport.LastInvocation(ctx, identity.InvocationSubject(typ, id))
		if e != nil {
			return trace, e
		}
		if e = worker.NotifyParentWithClient(ctx, bound, typ, id, handle.InvSeq, body, childInput.Header); e != nil {
			return trace, e
		}
		signal, e := transport.GetSignalAfter(ctx, "wf.sig.parent."+parentID+".result", 1)
		if e != nil {
			return trace, e
		}
		payload := body
		if mode == "parent_notify_consumed_corrupt" {
			payload = []byte(`{"inv_seq":2,"result":43}`)
		}
		event := map[string]any{"sig_seq": signal.Sequence, "name": "result", "hash": digest(body), "payload": payload}
		var owned [][]byte
		if mode == "parent_notify_consumed_external" {
			delete(event, "payload")
			event["ref"] = "signal-" + digest(body)
			owned = [][]byte{body}
		}
		eventBody, _ := json.Marshal(event)
		if e = parentAppend(journal.SignalConsumed, eventBody, owned); e != nil {
			return trace, e
		}
		transport.PurgeSignal(signal.Sequence)
	}
	parentPort.fail = mode == "parent_notify_unknown"

	if _, err = state.Create(ctx, identity.Key(typ, id), body); err != nil {
		return trace, err
	}

	stateProbe := &graphTerminalStateProbe{heldOutcomeProbe: state}

	owner, err := leasing.Acquire(ctx, typ, id, "healthy-owner")
	if err != nil {
		return trace, err
	}
	before, err := kv.Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}

	negative := mode == "parent_notify_unknown" || mode == "parent_notify_consumed_corrupt" || mode == "parent_notify_missing_unconfirmed"
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
			cancel()
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
	if handlers != 0 || effects != 0 || attempts != 1 {
		return trace, fmt.Errorf("terminal probe executed handler/effect or bypassed held lease: %d/%d/%d", handlers, effects, attempts)
	}
	wantAcks := 1

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

	if err != nil || currentTail != tail || len(records) != wantRecords || !bytes.Equal(records[len(records)-1].Payload, body) {
		return trace, fmt.Errorf("terminal graph changed: %v", err)
	}
	if mode == "parent_notify_active" || mode == "parent_notify_uninitialized" || mode == "parent_notify_forged_mirror" {
		signal, err := transport.GetSignalAfter(ctx, "wf.sig.parent."+parentID+".result", 1)
		if err != nil || signal == nil || !bytes.Equal(signal.Data, body) {
			return trace, fmt.Errorf("canonical parent notification differs: %v", err)
		}
	}

	if mode == "parent_notify_purging" || mode == "parent_notify_retired" || mode == "parent_notify_retired_missing_inv" || mode == "parent_notify_replaced" || mode == "parent_notify_missing_unconfirmed" || mode == "parent_notify_unknown" {
		if signals := transport.SignalFor("parent", parentID, "result"); len(signals) != 0 {
			return trace, fmt.Errorf("notification escaped parent rejection: %s", mode)
		}
	}
	if strings.HasPrefix(mode, "parent_notify_consumed_") {
		if signals := transport.SignalFor("parent", parentID, "result"); len(signals) != 0 {
			return trace, fmt.Errorf("source-deleted duplicate republished: %s", mode)
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

	status, e := graph.InspectRetirement(ctx, "parent", parentID)
	if e != nil {
		return trace, e
	}
	if !status.Retired && status.Invocation != 0 {
		if status.Kind != journal.Completed && status.Kind != journal.Failed {
			if e = parentFinish(); e != nil {
				return trace, e
			}
		}
		if e = graph.Retire(ctx, "parent", parentID, parent.InvSeq, parentTail); e != nil {
			return trace, e
		}
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
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_worker_parent_notification", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}
func TestSeededGraphParentWorkerReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-parent-worker-failure-")
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
		generated, e := runGraphParentWorker(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphParentWorker(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("terminal worker replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_PARENT_WORKER_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphParentWorkerModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph parent worker: modes=%v; canonical ACK/NAK, no handlers/effects, unchanged foreign lease, graph parent bytes and graph drain", observed)
}
