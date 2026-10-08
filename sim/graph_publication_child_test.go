package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphChildModes = []string{"sync", "async", "inline", "failed", "limit_sync", "limit_async", "wrong_parent", "wrong_generation", "mismatched_signal", "missing_child", "child_pending", "source_root_lost", "source_pin_lost", "source_get_lost", "copy_put_drop", "copy_put_lost", "parent_append_lost", "parent_readback_lost", "retire_while_pinned"}

// The source graph is an explicitly prepared terminal fixture with a small
// external result. Production parent workflows, dispatch, signal drain, graph
// append and replay execute unchanged. Native controls also execute the child.
func runGraphChild(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("graph_child_result_transfer"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphChildModes)
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
	outcomes := NewKVTransport(schedule, 0)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	async := mode == "async" || mode == "limit_async"
	limited := strings.HasPrefix(mode, "limit_")
	childType := "child"
	effects := 0
	armed := false
	fired := false
	sourceRetired := false
	result := []byte(`"immutable child bytes"`)
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		var got []byte
		var err error
		if async {
			var p wf.Promise
			p, err = wf.CallAsync(c, childType, []byte(`9`))
			if err == nil {
				got, err = wf.AwaitPromise(c, p)
			}
			if err == nil {
				again, e := wf.AwaitPromise(c, p)
				if e != nil || !bytes.Equal(again, got) {
					return nil, fmt.Errorf("repeated promise differs: %v", e)
				}
			}
		} else {
			got, err = wf.Call(c, childType, []byte(`9`))
		}
		if mode == "failed" {
			if err == nil || err.Error() != "child failed" {
				return nil, fmt.Errorf("child failure differs: %v", err)
			}
		} else {
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(got, result) {
				return nil, fmt.Errorf("child bytes differ")
			}
		}
		if _, err = wf.Run(c, "after-child", 1, func(context.Context) (int, error) { effects++; return 1, nil }); err != nil {
			return nil, err
		}
		if _, err = wf.AwaitSignal(c, "release"); err != nil {
			return nil, err
		}
		return []byte(`42`), nil
	}
	var childID string
	var childSeq, childTail uint64
	var sourceObject string
	// The retirement cut is installed only after child validation, when the
	// parent first uploads its owned copy. The child pin must preserve its bytes.
	retireSource := func() error {
		transport.PurgeInvocation(identity.InvocationSubject(childType, childID))
		if e := graph.Retire(ctx, childType, childID, childSeq, childTail); e != nil {
			return e
		}
		sourceRetired = true
		return nil
	}
	observer := func(e worker.OperationEvent) {
		if !armed || fired || e.Operation != "lease_renew_append" || (e.JournalKind != journal.SignalConsumed && e.JournalKind != journal.Failed) || e.Error != "" {
			return
		}
		fired = true
		switch mode {
		case "source_root_lost":
			runErr = m.QueueFault("read_root", DropBeforeCommit)
		case "source_pin_lost":
			runErr = m.QueueFault("cas_root", LoseAckAfterCommit)
			m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
		case "source_get_lost":
			runErr = m.QueueFault("get", DropBeforeCommit)
		case "copy_put_drop":
			runErr = m.QueueFault("put", DropBeforeCommit)
		case "copy_put_lost":
			runErr = m.QueueFault("put", LoseAckAfterCommit)
		case "parent_append_lost", "parent_readback_lost":
			m.PauseBefore("put", func() error {
				if e := m.QueueFault("cas_root", LoseAckAfterCommit); e != nil {
					return e
				}
				if mode == "parent_readback_lost" {
					m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
				}
				return nil
			})
		case "retire_while_pinned":
			m.PauseBefore("put", func() error {
				if e := retireSource(); e != nil {
					return e
				}
				if e := schedule.AdvanceMillis(500); e != nil {
					return e
				}
				if _, e := m.Protocol().SweepWithReaders(ctx, now()); e != nil {
					return e
				}
				objects, e := m.Objects(ctx)
				if e != nil {
					return e
				}
				for _, object := range objects {
					if object.Reference.Object == sourceObject {
						return nil
					}
				}
				return fmt.Errorf("pinned child source collected during transfer")
			})
		}
	}
	handle, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	// Find a child type whose actual runtime ID routes away from parent partition0.
	for {
		material := fmt.Sprintf("%s:%s:%d:%s:0", typ, id, handle.InvSeq, childType)
		childID = "c-" + digest([]byte(material))[:32]
		if identity.Partition(childType, childID, 64) != 0 {
			break
		}
		childType += "x"
	}
	// Stage loops stop only after a recorded ACK/NAK and join before inspection.
	runStage := func(name string) (worker.DispatchEvent, error) {
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var decision worker.DispatchEvent
		limit := uint64(0)
		if limited {
			limit = 4
			if async {
				limit = 6
			}
		}
		w, e := worker.NewWithPorts(name, map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: now, OperationObserver: observer, JournalEntryLimit: limit}, worker.WithGraphJournal(graph), worker.WithDispatchObserver(func(e worker.DispatchEvent) {
			if e.Stage == "ack" || e.Stage == "nak" {
				decision = e
				cancel()
			}
		}))
		if e != nil {
			return decision, e
		}
		e = w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch)
		return decision, e
	}
	decision, err := runStage("child-parent-first")
	if err != nil || decision.Stage != "ack" {
		return trace, fmt.Errorf("first parent stage: %+v %v", decision, err)
	}
	initial, _, e := graph.Read(ctx, typ, id, handle.InvSeq)
	if e != nil || len(initial) == 0 || initial[len(initial)-1].Kind != journal.Suspended {
		return trace, fmt.Errorf("initial parent did not suspend: %v", e)
	}
	childInput, err := transport.LastInvocation(ctx, identity.InvocationSubject(childType, childID))
	if err != nil {
		return trace, err
	}
	childSeq = childInput.Sequence
	originalHeader := cloneHeader(childInput.Header)
	beginSource := func() error {
		var e error
		childTail, e = graph.Begin(ctx, childType, childID, childSeq)
		if e != nil {
			return e
		}
		inputHash := digest(childInput.Data)
		started, _ := json.Marshal(map[string]string{"input_sha256": inputHash})
		childTail, e = graph.Append(ctx, childType, childID, childSeq, journal.Entry{Kind: journal.Started, Payload: started}, childTail, [][]byte{childInput.Data}, nil)
		return e
	}
	outcome := wf.Outcome{InvSeq: childSeq, ResultRef: "terminal-result-" + digest(result), ResultHash: digest(result)}
	if mode == "inline" {
		outcome = wf.Outcome{InvSeq: childSeq, Result: result}
	}
	if mode == "failed" {
		outcome = wf.Outcome{InvSeq: childSeq, Error: "child failed"}
	}
	body, _ := json.Marshal(outcome)
	finishSource := func() error {
		kind := journal.Completed
		var payloads [][]byte
		if mode == "failed" {
			kind = journal.Failed
		}
		if outcome.ResultRef != "" {
			payloads = [][]byte{result}
		}
		var e error
		childTail, e = graph.Append(ctx, childType, childID, childSeq, journal.Entry{Kind: kind, Index: 1, Payload: body}, childTail, payloads, nil)
		if e != nil {
			return e
		}
		view, e := graph.OpenTerminal(ctx, childType, childID, childSeq)
		if e != nil {
			return e
		}
		record, e := view.Read(ctx, 1)
		if e == nil {
			for _, link := range record.Blobs {
				if link.Hash == outcome.ResultHash {
					sourceObject = link.Reference.Object
				}
			}
		}
		closeErr := view.Close(ctx)
		if e != nil {
			return e
		}
		return closeErr
	}
	if mode != "missing_child" {
		if err = beginSource(); err != nil {
			return trace, err
		}
		if mode != "child_pending" {
			if err = finishSource(); err != nil {
				return trace, err
			}
		}
	}
	signalBody := body
	if mode == "wrong_generation" {
		wrong := outcome
		wrong.InvSeq++
		signalBody, _ = json.Marshal(wrong)
	}
	if mode == "mismatched_signal" {
		wrong := outcome
		wrong.Error = "forged"
		signalBody, _ = json.Marshal(wrong)
	}
	if mode == "wrong_parent" {
		transport.StartTransport.mu.Lock()
		input := transport.invocations[identity.InvocationSubject(childType, childID)]
		input.Header.Set(client.ParentIDHeader, "foreign")
		transport.invocations[input.Subject] = input
		transport.StartTransport.mu.Unlock()
		schedule.RecordTransport(TransportEvent{Operation: "fixture_child_parent_header", Outcome: "foreign"})
	}
	signal, err := c.Signal(ctx, typ, id, "child_0", signalBody, "child-result")
	if err != nil {
		return trace, err
	}
	armed = true
	decision, err = runStage("child-parent-transfer")
	if err != nil {
		return trace, err
	}
	if runErr != nil {
		return trace, runErr
	}
	negative := mode == "wrong_parent" || mode == "wrong_generation" || mode == "mismatched_signal" || mode == "missing_child" || mode == "child_pending" || mode == "source_root_lost" || mode == "source_pin_lost" || mode == "source_get_lost" || mode == "copy_put_drop" || mode == "copy_put_lost" || mode == "parent_readback_lost"
	if negative {
		if decision.Stage != "nak" || effects != 0 {
			return trace, fmt.Errorf("invalid transfer was accepted: %s %+v effects=%d", mode, decision, effects)
		}
		if mode == "missing_child" {
			if err = beginSource(); err != nil {
				return trace, err
			}
		}
		if mode == "missing_child" || mode == "child_pending" {
			if err = finishSource(); err != nil {
				return trace, err
			}
		}
		if mode == "wrong_parent" {
			transport.StartTransport.mu.Lock()
			input := transport.invocations[childInput.Subject]
			input.Header = cloneHeader(originalHeader)
			transport.invocations[input.Subject] = input
			transport.StartTransport.mu.Unlock()
			schedule.RecordTransport(TransportEvent{Operation: "fixture_child_parent_header", Outcome: "restored"})
		}
		if mode == "wrong_generation" || mode == "mismatched_signal" {
			transport.SignalTransport.mu.Lock()
			message := transport.signals[signal]
			message.Data = bytes.Clone(body)
			message.Header.Set("Wf-Input-SHA256", digest(body))
			transport.signals[signal] = message
			transport.SignalTransport.mu.Unlock()
			schedule.RecordTransport(TransportEvent{Operation: "fixture_child_signal", Outcome: "restored"})
		}
		decision, err = runStage("child-parent-retry")
		if err != nil {
			return trace, err
		}
	}
	if limited {
		if decision.Stage != "nak" || effects != 0 {
			return trace, fmt.Errorf("limit failure did not retain dispatch: %+v", decision)
		}
		decision, err = runStage("child-parent-limit-repair")
		if err != nil {
			return trace, err
		}
	}
	if decision.Stage != "ack" {
		return trace, fmt.Errorf("transfer did not ACK: %s %+v", mode, decision)
	}
	if !fired {
		return trace, fmt.Errorf("transfer cut not visited")
	}
	records, tail, err := graph.Read(ctx, typ, id, handle.InvSeq)
	if err != nil {
		return trace, err
	}
	found := false
	for _, record := range records {
		var event struct {
			Child *struct {
				Type       string `json:"type"`
				ID         string `json:"id"`
				Invocation uint64 `json:"inv_seq"`
				Ref        string `json:"result_ref"`
				Hash       string `json:"result_hash"`
			} `json:"graph_child"`
		}
		payload := record.Payload
		if record.Kind == journal.Failed {
			var failure wf.Outcome
			if json.Unmarshal(payload, &failure) != nil {
				return trace, fmt.Errorf("bad failure")
			}
			if failure.LimitEntry != nil && failure.LimitEntry.Kind == string(journal.SignalConsumed) {
				payload = failure.LimitEntry.Payload
			} else {
				continue
			}
		} else if record.Kind != journal.SignalConsumed {
			continue
		}
		if json.Unmarshal(payload, &event) != nil || event.Child == nil {
			return trace, fmt.Errorf("missing child transfer declaration")
		}
		child := event.Child
		if child.Type != childType || child.ID != childID || child.Invocation != childSeq || child.Ref != outcome.ResultRef || child.Hash != outcome.ResultHash {
			return trace, fmt.Errorf("child provenance differs")
		}
		found = true
	}
	if !found {
		return trace, fmt.Errorf("no parent child declaration")
	}
	if !sourceRetired {
		if err = retireSource(); err != nil {
			return trace, err
		}
	}
	transport.PurgeSignal(signal)
	if err = schedule.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil {
		return trace, err
	}
	for _, object := range objects {
		if sourceObject != "" && object.Reference.Object == sourceObject {
			return trace, fmt.Errorf("child physical source survives retirement")
		}
	}
	if len(objects) == 0 {
		return trace, fmt.Errorf("parent ownership was collected")
	}
	if !limited {
		if _, err = c.Signal(ctx, typ, id, "release", []byte(`true`), "release"); err != nil {
			return trace, err
		}
		decision, err = runStage("child-parent-replay")
		if err != nil || decision.Stage != "ack" {
			return trace, fmt.Errorf("parent replay without child: %+v %v", decision, err)
		}
		records, tail, err = graph.Read(ctx, typ, id, handle.InvSeq)
		if err != nil || records[len(records)-1].Kind != journal.Completed || effects != 1 {
			return trace, fmt.Errorf("parent completion/effect differs %d: %v", effects, err)
		}
	} else {
		if records[len(records)-1].Kind != journal.Failed || effects != 0 {
			return trace, fmt.Errorf("journal limit boundary differs")
		}
	}
	if limited {
		transport.Dispatch.PublishRun("wf.run.0", []byte(identity.Key(typ, id)))
		decision, err = runStage("child-parent-limit-replay")
		if err != nil || decision.Stage != "ack" || effects != 0 {
			return trace, fmt.Errorf("limit replay without child differs: %+v %v", decision, err)
		}
	}

	legacyRecords, _, err := store.Read(ctx, typ, id)
	if err != nil || len(legacyRecords) != 0 {
		return trace, fmt.Errorf("child transfer wrote legacy journal: %v", err)
	}
	// Explicit closed-fixture lifecycle ordering, not production retention.
	if old, e := outcomes.Get(ctx, identity.Key(typ, id)); e == nil {
		if err = outcomes.Delete(ctx, identity.Key(typ, id), old.Revision); err != nil {
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
	objects, err = m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("child transfer graph did not drain: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_child_transfer", Outcome: mode})
	if err = ctx.Err(); err != nil {
		return trace, err
	}
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphChildReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-child-failure-")
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
	resultFor := graphChildSeedPipeline(t, seededScheduleLimit(t), runGraphChild)
	for seed := range seededSchedules(t) {
		result := resultFor(seed)
		generated, e := result.trace, result.err
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_GRAPH_CHILD_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphChildModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph child transfer modes=%v; copy before publication, verified provenance, bounded source pin, parent replay without child and graph drain", observed)
}
