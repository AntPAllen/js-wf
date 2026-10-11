package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var graphChildLinkModes = []string{"healthy", "projection_mismatch", "parent_mismatch", "input_mismatch", "projection_drop", "projection_lost_ack", "projection_stale_read"}

func runGraphChildLinkAudit(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("graph_child_link_audit"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	stage, err := s.Choose([]string{"consumed", "rejected"})
	if err != nil {
		return trace, err
	}
	encoding, err := s.Choose([]string{"json", "protobuf-v1"})
	if err != nil {
		return trace, err
	}
	mode, err := s.Choose(graphChildLinkModes)
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m := NewGraphPublicationTransport(s)
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	g, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Encoding: journal.Encoding(encoding), Now: now, CanonicalStarts: true, CanonicalSignals: true, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	source := NewSignalTransport(s)
	outcomes := NewKVTransport(s, 0)
	start := func(r journal.GraphStartRequest, input []byte) (uint64, error) {
		attempt, e := g.ReserveStart(ctx, r, input)
		if e != nil {
			return 0, e
		}
		headers := nats.Header{journal.GraphStartTokenHeader: []string{attempt.Start.Token}, "Wf-Input-SHA256": []string{attempt.Start.InputSHA256}}
		if r.ParentType != "" {
			headers.Set("Wf-Parent-Type", r.ParentType)
			headers.Set("Wf-Parent-Id", r.ParentID)
			headers.Set("Wf-Parent-Inv-Seq", fmt.Sprint(r.ParentInvocation))
			headers.Set("Wf-Parent-Signal", r.SignalName)
		}
		seq, e := source.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(r.Type, r.ID), Data: attempt.Start.PointerBytes(), Header: headers})
		if e != nil {
			return 0, e
		}
		return seq, g.BindStart(ctx, r.Type, r.ID, attempt.Start.Token, seq)
	}
	parent, err := start(journal.GraphStartRequest{Type: "parent", ID: "id"}, []byte(`7`))
	if err != nil {
		return trace, err
	}
	request := journal.GraphStartRequest{Type: "child", ID: "id", ParentType: "parent", ParentID: "id", ParentInvocation: parent, SignalName: "gate"}
	child, err := start(request, []byte(`7`))
	if err != nil {
		return trace, err
	}
	childBody := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"NDI="}`, child))
	childTail, err := g.Begin(ctx, "child", "id", child)
	if err != nil {
		return trace, err
	}
	for i, kind := range []journal.Kind{journal.Started, journal.Completed} {
		childTail, err = g.Append(ctx, "child", "id", child, journal.Entry{Index: uint64(i), Kind: kind, Epoch: 1, WorkerID: "child-owner", Payload: childBody}, childTail, nil, nil)
		if err != nil {
			return trace, err
		}
	}
	if err = g.Retire(ctx, "child", "id", child, childTail); err != nil {
		return trace, err
	}
	source.PurgeInvocation(identity.InvocationSubject("child", "id"))
	tail, err := g.Begin(ctx, "parent", "id", parent)
	if err != nil {
		return trace, err
	}
	entries := []journal.Entry{{Kind: journal.Started}, {Index: 1, Kind: journal.StepRequested, Payload: []byte(`{"kind":"call_async","name":"gate","child_type":"child","child_id":"id","input_hash":"` + digest([]byte(`7`)) + `"}`)}, {Index: 2, Kind: journal.StepCompleted, Payload: []byte(`{}`)}}
	for _, entry := range entries {
		entry.Epoch = 1
		entry.WorkerID = "parent-owner"
		tail, err = g.Append(ctx, "parent", "id", parent, entry, tail, nil, nil)
		if err != nil {
			return trace, err
		}
	}
	input, err := g.ReserveSignal(ctx, journal.GraphSignalRequest{Type: "parent", ID: "id", Invocation: parent, Name: "gate", Key: "key"}, childBody, false)
	if err != nil {
		return trace, err
	}
	signalSeq := source.CommitSignal(&nats.Msg{Subject: "wf.sig.parent.id.gate", Data: input.PointerBytes(), Header: nats.Header{journal.GraphSignalTokenHeader: []string{input.Token}, "Wf-Input-SHA256": []string{input.InputSHA256}, "Wf-Inv-Seq": []string{fmt.Sprint(parent)}}})
	if progressed, e := g.BindNextSignal(ctx, "parent", "id", parent, signalSeq, source); e != nil || !progressed {
		return trace, fmt.Errorf("signal binding: %v", e)
	}
	event, err := json.Marshal(map[string]any{"sig_seq": signalSeq, "name": "gate", "ref": "graph-signal-" + input.InputSHA256, "hash": input.InputSHA256, "graph_child": map[string]any{"type": "child", "id": "id", "inv_seq": child}, "canonical_signal": map[string]any{"index": input.Index, "token": input.Token}})
	if err != nil {
		return trace, err
	}
	entry := journal.Entry{Index: 3, Kind: journal.SignalConsumed, Epoch: 1, WorkerID: "parent-owner", Payload: event}
	if stage == "rejected" {
		entry.Kind = journal.Failed
		entry.Payload, err = json.Marshal(map[string]any{"inv_seq": parent, "error": journal.ErrTooLong.Error(), "limit_entry": map[string]any{"kind": string(journal.SignalConsumed), "payload": json.RawMessage(event)}})
		if err != nil {
			return trace, err
		}
		if _, err = outcomes.Create(ctx, "parent.id", entry.Payload); err != nil {
			return trace, err
		}
	}
	tail, err = g.Append(ctx, "parent", "id", parent, entry, tail, [][]byte{childBody}, nil)
	if err != nil {
		return trace, err
	}
	if mode == "projection_drop" || mode == "projection_lost_ack" {
		fault := KVDropBeforeCommit
		if mode == "projection_lost_ack" {
			fault = KVLoseAckAfterCommit
		}
		if err = outcomes.QueueFault(KVFault{Operation: "create", Kind: fault}); err != nil {
			return trace, err
		}
	}
	_, err = outcomes.Create(ctx, "child.id", childBody)
	if mode == "projection_drop" || mode == "projection_lost_ack" {
		if !errors.Is(err, ErrTransportLost) {
			return trace, fmt.Errorf("missing projection uncertainty: %v", err)
		}
	} else if err != nil {
		return trace, err
	}
	wrong := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"NDM="}`, child))
	if mode == "projection_mismatch" || mode == "projection_stale_read" {
		if _, err = outcomes.Put(ctx, "child.id", wrong); err != nil {
			return trace, err
		}
	}
	if mode == "projection_stale_read" {
		if _, err = outcomes.Put(ctx, "child.id", childBody); err != nil {
			return trace, err
		}
	}
	destination := "journal/" + digest([]byte("wf.jrn.child.id"))
	m.mu.Lock()
	original := graphRootCopy(m.roots[destination])
	m.mu.Unlock()
	if mode == "parent_mismatch" || mode == "input_mismatch" {
		var cursor map[string]any
		if err = json.Unmarshal(original.Application, &cursor); err != nil {
			return trace, err
		}
		start := cursor["start"].(map[string]any)
		if mode == "parent_mismatch" {
			start["request"].(map[string]any)["parent_invocation"] = parent + 1
		} else {
			start["input_sha256"] = digest([]byte(`8`))
		}
		raw, e := json.Marshal(cursor)
		if e != nil {
			return trace, e
		}
		// Corruption is a recorded test-only mutation of a retired metadata witness.
		m.mu.Lock()
		root := m.roots[destination]
		root.Application = raw
		m.roots[destination] = root
		m.mu.Unlock()
		s.RecordTransport(TransportEvent{Operation: "corrupt_retired_child_start", Subject: destination, DataSHA256: digest(raw), Outcome: mode})
	}
	snapshot := func() (integrity.GraphJournalSnapshot, error) {
		roots := map[string]graphpublication.Root{}
		fences := map[string]graphpublication.Fence{}
		m.mu.Lock()
		for k, v := range m.roots {
			roots[k] = graphRootCopy(v)
		}
		for k, v := range m.blobs {
			fences[k] = graphFenceCopy(v.Fence)
		}
		m.mu.Unlock()
		graph := integrity.GraphReferenceSnapshot{Roots: roots, Fences: fences, PayloadLimit: journal.MaxGraphEntryBytes, LoadObject: func(call context.Context, name string, limit int) ([]byte, error) {
			if e := call.Err(); e != nil {
				return nil, e
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			data, ok := m.objects[name]
			if !ok || len(data) > limit {
				return nil, fmt.Errorf("missing/oversize audit object")
			}
			m.event("audit_object", name, 0, 0, data, "ok")
			return append([]byte(nil), data...), nil
		}}
		projections := map[string][]byte{}
		keys, e := outcomes.Keys(ctx)
		if e != nil && !errors.Is(e, jetstream.ErrNoKeysFound) {
			return integrity.GraphJournalSnapshot{}, e
		}
		for _, key := range keys {
			value, e := outcomes.Get(ctx, key)
			if e != nil {
				return integrity.GraphJournalSnapshot{}, e
			}
			projections[key] = append([]byte(nil), value.Value...)
		}
		inv, e := source.GetInvocation(ctx, parent)
		if e != nil {
			return integrity.GraphJournalSnapshot{}, e
		}
		return integrity.GraphJournalSnapshot{Graph: graph, Invocations: map[string]*jetstream.RawStreamMsg{"parent.id": inv}, Projections: projections, ReadProjection: func(call context.Context, key string) ([]byte, error) {
			value, e := outcomes.Get(call, key)
			return value.Value, e
		}}, nil
	}
	snap, err := snapshot()
	if err != nil {
		return trace, err
	}
	if _, err = integrity.CheckGraphReferences(ctx, snap.Graph); err != nil {
		return trace, fmt.Errorf("physical fixture: %w", err)
	}
	if mode == "projection_stale_read" {
		if err = outcomes.QueueFault(KVFault{Operation: "get", Kind: KVStaleRead}); err != nil {
			return trace, err
		}
	}
	report, err := integrity.CheckGraphJournals(ctx, snap)
	valid := mode == "healthy" || mode == "projection_drop" || mode == "projection_lost_ack"
	if (err == nil) != valid {
		return trace, fmt.Errorf("child links %s/%s: %v", stage, mode, err)
	}
	if valid {
		verified, unavailable := 1, 0
		if mode == "projection_drop" {
			verified, unavailable = 0, 1
		}
		if report.ChildStartLinksVerified != 1 || report.ChildTerminalLinksVerified != verified || report.ChildTerminalLinksUnavailable != unavailable {
			return trace, fmt.Errorf("witness scope: %+v", report)
		}
	}
	s.RecordTransport(TransportEvent{Operation: "check_child_link_audit", Subject: "parent.id", Sequence: parent, Outcome: fmt.Sprintf("%s_%s_%s_valid=%t", stage, encoding, mode, valid)})
	// Restore only the injected fixture witness; this is not automatic repair.
	m.mu.Lock()
	m.roots[destination] = original
	m.mu.Unlock()
	s.RecordTransport(TransportEvent{Operation: "restore_retired_child_start", Subject: destination, DataSHA256: digest(original.Application), Outcome: "fixture_heal"})
	if _, err = outcomes.Put(ctx, "child.id", childBody); err != nil {
		return trace, err
	}
	snap, err = snapshot()
	if err != nil {
		return trace, err
	}
	report, err = integrity.CheckGraphJournals(ctx, snap)
	if err != nil || report.ChildStartLinksVerified != 1 || report.ChildTerminalLinksVerified != 1 || report.ChildTerminalLinksUnavailable != 0 {
		return trace, fmt.Errorf("healed child links: %+v %v", report, err)
	}
	if stage == "consumed" {
		terminal := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"NDM="}`, parent))
		tail, err = g.Append(ctx, "parent", "id", parent, journal.Entry{Index: 4, Kind: journal.Completed, Epoch: 1, WorkerID: "parent-owner", Payload: terminal}, tail, nil, nil)
		if err != nil {
			return trace, err
		}
	}
	if err = g.Retire(ctx, "parent", "id", parent, tail); err != nil {
		return trace, err
	}
	source.PurgeInvocation(identity.InvocationSubject("parent", "id"))
	source.PurgeSignal(signalSeq)
	keys, err := outcomes.Keys(ctx)
	if err != nil {
		return trace, err
	}
	for _, key := range keys {
		value, e := outcomes.Get(ctx, key)
		if e != nil {
			return trace, e
		}
		if e = outcomes.Delete(ctx, key, value.Revision); e != nil {
			return trace, e
		}
	}
	if err = s.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("owned graph drain=%d: %v", len(objects), err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_child_link_heal_and_drain", Subject: "parent.id", Sequence: parent, Outcome: stage + "_" + encoding + "_" + mode})
	return trace, s.Finish()
}

func TestSeededGraphChildLinkAuditReplay(t *testing.T) {
	seen := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path, err := saveStartSourceHeaderFailure(trace, os.Getenv("FAULT_TRACE_OUT"))
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphChildLinkAudit(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		replayed, err := runGraphChildLinkAudit(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("replay differs: %v", err))
		}
		mode := generated.Decisions[0].Chosen + "_" + generated.Decisions[1].Chosen + "_" + generated.Decisions[2].Chosen
		seen[mode]++
		if root := os.Getenv("SIM_GRAPH_CHILD_LINK_AUDIT_ROOT"); root != "" && seen[mode] == 1 {
			if err = generated.Save(filepath.Join(root, "graph_child_link_audit_"+mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(seen) != 4*len(graphChildLinkModes) {
		t.Fatalf("coverage=%v", seen)
	}
	t.Logf("CHILD_LINK_AUDIT modes=%v; consumed/rejected, retained witnesses, uncertainty, exact replay, fixture heal and zero owned objects", seen)
}
