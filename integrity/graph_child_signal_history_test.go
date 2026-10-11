package integrity_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"crypto/sha256"
	"encoding/hex"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
)

func childHistoryHash(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

type childHistorySource struct{ msg *jetstream.RawStreamMsg }

func (s childHistorySource) NextSignal(_ context.Context, from uint64, _ string) (*jetstream.RawStreamMsg, error) {
	if from > s.msg.Sequence {
		return nil, jetstream.ErrMsgNotFound
	}
	return s.msg, nil
}

func childHistorySnapshot(t *testing.T, enc journal.Encoding, mode string) integrity.GraphJournalSnapshot {
	t.Helper()
	ctx := context.Background()
	m := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
	g, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Encoding: enc, Now: func() time.Time { return time.Unix(0, 0) }, CanonicalStarts: true, CanonicalSignals: true, PinTTL: time.Minute, IntentTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	start, err := g.ReserveStart(ctx, journal.GraphStartRequest{Type: "parent", ID: "id"}, []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	if err = g.BindStart(ctx, "parent", "id", start.Start.Token, 1); err != nil {
		t.Fatal(err)
	}
	tail, err := g.Begin(ctx, "parent", "id", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendEntry := func(entry journal.Entry, payloads ...[]byte) {
		t.Helper()
		entry.Epoch = 1
		entry.WorkerID = "owner"
		tail, err = g.Append(ctx, "parent", "id", 1, entry, tail, payloads, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	appendEntry(journal.Entry{Kind: journal.Started})
	request := []byte(`{"kind":"call_async","name":"gate","child_type":"child","child_id":"id","input_hash":"` + childHistoryHash([]byte(`7`)) + `"}`)
	if mode == "no-declaration" {
		request = []byte(`{"kind":"run","name":"x","input_hash":"` + childHistoryHash([]byte(`7`)) + `"}`)
	}
	appendEntry(journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: request})
	appendEntry(journal.Entry{Index: 2, Kind: journal.StepCompleted, Payload: []byte(`{}`)})
	child := map[string]any{"type": "child", "id": "id", "inv_seq": uint64(3)}
	outcome := map[string]any{"inv_seq": uint64(3), "result": []byte(`42`)}
	var external []byte
	if strings.HasPrefix(mode, "external") || strings.HasPrefix(mode, "terminal-external") {
		external = []byte(`42`)
		h := childHistoryHash(external)
		child["result_ref"], child["result_hash"] = "child-result", h
		delete(outcome, "result")
		outcome["result_ref"], outcome["result_hash"] = "child-result", h
	}
	switch mode {
	case "failed", "terminal-failed", "terminal-error":
		delete(outcome, "result")
		outcome["error"] = "child failed"
	case "wrong-type":
		child["type"] = "other"
	case "wrong-id":
		child["id"] = "other"
	case "zero-generation":
		child["inv_seq"] = uint64(0)
	case "outcome-generation":
		outcome["inv_seq"] = uint64(4)
	case "child-hash-only":
		child["result_hash"] = childHistoryHash([]byte(`42`))
	case "failed-result":
		outcome["error"] = "failed"
	case "null-error":
		outcome["error"] = nil
	case "unknown-outcome":
		outcome["unknown"] = 1
	case "external-outcome-ref":
		outcome["result_ref"] = "foreign-result"
	case "external-unowned":
		external = nil
	case "external-wrong-bytes":
		external = []byte(`43`)
	}
	body, _ := json.Marshal(outcome)
	input, err := g.ReserveSignal(ctx, journal.GraphSignalRequest{Type: "parent", ID: "id", Invocation: 1, Name: "gate", Key: "key"}, body, false)
	if err != nil {
		t.Fatal(err)
	}
	msg := &jetstream.RawStreamMsg{Sequence: 7, Subject: "wf.sig.parent.id.gate", Data: input.PointerBytes(), Header: nats.Header{journal.GraphSignalTokenHeader: []string{input.Token}, "Wf-Input-SHA256": []string{input.InputSHA256}, "Wf-Inv-Seq": []string{"1"}}}
	if progress, err := g.BindNextSignal(ctx, "parent", "id", 1, 7, childHistorySource{msg}); err != nil || !progress {
		t.Fatal("bind", err)
	}
	event := map[string]any{"sig_seq": uint64(7), "name": "gate", "ref": "graph-signal-" + input.InputSHA256, "hash": input.InputSHA256, "graph_child": child, "canonical_signal": map[string]any{"index": input.Index, "token": input.Token}}
	if mode == "missing-child" {
		delete(event, "graph_child")
	}
	if mode == "mixed-body" {
		event["payload"] = []byte(`forged`)
	}
	payload, _ := json.Marshal(event)
	payloads := [][]byte{body}
	if external != nil {
		payloads = append(payloads, external)
	}
	appendEntry(journal.Entry{Index: 3, Kind: journal.SignalConsumed, Payload: payload}, payloads...)
	projections := map[string][]byte{}
	if strings.HasPrefix(mode, "start-") || strings.HasPrefix(mode, "terminal-") {
		r := journal.GraphStartRequest{Type: "child", ID: "id", ParentType: "parent", ParentID: "id", ParentInvocation: 1, SignalName: "gate"}
		input := []byte(`7`)
		inv := uint64(3)
		switch mode {
		case "start-parent-type":
			r.ParentType = "other"
		case "start-parent-id":
			r.ParentID = "other"
		case "start-parent-generation":
			r.ParentInvocation = 2
		case "start-signal":
			r.SignalName = "other"
		case "start-unparented":
			r.ParentType, r.ParentID, r.ParentInvocation, r.SignalName = "", "", 0, ""
		case "start-input":
			input = []byte(`8`)
		case "start-reused", "terminal-reused":
			inv = 4
		}
		childStart, e := g.ReserveStart(ctx, r, input)
		if e != nil {
			t.Fatal(e)
		}
		if e = g.BindStart(ctx, r.Type, r.ID, childStart.Start.Token, inv); e != nil {
			t.Fatal(e)
		}
		ct, e := g.Begin(ctx, r.Type, r.ID, inv)
		if e != nil {
			t.Fatal(e)
		}
		terminalBody := append([]byte(nil), body...)
		if strings.HasPrefix(mode, "terminal-") {
			var projected map[string]any
			if e = json.Unmarshal(body, &projected); e != nil {
				t.Fatal(e)
			}
			projected["inv_seq"] = inv
			switch mode {
			case "terminal-result":
				projected["result"] = []byte(`43`)
			case "terminal-error":
				projected["error"] = "different failure"
			case "terminal-external-ref":
				projected["result_ref"] = "other-result"
			case "terminal-external-hash":
				projected["result_hash"] = childHistoryHash([]byte(`43`))
			case "terminal-kind":
				delete(projected, "result")
				projected["error"] = "failed"
			}
			terminalBody, e = json.Marshal(projected)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "terminal-whitespace" {
				terminalBody = append([]byte(" "), terminalBody...)
			}
			if mode != "terminal-missing" {
				projections["child.id"] = terminalBody
			}
		}
		kind := journal.Completed
		var projected struct {
			Error string `json:"error"`
		}
		if e = json.Unmarshal(terminalBody, &projected); e != nil {
			t.Fatal(e)
		}
		if projected.Error != "" {
			kind = journal.Failed
		}
		for i, kind := range []journal.Kind{journal.Started, kind} {
			ct, e = g.Append(ctx, r.Type, r.ID, inv, journal.Entry{Index: uint64(i), Kind: kind, Epoch: 1, WorkerID: "child-owner", Payload: terminalBody}, ct, nil, nil)
			if e != nil {
				t.Fatal(e)
			}
		}
		if e = g.Retire(ctx, r.Type, r.ID, inv, ct); e != nil {
			t.Fatal(e)
		}
	}
	roots := map[string]graphpublication.Root{}
	fences := map[string]graphpublication.Fence{}
	keys, err := m.RootKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		r, err := m.ReadRoot(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		roots[k] = r
	}
	keys, err = m.BlobKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		r, err := m.ReadBlob(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		fences[k] = r.Fence
	}
	graph := integrity.GraphReferenceSnapshot{Roots: roots, Fences: fences, PayloadLimit: 1 << 20, LoadObject: func(ctx context.Context, name string, limit int) ([]byte, error) {
		return m.Get(ctx, retainedgraph.Link{Reference: blobpublication.Reference{Object: name}}, limit)
	}}
	source := &jetstream.RawStreamMsg{Sequence: 1, Subject: "wf.inv.parent.id", Data: start.Start.PointerBytes(), Header: nats.Header{journal.GraphStartTokenHeader: []string{start.Start.Token}, "Wf-Input-SHA256": []string{start.Start.InputSHA256}}}
	return integrity.GraphJournalSnapshot{Graph: graph, Invocations: map[string]*jetstream.RawStreamMsg{"parent.id": source}, Projections: projections, ReadProjection: func(_ context.Context, key string) ([]byte, error) {
		if p, ok := projections[key]; ok {
			return p, nil
		}
		return nil, fmt.Errorf("no terminal")
	}}
}
func TestRawGraphChildSignalHistory(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"inline", "failed", "external", "no-declaration", "missing-child", "wrong-type", "wrong-id", "zero-generation", "outcome-generation", "child-hash-only", "failed-result", "null-error", "unknown-outcome", "external-outcome-ref", "external-unowned", "external-wrong-bytes", "mixed-body"} {
			t.Run(string(enc)+"/"+mode, func(t *testing.T) {
				s := childHistorySnapshot(t, enc, mode)
				if _, err := integrity.CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("physical references", err)
				}
				report, err := integrity.CheckGraphJournals(context.Background(), s)
				valid := mode == "inline" || mode == "failed" || mode == "external"
				if valid && err != nil {
					t.Fatal(report, err)
				}
				if !valid && err == nil {
					t.Fatal("invalid child signal history accepted", report)
				}
			})
		}
	}
}

func TestRawGraphChildStartLinks(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"start-valid", "start-reused", "inline", "start-parent-type", "start-parent-id", "start-parent-generation", "start-signal", "start-unparented", "start-input"} {
			t.Run(string(enc)+"/"+mode, func(t *testing.T) {
				s := childHistorySnapshot(t, enc, mode)
				if _, err := integrity.CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("physical references", err)
				}
				report, err := integrity.CheckGraphJournals(context.Background(), s)
				valid := mode == "start-valid" || mode == "start-reused" || mode == "inline"
				if valid && err != nil {
					t.Fatal(report, err)
				}
				if !valid && err == nil {
					t.Fatal("invalid child start link accepted", report)
				}
				if valid {
					verified, unavailable := 0, 1
					if mode == "start-valid" {
						verified, unavailable = 1, 0
					}
					if report.ChildStartLinksVerified != verified || report.ChildStartLinksUnavailable != unavailable {
						t.Fatal("incorrect child start evidence scope", report)
					}
				}
			})
		}
	}
}

func TestRawGraphChildTerminalLinks(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"terminal-inline", "terminal-failed", "terminal-external", "terminal-missing", "terminal-reused", "terminal-result", "terminal-error", "terminal-external-ref", "terminal-external-hash", "terminal-kind", "terminal-whitespace"} {
			t.Run(string(enc)+"/"+mode, func(t *testing.T) {
				s := childHistorySnapshot(t, enc, mode)
				if _, err := integrity.CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("physical references", err)
				}
				report, err := integrity.CheckGraphJournals(context.Background(), s)
				valid := mode == "terminal-inline" || mode == "terminal-failed" || mode == "terminal-external" || mode == "terminal-missing" || mode == "terminal-reused"
				if valid && err != nil {
					t.Fatal(report, err)
				}
				if !valid && err == nil {
					t.Fatal("invalid child terminal link accepted", report)
				}
				if valid {
					verified, unavailable := 1, 0
					if mode == "terminal-missing" || mode == "terminal-reused" {
						verified, unavailable = 0, 1
					}
					if report.ChildTerminalLinksVerified != verified || report.ChildTerminalLinksUnavailable != unavailable {
						t.Fatal("incorrect terminal evidence scope", report)
					}
				}
			})
		}
	}
}
