//go:build linux

package integration_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

func TestMatrixBulkProjectionBudgetAndCohortControls(t *testing.T) {
	at := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	newProjection := func() *matrixBulkProjection {
		return &matrixBulkProjection{bySubject: map[string]*matrixBulkInvocation{}, signals: map[uint64]time.Time{}, limit: 4096}
	}
	p := newProjection()
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p", Sequence: 1, Time: at}); err != nil {
		t.Fatal(err)
	}
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p", Sequence: 2, Time: at}); err == nil {
		t.Fatal("duplicate invocation accepted")
	}
	for _, subject := range []string{"wrong.inv.parent.p", "wf.inv.parent", "wf.inv.parent.bad.id"} {
		if err := newProjection().invocation(&jetstream.RawStreamMsg{Subject: subject, Time: at}); err == nil {
			t.Fatalf("invalid subject accepted: %s", subject)
		}
	}
	if err := newProjection().invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p"}); err == nil {
		t.Fatal("missing invocation time accepted")
	}
	p = newProjection()
	p.limit = 1
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p", Time: at}); err == nil || len(p.ordered) != 0 {
		t.Fatal("budget failure retained invocation")
	}
	p.charged = p.limit + 1
	if err := p.charge(0); err == nil {
		t.Fatal("invalid prior budget charge accepted")
	}
}

func TestMatrixBulkProjectionPreservesCausalPayloadsAndDropsEffectBodies(t *testing.T) {
	f := latencyReductionCase(t)
	p := matrixBulkProjection{bySubject: map[string]*matrixBulkInvocation{}, limit: 1 << 20}
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p", Time: f.start}); err != nil {
		t.Fatal(err)
	}
	for index, record := range f.records {
		entry := record.Entry
		if entry.Kind == journal.StepCompleted {
			entry.Payload = json.RawMessage(`{"result":"large irrelevant effect result"}`)
		}
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.parent.p", Sequence: uint64(index + 1), Time: f.times[index], Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	inv := p.ordered[0]
	if len(inv.records) != len(f.records) || inv.records[2].Payload != nil || inv.records[4].Payload != nil {
		t.Fatal("projection retained effect results or omitted records")
	}
	f.records, f.times = inv.records, inv.times
	samples, err := f.reduce(t.Context(), 0)
	if err != nil || len(samples) != 5 || samples[4].Delay != 13*time.Millisecond {
		t.Fatalf("causal projection changed semantics: %+v %v", samples, err)
	}
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.other.x", Time: f.start}); err == nil {
		t.Fatal("journal outside cohort accepted")
	}
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.parent.p", Data: []byte("broken"), Time: f.start}); err == nil {
		t.Fatal("undecodable journal accepted")
	}
	before := len(inv.records)
	p.limit = p.charged
	data, _ := json.Marshal(journal.Entry{Kind: journal.Completed})
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.parent.p", Sequence: 100, Time: f.start, Data: data}); err == nil || len(inv.records) != before {
		t.Fatal("budget failure retained journal record")
	}
}
