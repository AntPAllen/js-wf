//go:build linux

package integration_test

import (
	"encoding/json"
	"reflect"
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

func TestMatrixBulkDeliveryTimeRepresentationMatchesPointUTC(t *testing.T) {
	at := time.Date(2026, 10, 6, 0, 0, 0, 123456789, time.UTC)
	delivery := at.In(time.FixedZone("delivery-local", 0))
	if !delivery.Equal(at) || reflect.DeepEqual(delivery, at) {
		t.Fatal("control requires equal instants with distinct representations")
	}
	p := matrixBulkProjection{bySubject: map[string]*matrixBulkInvocation{}, limit: 4096}
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p", Time: delivery}); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(journal.Entry{Kind: journal.Started})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.parent.p", Sequence: 1, Time: delivery, Data: data}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.ordered[0].enabled, at) || !reflect.DeepEqual(p.ordered[0].times[0], at) {
		t.Fatal("bulk retains delivery time representation instead of point UTC")
	}
}

func TestMatrixBulkPrefixExclusionAndSourceCuts(t *testing.T) {
	cut := matrixBulkSourceCut{First: 1, Last: 3, Messages: 3}
	if got, err := matrixBulkCohortCut(cut, 2, 2); err != nil || got != 2 {
		t.Fatalf("valid prefix: %d %v", got, err)
	}
	for _, bad := range []matrixBulkSourceCut{{First: 0, Last: 3, Messages: 3}, {First: 3, Last: 1, Messages: 3}, {First: 1, Last: 3, Messages: 2}} {
		if _, err := matrixBulkCohortCut(bad, 2, 2); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	for _, boundary := range []uint64{0, 1, 4} {
		if _, err := matrixBulkCohortCut(cut, 2, boundary); err == nil {
			t.Fatal("wrong cohort boundary accepted")
		}
	}
	at := time.Now().UTC()
	p := matrixBulkProjection{cutoff: 1, bySubject: map[string]*matrixBulkInvocation{}, limit: 4096}
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.parent.p", Sequence: 1, Time: at}); err != nil {
		t.Fatal(err)
	}
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.child.c", Sequence: 2, Time: at}); err != nil {
		t.Fatal(err)
	}
	if len(p.ordered) != 1 {
		t.Fatal("outside invocation retained in cohort")
	}
	if err := p.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.child.c", Sequence: 3, Time: at}); err == nil {
		t.Fatal("duplicate excluded invocation accepted")
	}
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.child.c", Time: at}); err != nil {
		t.Fatal(err)
	}
	if !p.excluded["wf.jrn.child.c"].Equal(at) {
		t.Fatal("excluded child last time missing")
	}
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.unknown.u", Time: at}); err == nil {
		t.Fatal("unknown outside journal accepted")
	}
	if err := p.journal(&jetstream.RawStreamMsg{Subject: "wf.jrn.child.c"}); err == nil {
		t.Fatal("missing outside child time accepted")
	}
}
