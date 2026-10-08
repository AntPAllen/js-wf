package reconcile_test

import (
	"context"
	"errors"
	"testing"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"
)

type uncertainRecovery struct {
	delegate *client.Client
	fail     bool
	before   func() error
}

func (p *uncertainRecovery) RecoverStartAttempt(ctx context.Context, typ, id, token string) (client.Handle, error) {
	if p.before != nil {
		f := p.before
		p.before = nil
		if err := f(); err != nil {
			return client.Handle{}, err
		}
	}
	if p.fail {
		return client.Handle{}, context.DeadlineExceeded
	}
	return p.delegate.RecoverStartAttempt(ctx, typ, id, token)
}

func TestGraphCanonicalStartScanDryRunUnknownAndPrefix(t *testing.T) {
	ctx := context.Background()
	sched := sim.NewScheduler(2)
	model := sim.NewGraphPublicationTransport(sched)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true})
	if err != nil {
		t.Fatal(err)
	}
	source := sim.NewSignalTransport(sched)
	c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	// A witnessed empty destination is a completed prefix, not a missing Start.
	if _, _, err = graph.ReadStart(ctx, "flow", "empty"); err != nil {
		t.Fatal(err)
	}
	state, err := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "flow", ID: "pending"}, []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	port := &uncertainRecovery{delegate: c, fail: true}
	scan, err := reconcile.NewCanonicalStartScanWithPort(graph, port)
	if err != nil {
		t.Fatal(err)
	}
	dry, err := scan.Scan(ctx, 1, 2, true)
	if err != nil || dry.Inspected != 2 || len(dry.Candidates) != 1 || dry.Reenqueued != 0 {
		t.Fatal(dry, err)
	}
	got, _, err := graph.ReadStart(ctx, "flow", "pending")
	if err != nil || !got.Pending || got.Start.Token != state.Start.Token {
		t.Fatal(got, err)
	}
	// Certify only the completed empty-root prefix; leave the uncertain
	// pending attempt unresolved even though quorum reads move its sequence.
	result, err := scan.Scan(ctx, 1, 2, false)
	if !errors.Is(err, journal.ErrUnknown) || result.Reenqueued != 0 || result.RetrySequence <= 1 {
		t.Fatal("uncertain recovery lost prefix", result, err)
	}
	retry := result.RetrySequence
	port.fail = false
	result, err = scan.Scan(ctx, retry, 2, false)
	if err != nil || result.Reenqueued != 1 {
		t.Fatal(result, err)
	}
	bound, _, err := graph.ReadStart(ctx, "flow", "pending")
	if err != nil || bound.Pending || bound.Invocation == 0 || bound.Start.Token != state.Start.Token {
		t.Fatal(bound, err)
	}
	if _, err = c.RecoverStartAttempt(ctx, "flow", "pending", "different"); !errors.Is(err, client.ErrStaleGeneration) {
		t.Fatal(err)
	}
	if err = model.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphCanonicalStartScanRetirementIsSupersededNotReenqueued(t *testing.T) {
	ctx := context.Background()
	sched := sim.NewScheduler(3)
	model := sim.NewGraphPublicationTransport(sched)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true})
	if err != nil {
		t.Fatal(err)
	}
	source := sim.NewSignalTransport(sched)
	c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "flow", "retirement", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	port := &uncertainRecovery{delegate: c, before: func() error {
		tail, e := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
		if e != nil {
			return e
		}
		tail, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
		if e != nil {
			return e
		}
		tail, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
		if e != nil {
			return e
		}
		return graph.Retire(ctx, h.Type, h.ID, h.InvSeq, tail)
	}}
	scan, err := reconcile.NewCanonicalStartScanWithPort(graph, port)
	if err != nil {
		t.Fatal(err)
	}
	var event reconcile.RepairEvent
	scan.Observe = func(e reconcile.RepairEvent) { event = e }
	result, err := scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued != 0 || event.Outcome != "superseded" {
		t.Fatal(result, event, err)
	}
	state, err := graph.InspectRetirement(ctx, h.Type, h.ID)
	if err != nil || !state.Retired || state.PendingStart {
		t.Fatal(state, err)
	}
}
