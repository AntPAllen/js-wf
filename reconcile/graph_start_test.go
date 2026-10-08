package reconcile_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"
)

// Interleave repair after the runtime append has staged its owned objects but
// before its head CAS. The old pinned-input repair is a negative control: it
// invalidates the append on every schedule. Metadata-only repair must not.
func TestGraphCanonicalStartRepairDuringPreparedAppend(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		for _, pinned := range []bool{true, false} {
			t.Run(fmt.Sprintf("seed%d/pinned%v", seed, pinned), func(t *testing.T) {
				ctx := context.Background()
				sched := sim.NewScheduler(seed)
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
				h, err := c.Start(ctx, "flow", "append", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				state, err := graph.InspectStart(ctx, h.Type, h.ID)
				if err != nil {
					t.Fatal(err)
				}
				scan, err := reconcile.NewCanonicalStartScanWithPort(graph, c)
				if err != nil {
					t.Fatal(err)
				}
				tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				cuts := 0
				model.PauseBefore("cas_root", func() error {
					cuts++
					if pinned {
						_, e := c.RecoverStartAttempt(ctx, h.Type, h.ID, state.State.Start.Token)
						return e
					}
					result, e := scan.Scan(ctx, 1, 1, false)
					if e == nil && result.Reenqueued != 1 {
						return fmt.Errorf("repair did not enqueue: %+v", result)
					}
					return e
				})
				_, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
				if cuts != 1 {
					t.Fatal("interleaving not exercised", cuts)
				}
				if pinned {
					if !errors.Is(err, journal.ErrStale) {
						t.Fatal("negative control did not invalidate staged append", err)
					}
				} else if err != nil {
					t.Fatal("enqueue repair invalidated staged runtime append", err)
				}
				if err = model.CheckReferences(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type uncertainRecovery struct {
	delegate *client.Client
	fail     bool
	before   func() error
}

func (p *uncertainRecovery) RecoverStartAttempt(ctx context.Context, typ, id, token string) (client.Handle, error) {
	if err := p.cut(); err != nil {
		return client.Handle{}, err
	}
	return p.delegate.RecoverStartAttempt(ctx, typ, id, token)
}

func (p *uncertainRecovery) RepairBoundStartAttempt(ctx context.Context, typ, id, token string, invocation uint64) (client.Handle, error) {
	if err := p.cut(); err != nil {
		return client.Handle{}, err
	}
	return p.delegate.RepairBoundStartAttempt(ctx, typ, id, token, invocation)
}

func (p *uncertainRecovery) cut() error {
	if p.before != nil {
		f := p.before
		p.before = nil
		if err := f(); err != nil {
			return err
		}
	}
	if p.fail {
		return context.DeadlineExceeded
	}
	return nil
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
