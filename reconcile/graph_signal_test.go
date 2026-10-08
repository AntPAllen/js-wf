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

func signalRepairFixture(t *testing.T, seed int64) (*journal.GraphStore, *sim.GraphPublicationTransport, *sim.SignalTransport, *client.Client, client.Handle) {
	t.Helper()
	schedule := sim.NewScheduler(seed)
	model := sim.NewGraphPublicationTransport(schedule)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
	if err != nil {
		t.Fatal(err)
	}
	source := sim.NewSignalTransport(schedule)
	c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(context.Background(), "flow", "repair", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	return graph, model, source, c, h
}

// Discovery needs neither the caller's idempotency key nor its original body.
// Reopening adapters between bounded passes models a lost scanner process.
func TestGraphCanonicalSignalScanBoundedRestart(t *testing.T) {
	for seed := int64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			graph, model, source, c, h := signalRepairFixture(t, seed)
			const inputs = 19
			for i := 0; i < inputs; i++ {
				_, err := graph.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: fmt.Sprint(i)}, []byte(fmt.Sprint(i)), false)
				if err != nil {
					t.Fatal(err)
				}
			}
			for pass := 0; pass < 3; pass++ {
				var err error
				graph, err = journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
				if err != nil {
					t.Fatal(err)
				}
				c, err = client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
				if err != nil {
					t.Fatal(err)
				}
				scan, err := reconcile.NewCanonicalSignalScanWithPort(graph, c)
				if err != nil {
					t.Fatal(err)
				}
				result, err := scan.Scan(ctx, 1, 1, false)
				if err != nil {
					t.Fatal(result, err)
				}
				state, err := graph.InspectStart(ctx, h.Type, h.ID)
				want := uint64((pass + 1) * reconcile.CanonicalSignalRepairBatch)
				if want > inputs {
					want = inputs
				}
				position := want
				if want == inputs {
					position = 0
				}
				if err != nil || state.SignalBindings != want || state.SignalRepair != position {
					t.Fatal(pass, state, err)
				}
			}
			for i := 0; i < inputs; i++ {
				r := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: fmt.Sprint(i)}
				b, body, found, err := graph.ReadSignalBinding(ctx, r)
				if err != nil || !found || b.Index != uint64(i) || string(body) != fmt.Sprint(i) {
					t.Fatal(i, b, found, err)
				}
				source.PurgeSignal(b.Sequence)
			}
			// Already-bound recovery cannot depend on WF_SIG retaining its source.
			scan, err := reconcile.NewCanonicalSignalScanWithPort(graph, c)
			if err != nil {
				t.Fatal(err)
			}
			result, err := scan.Scan(ctx, 1, 1, false)
			if err != nil || result.Reenqueued != 1 {
				t.Fatal(result, err)
			}
			if err = model.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type uncertainSignalRecovery struct {
	delegate *client.Client
	fail     bool
	before   func() error
}

func (p *uncertainSignalRecovery) RecoverSignal(ctx context.Context, r journal.GraphSignalRequest, token string) (uint64, error) {
	if p.before != nil {
		f := p.before
		p.before = nil
		if e := f(); e != nil {
			return 0, e
		}
	}
	if p.fail {
		return 0, context.DeadlineExceeded
	}
	return p.delegate.RecoverSignal(ctx, r, token)
}
func (p *uncertainSignalRecovery) RepairBoundSignalAttempt(ctx context.Context, b journal.GraphSignalBinding) (bool, error) {
	if p.fail {
		return false, context.DeadlineExceeded
	}
	return p.delegate.RepairBoundSignalAttempt(ctx, b)
}
func TestGraphCanonicalSignalScanDryRunAndUnknown(t *testing.T) {
	ctx := context.Background()
	graph, model, _, c, h := signalRepairFixture(t, 41)
	_, err := graph.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: "pending"}, []byte("body"), false)
	if err != nil {
		t.Fatal(err)
	}
	port := &uncertainSignalRecovery{delegate: c, fail: true}
	scan, err := reconcile.NewCanonicalSignalScanWithPort(graph, port)
	if err != nil {
		t.Fatal(err)
	}
	dry, err := scan.Scan(ctx, 1, 1, true)
	if err != nil || len(dry.Candidates) != 1 || dry.Reenqueued != 0 {
		t.Fatal(dry, err)
	}
	state, err := graph.InspectStart(ctx, h.Type, h.ID)
	if err != nil || state.SignalBindings != 0 || state.SignalRepair != 0 {
		t.Fatal(state, err)
	}
	result, err := scan.Scan(ctx, 1, 1, false)
	if !errors.Is(err, journal.ErrUnknown) || result.RetrySequence != 0 || result.NextSequence != 1 {
		t.Fatal(result, err)
	}
	port.fail = false
	result, err = scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued == 0 {
		t.Fatal(result, err)
	}
	if err = model.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphCanonicalSignalBoundRepairDuringPreparedAppend(t *testing.T) {
	for seed := int64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			graph, model, source, c, h := signalRepairFixture(t, seed)
			seq, err := c.Signal(ctx, h.Type, h.ID, "signal", []byte("body"), "key")
			if err != nil {
				t.Fatal(err)
			}
			source.PurgeSignal(seq)
			tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			scan, err := reconcile.NewCanonicalSignalScanWithPort(graph, c)
			if err != nil {
				t.Fatal(err)
			}
			cuts := 0
			model.PauseBefore("cas_root", func() error {
				cuts++
				result, e := scan.Scan(ctx, 1, 1, false)
				if e == nil && result.Reenqueued != 1 {
					return fmt.Errorf("missing repair: %+v", result)
				}
				return e
			})
			_, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil || cuts != 1 {
				t.Fatal("repair invalidated runtime append", cuts, err)
			}
			if err = model.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGraphCanonicalSignalScanRetirementFencesCapturedReservation(t *testing.T) {
	ctx := context.Background()
	graph, model, _, c, h := signalRepairFixture(t, 81)
	_, err := graph.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: "pending"}, []byte("body"), false)
	if err != nil {
		t.Fatal(err)
	}
	port := &uncertainSignalRecovery{delegate: c, before: func() error {
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
	scan, err := reconcile.NewCanonicalSignalScanWithPort(graph, port)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued != 0 {
		t.Fatal(result, err)
	}
	status, err := graph.InspectStart(ctx, h.Type, h.ID)
	if err != nil || !status.Retired {
		t.Fatal(status, err)
	}
	if err = model.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}
