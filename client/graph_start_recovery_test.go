package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type retireOnReaderRelease struct {
	*sim.GraphPublicationTransport
	retire func() error
}

func (p *retireOnReaderRelease) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	result, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	if err == nil && len(root.Readers) == 0 && p.retire != nil {
		callback := p.retire
		p.retire = nil
		if e := callback(); e != nil {
			return result, e
		}
	}
	return result, err
}

func TestCanonicalStartRecoveryCannotReserveReplacementAfterRetirement(t *testing.T) {
	ctx := context.Background()
	sched := sim.NewScheduler(1)
	model := sim.NewGraphPublicationTransport(sched)
	port := &retireOnReaderRelease{GraphPublicationTransport: model}
	protocol := model.Protocol()
	protocol.Port = port
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true})
	if err != nil {
		t.Fatal(err)
	}
	source := sim.NewSignalTransport(sched)
	c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "flow", "retire", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := graph.ReadStart(ctx, h.Type, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	port.retire = func() error {
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
	}
	if _, err = c.RecoverStartAttempt(ctx, h.Type, h.ID, state.Start.Token); !errors.Is(err, journal.ErrStale) {
		t.Fatal("retirement not observed", err)
	}
	if port.retire != nil {
		t.Fatal("retirement cut was not exercised")
	}
	status, err := graph.InspectRetirement(ctx, h.Type, h.ID)
	if err != nil || !status.Retired || status.PendingStart || status.Invocation != h.InvSeq {
		t.Fatal("recovery resurrected retired generation", status, err)
	}
	if err = model.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

type pendingAwaitPort struct {
	*sim.SignalTransport
	waits int
}

func (p *pendingAwaitPort) State(ctx context.Context, key string) ([]byte, error) {
	return p.StateValue(ctx, key)
}
func (p *pendingAwaitPort) Wait(context.Context, time.Duration) error {
	p.waits++
	return context.DeadlineExceeded
}

func TestCanonicalStartAwaitPendingReplacementAndMissingReadyPointer(t *testing.T) {
	ctx := context.Background()
	sched := sim.NewScheduler(4)
	model := sim.NewGraphPublicationTransport(sched)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true})
	if err != nil {
		t.Fatal(err)
	}
	source := sim.NewSignalTransport(sched)
	results := &pendingAwaitPort{SignalTransport: source}
	c, err := client.NewWithGraphJournalPorts(source, source, results, graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "flow", "replacement", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = graph.Retire(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
		t.Fatal(err)
	}
	source.PurgeInvocation("wf.inv." + h.Type + "." + h.ID)
	state, err := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: h.Type, ID: h.ID}, []byte(`8`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Await(ctx, h.Type, h.ID); !errors.Is(err, context.DeadlineExceeded) || results.waits != 1 {
		t.Fatal("pending replacement was not awaited", err, results.waits)
	}
	// Prepared binding with no native source pointer must remain uncertain.
	if err = graph.BindStart(ctx, h.Type, h.ID, state.Start.Token, h.InvSeq+1); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Await(ctx, h.Type, h.ID); !errors.Is(err, client.ErrStartUnknown) {
		t.Fatal("missing ready pointer treated as absence", err)
	}
}
