package reconcile_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"
	"js-wf/wf"
)

type terminalLookup struct{ kv *sim.KVTransport }

func (p terminalLookup) ProjectionPresent(ctx context.Context, typ, id string) (bool, error) {
	_, err := p.kv.Get(ctx, identity.Key(typ, id))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

type terminalRecoveryCut struct {
	c      *client.Client
	before func() error
}

func (p *terminalRecoveryCut) RepairTerminalProjectionAttempt(ctx context.Context, typ, id, token string, inv uint64) (bool, error) {
	if p.before != nil {
		f := p.before
		p.before = nil
		if e := f(); e != nil {
			return false, e
		}
	}
	return p.c.RepairTerminalProjectionAttempt(ctx, typ, id, token, inv)
}

func TestGraphCanonicalTerminalScanBoundedDryRunUnknownAndRecovery(t *testing.T) {
	for seed := int64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			sched := sim.NewScheduler(seed)
			m := sim.NewGraphPublicationTransport(sched)
			graph, e := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalStarts: true})
			if e != nil {
				t.Fatal(e)
			}
			source := sim.NewSignalTransport(sched)
			kv := sim.NewKVTransport(sched, 0)
			c, e := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
			if e != nil {
				t.Fatal(e)
			}
			if _, _, e = graph.ReadStart(ctx, "flow", "empty"); e != nil {
				t.Fatal(e)
			}
			h, e := c.Start(ctx, "flow", "terminal", []byte(`7`))
			if e != nil {
				t.Fatal(e)
			}
			state, e := graph.InspectStart(ctx, h.Type, h.ID)
			if e != nil {
				t.Fatal(e)
			}
			tail, e := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if e != nil {
				t.Fatal(e)
			}
			started, _ := json.Marshal(map[string]string{"input_sha256": state.State.Start.InputSHA256})
			tail, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
			if e != nil {
				t.Fatal(e)
			}
			payload, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: json.RawMessage(`42`)})
			_, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1, Payload: payload}, tail, nil, nil)
			if e != nil {
				t.Fatal(e)
			}
			scan, e := reconcile.NewCanonicalTerminalScanWithPort(graph, terminalLookup{kv}, c)
			if e != nil {
				t.Fatal(e)
			}
			first, e := scan.Scan(ctx, 1, 1, true)
			if e != nil || first.Inspected != 1 || len(first.Candidates) != 0 || first.NextSequence <= 1 {
				t.Fatal(first, e)
			}
			runs := len(source.Runs())
			events := len(sched.Trace().Transport)
			dry, e := scan.Scan(ctx, first.NextSequence, 1, true)
			if e != nil || len(dry.Candidates) != 1 || dry.Reenqueued != 0 || len(source.Runs()) != runs {
				t.Fatal(dry, e)
			}
			if e = kv.QueueFault(sim.KVFault{Operation: "get", Kind: sim.KVGetTransportLost}); e != nil {
				t.Fatal(e)
			}
			unknown, e := scan.Scan(ctx, 1, 2, false)
			if !errors.Is(e, journal.ErrUnknown) || unknown.RetrySequence <= 1 || unknown.NextSequence != unknown.RetrySequence || len(source.Runs()) != runs {
				t.Fatal(unknown, e)
			}
			cursor := unknown.RetrySequence
			for pass := 0; pass < 2; pass++ {
				found := false
				for attempt := 0; attempt < 8; attempt++ {
					repaired, e := scan.Scan(ctx, cursor, 1, false)
					if e != nil {
						t.Fatal(repaired, e)
					}
					cursor = repaired.NextSequence
					if repaired.Reenqueued == 1 {
						found = true
						break
					}
				}
				if !found || len(source.Runs()) != runs+pass+1 {
					t.Fatal("bounded catalog did not revisit terminal", pass, source.Runs())
				}
			}
			// Repeated repairs must publish fresh deliveries even inside native dedup
			// windows, and no scanner operation may pin/read/upload a payload or mutate head.
			for _, event := range sched.Trace().Transport[events:] {
				switch event.Operation {
				case "graph_publication_get", "graph_publication_put", "graph_publication_cas_root", "graph_publication_cas_blob":
					t.Fatal("metadata scan changed ownership", event)
				}
			}
			if _, e = kv.Create(ctx, identity.Key(h.Type, h.ID), []byte(`{"tombstone":true}`)); e != nil {
				t.Fatal(e)
			}
			wrapped := false
			for pass := 0; pass < 8; pass++ {
				skip, e := scan.Scan(ctx, cursor, 1, false)
				if e != nil || skip.Reenqueued != 0 || len(source.Runs()) != runs+2 {
					t.Fatal(skip, e)
				}
				cursor = skip.NextSequence
				if cursor == 1 {
					wrapped = true
				}
			}
			if !wrapped {
				t.Fatal("self-generated read witnesses prevented catalog wrap")
			}
			if e = m.CheckReferences(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestGraphCanonicalTerminalScanRetirementAndUncertainEnqueue(t *testing.T) {
	for _, retire := range []bool{false, true} {
		t.Run(fmt.Sprint(retire), func(t *testing.T) {
			ctx := context.Background()
			sched := sim.NewScheduler(17)
			m := sim.NewGraphPublicationTransport(sched)
			graph, e := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalStarts: true})
			if e != nil {
				t.Fatal(e)
			}
			source := sim.NewSignalTransport(sched)
			kv := sim.NewKVTransport(sched, 0)
			c, e := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
			if e != nil {
				t.Fatal(e)
			}
			h, e := c.Start(ctx, "flow", "terminal", []byte(`7`))
			if e != nil {
				t.Fatal(e)
			}
			state, e := graph.InspectStart(ctx, h.Type, h.ID)
			if e != nil {
				t.Fatal(e)
			}
			tail, e := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if e != nil {
				t.Fatal(e)
			}
			started, _ := json.Marshal(map[string]string{"input_sha256": state.State.Start.InputSHA256})
			tail, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
			if e != nil {
				t.Fatal(e)
			}
			payload, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: json.RawMessage(`42`)})
			tail, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1, Payload: payload}, tail, nil, nil)
			if e != nil {
				t.Fatal(e)
			}
			cut := &terminalRecoveryCut{c: c}
			if retire {
				cut.before = func() error { return graph.Retire(ctx, h.Type, h.ID, h.InvSeq, tail) }
			} else {
				cut.before = func() error {
					return source.QueueFault(sim.StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
				}
			}
			scan, e := reconcile.NewCanonicalTerminalScanWithPort(graph, terminalLookup{kv}, cut)
			if e != nil {
				t.Fatal(e)
			}
			result, e := scan.Scan(ctx, 1, 1, false)
			if cut.before != nil {
				t.Fatal("interception not reached")
			}
			if retire {
				if e != nil || result.Reenqueued != 0 {
					t.Fatal(result, e)
				}
			} else {
				if !errors.Is(e, journal.ErrUnknown) || result.NextSequence != 1 || result.Reenqueued != 0 {
					t.Fatal(result, e)
				}
				if again, e := scan.Scan(ctx, 1, 1, false); e != nil || again.Reenqueued != 1 {
					t.Fatal(again, e)
				}
			}
		})
	}
}
