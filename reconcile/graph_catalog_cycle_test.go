package reconcile_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"
	"js-wf/wf"
)

// Each lookup republishes a quorum witness. All three scanners must finish a
// two-destination cycle with one-record budgets, even when neither root needs
// repair. This catches moving-watermark starvation without a real cluster.
func TestGraphCanonicalCatalogCyclesWrapWithoutHeadMutation(t *testing.T) {
	for _, kind := range []string{"start", "signal", "terminal"} {
		for seed := int64(1); seed <= 16; seed++ {
			t.Run(fmt.Sprintf("%s/%d", kind, seed), func(t *testing.T) {
				ctx := context.Background()
				schedule := sim.NewScheduler(seed)
				model := sim.NewGraphPublicationTransport(schedule)
				graph, e := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
				if e != nil {
					t.Fatal(e)
				}
				source := sim.NewSignalTransport(schedule)
				state := sim.NewKVTransport(schedule, 0)
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
				status, e := graph.InspectStart(ctx, h.Type, h.ID)
				if e != nil {
					t.Fatal(e)
				}
				tail, e := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
				if e != nil {
					t.Fatal(e)
				}
				started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
				tail, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
				if e != nil {
					t.Fatal(e)
				}
				body, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: json.RawMessage(`42`)})
				_, e = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1, Payload: body}, tail, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = state.Create(ctx, identity.Key(h.Type, h.ID), body); e != nil {
					t.Fatal(e)
				}
				var scan func(context.Context, uint64, int, bool) (reconcile.ScanResult, error)
				switch kind {
				case "start":
					s, e := reconcile.NewCanonicalStartScanWithPort(graph, c)
					if e != nil {
						t.Fatal(e)
					}
					scan = s.Scan
				case "signal":
					s, e := reconcile.NewCanonicalSignalScanWithPort(graph, c)
					if e != nil {
						t.Fatal(e)
					}
					scan = s.Scan
				case "terminal":
					s, e := reconcile.NewCanonicalTerminalScanWithPort(graph, terminalLookup{state}, c)
					if e != nil {
						t.Fatal(e)
					}
					scan = s.Scan
				}
				cursor := uint64(1)
				wraps := 0
				runs := len(source.Runs())
				offset := len(schedule.Trace().Transport)
				for pass := 0; pass < 8; pass++ {
					result, e := scan(ctx, cursor, 1, false)
					if e != nil || result.Reenqueued != 0 || result.Inspected > 1 || result.RetrySequence != 0 {
						t.Fatal(result, e)
					}
					cursor = result.NextSequence
					if cursor == 1 {
						wraps++
					}
				}
				if wraps < 2 || len(source.Runs()) != runs {
					t.Fatal("read witnesses starved catalog wrap", kind, wraps, len(source.Runs()))
				}
				for _, event := range schedule.Trace().Transport[offset:] {
					switch event.Operation {
					case "graph_publication_cas_root", "graph_publication_cas_blob", "graph_publication_put", "graph_publication_get":
						t.Fatal("cycle scanner mutated head or opened payload", event)
					}
				}
			})
		}
	}
}
