package worker_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

func TestWorkerStepMetadataBeforeHandler(t *testing.T) {
	for _, backend := range []string{"legacy", "canonical"} {
		for _, level := range []string{"healthy", "opaque_result", "request", "case", "completion"} {
			fields := []string{"valid"}
			switch level {
			case "request":
				fields = []string{"kind", "name", "input_hash", "duration_nanos", "fire_at", "clock_domain", "timer_step", "timer_name", "child_type", "child_id", "cases"}
			case "case":
				fields = []string{"kind", "name", "timer_step", "fire_at", "clock_domain", "child_type", "child_id"}
			case "completion":
				fields = []string{"case_index", "result_ref", "result_hash", "error", "error_kind", "signal_seq", "selected", "cancelled", "checkpoint_metadata_ref", "checkpoint_metadata_hash"}
			}
			for _, field := range fields {
				t.Run(backend+"/"+level+"/"+field, func(t *testing.T) {
					ctx := context.Background()
					schedule := sim.NewScheduler(1)
					transport := sim.NewWorkerTransport(schedule, 3*time.Second)
					legacy := sim.NewJournalTransport(schedule)
					jrn := journal.NewWithPorts(legacy, legacy)
					c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
					var graph *journal.GraphStore
					var model *sim.GraphPublicationTransport
					if backend == "canonical" {
						model = sim.NewGraphPublicationTransport(schedule)
						var err error
						graph, err = journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return time.Unix(1000, 0) }})
						if err != nil {
							t.Fatal(err)
						}
						c, err = c.WithGraphJournal(graph)
						if err != nil {
							t.Fatal(err)
						}
					}
					h, err := c.Start(ctx, "test", "metadata", []byte(`7`))
					if err != nil {
						t.Fatal(err)
					}
					hash := sha256.Sum256([]byte(`null`))
					req := map[string]any{"kind": "run", "name": "step", "input_hash": hex.EncodeToString(hash[:])}
					done := map[string]any{"result": json.RawMessage(`42`)}
					switch level {
					case "opaque_result":
						done["result"] = json.RawMessage(`{"unknown":null,"repeat":1,"repeat":2}`)
					case "request":
						req[field] = nil
					case "case":
						req["cases"] = []any{map[string]any{"kind": "signal", "name": "gate", field: nil}}
					case "completion":
						done[field] = nil
					}
					r, _ := json.Marshal(req)
					d, _ := json.Marshal(done)
					var tail uint64
					if graph != nil {
						tail, err = graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
						if err != nil {
							t.Fatal(err)
						}
					}
					inputHash := sha256.Sum256([]byte(`7`))
					start, _ := json.Marshal(map[string]any{"input_sha256": hex.EncodeToString(inputHash[:])})
					entries := []journal.Entry{{Kind: journal.Started, Payload: start}, {Index: 1, Kind: journal.StepRequested, Payload: r}, {Index: 2, Kind: journal.StepCompleted, Payload: d}}
					for _, entry := range entries {
						if graph != nil {
							var owned [][]byte
							if entry.Kind == journal.Started {
								owned = [][]byte{[]byte(`7`)}
							}
							tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, owned, nil)
						} else {
							tail, err = jrn.Append(ctx, h.Type, h.ID, entry, tail)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					handlers, effects := 0, 0
					opts := []worker.Option{}
					if graph != nil {
						opts = append(opts, worker.WithGraphJournal(graph))
					}
					w, err := worker.NewWithPorts("metadata-worker", map[string]worker.Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
						handlers++
						return wf.Run(c, "step", nil, func(context.Context) (json.RawMessage, error) { effects++; return json.RawMessage(`42`), nil })
					}}, worker.ModeledWorkerPorts{Journal: jrn, Leases: lease.NewWithKVPort(sim.NewKVTransport(schedule, 30*time.Second)), Outcome: sim.NewKVTransport(schedule, 0), Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)}, opts...)
					if err != nil {
						t.Fatal(err)
					}
					valid := level == "healthy" || level == "opaque_result"
					runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					transport.Dispatch.StopWhenDrained(cancel)
					if !valid {
						transport.Dispatch.StopAfterNextNak(cancel)
					}
					if err = w.RunPartitionWithTransport(runCtx, identity.Partition(h.Type, h.ID, provision.Partitions), transport.Dispatch); err != nil {
						t.Fatal(err)
					}
					want := 0
					if valid {
						want = 1
					}
					if handlers != want || effects != 0 {
						t.Fatalf("handler admission: handlers=%d want=%d effects=%d", handlers, want, effects)
					}
					var count int
					if graph != nil {
						records, _, e := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
						if e != nil {
							t.Fatal(e)
						}
						count = len(records)
						if e = model.CheckReferences(); e != nil {
							t.Fatal(e)
						}
					} else {
						records, _, e := jrn.Read(ctx, h.Type, h.ID)
						if e != nil {
							t.Fatal(e)
						}
						count = len(records)
					}
					if !valid && count != len(entries) {
						t.Fatal(fmt.Sprintf("corrupt delivery changed journal length: %d", count))
					}
				})
			}
		}
	}
}
