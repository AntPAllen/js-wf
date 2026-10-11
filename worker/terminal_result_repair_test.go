package worker_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type terminalResultGraphPort struct {
	*sim.GraphPublicationTransport
	hash, mode string
	reads      int
}

func (p *terminalResultGraphPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if link.Hash == p.hash {
		p.reads++
		if p.mode == "unavailable" {
			return nil, jetstream.ErrMsgNotFound
		}
		if p.mode == "wrong_bytes" {
			return []byte(`43`), nil
		}
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}

type terminalResultBlobPort struct {
	*sim.ResultBlobTransport
	mode  string
	reads int
}

func (p *terminalResultBlobPort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	p.reads++
	if p.mode == "unavailable" {
		return nil, worker.ErrResultBlobUnavailable
	}
	if p.mode == "wrong_bytes" {
		return []byte(`43`), nil
	}
	return p.ResultBlobTransport.GetBytes(ctx, name)
}

func TestWorkerTerminalResultVerifiedBeforeRepair(t *testing.T) {
	for _, backend := range []string{"legacy", "canonical"} {
		for _, mode := range []string{"healthy", "unavailable", "wrong_bytes", "inline_conflict"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				schedule := sim.NewScheduler(1)
				transport := sim.NewWorkerTransport(schedule, 3*time.Second)
				legacy := sim.NewJournalTransport(schedule)
				jrn := journal.NewWithPorts(legacy, legacy)
				c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
				body := []byte(`42`)
				sum := sha256.Sum256(body)
				hash := hex.EncodeToString(sum[:])
				ref := "terminal-result-" + hash
				blobs := &terminalResultBlobPort{ResultBlobTransport: sim.NewResultBlobTransport(schedule)}
				if err := blobs.PutBytes(ctx, ref, body); err != nil {
					t.Fatal(err)
				}
				var graph *journal.GraphStore
				var graphPort *terminalResultGraphPort
				if backend == "canonical" {
					model := sim.NewGraphPublicationTransport(schedule)
					graphPort = &terminalResultGraphPort{GraphPublicationTransport: model, hash: hash}
					protocol := model.Protocol()
					protocol.Port = graphPort
					var err error
					graph, err = journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return time.Unix(1000, 0) }})
					if err != nil {
						t.Fatal(err)
					}
					c, err = c.WithGraphJournal(graph)
					if err != nil {
						t.Fatal(err)
					}
				}
				h, err := c.Start(ctx, "test", "result_repair", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				outcome := wf.Outcome{InvSeq: h.InvSeq, ResultRef: ref, ResultHash: hash}
				if mode == "inline_conflict" {
					outcome.Result = body
				}
				raw, err := json.Marshal(outcome)
				if err != nil {
					t.Fatal(err)
				}
				inputSum := sha256.Sum256([]byte(`7`))
				start, _ := json.Marshal(map[string]string{"input_sha256": hex.EncodeToString(inputSum[:])})
				entries := []journal.Entry{{Kind: journal.Started, Payload: start}, {Index: 1, Kind: journal.Completed, Payload: raw}}
				var tail uint64
				if graph != nil {
					tail, err = graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
				}
				for _, entry := range entries {
					if graph != nil {
						owned := [][]byte{body}
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
				outcomes := sim.NewKVTransport(schedule, 0)
				leases := lease.NewWithKVPort(sim.NewKVTransport(schedule, 30*time.Second))
				handlers := 0
				deliver := func(fault string, valid bool) {
					blobs.mode = fault
					blobs.reads = 0
					if graphPort != nil {
						graphPort.mode = fault
						graphPort.reads = 0
					}
					opts := []worker.Option{}
					var resultPort worker.ResultBlobPort = blobs
					if graph != nil {
						opts = append(opts, worker.WithGraphJournal(graph))
						resultPort = nil
					}
					w, e := worker.NewWithPorts("result-reader", map[string]worker.Handler{h.Type: func(*wf.Context, json.RawMessage) (json.RawMessage, error) { handlers++; return nil, nil }}, worker.ModeledWorkerPorts{Journal: jrn, Leases: leases, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, ResultBlobs: resultPort, HeartbeatTicks: make(chan time.Time)}, opts...)
					if e != nil {
						t.Fatal(e)
					}
					runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					transport.Dispatch.StopWhenDrained(cancel)
					transport.Dispatch.StopAfterNextNak(cancel)
					if e = w.RunPartitionWithTransport(runCtx, identity.Partition(h.Type, h.ID, provision.Partitions), transport.Dispatch); e != nil {
						t.Fatal(e)
					}
					if handlers != 0 {
						t.Fatal("terminal repair entered handler", handlers)
					}
					state, e := outcomes.Get(ctx, identity.Key(h.Type, h.ID))
					if valid {
						if e != nil || string(state.Value) != string(raw) {
							t.Fatal("valid result not projected", e)
						}
					} else if !errors.Is(e, jetstream.ErrKeyNotFound) {
						t.Fatal("unverified result projected", string(state.Value), e)
					}
					reads := blobs.reads
					if graphPort != nil {
						reads = graphPort.reads
					}
					wantReads := 1
					if fault == "inline_conflict" {
						wantReads = 0
					}
					if reads != wantReads {
						t.Fatalf("terminal result reads=%d want=%d", reads, wantReads)
					}
					var count int
					if graph != nil {
						records, _, e := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
						if e != nil {
							t.Fatal(e)
						}
						count = len(records)
						if e = graphPort.CheckReferences(); e != nil {
							t.Fatal(e)
						}
					} else {
						records, _, e := jrn.Read(ctx, h.Type, h.ID)
						if e != nil {
							t.Fatal(e)
						}
						count = len(records)
					}
					if count != 2 {
						t.Fatal("repair changed journal", count)
					}
				}
				deliver(mode, mode == "healthy")
				if mode == "unavailable" || mode == "wrong_bytes" {
					deliver("healthy", true)
				}
			})
		}
	}
}
