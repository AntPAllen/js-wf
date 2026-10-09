package reconcile_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"
)

// Forbid both payload access and reader mutation during recovery discovery.
type continuationMetadataPort struct {
	*sim.GraphPublicationTransport
	metadataOnly                       bool
	forbiddenReads, forbiddenMutations int
}

func (p *continuationMetadataPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if p.metadataOnly {
		p.forbiddenReads++
		return nil, fmt.Errorf("recovery opened payload")
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}
func (p *continuationMetadataPort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if p.metadataOnly {
		p.forbiddenMutations++
		return graphpublication.Root{}, fmt.Errorf("recovery mutated reader/root")
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

type continuationSource struct {
	*sim.SignalTransport
	forged       bool
	beforeLookup func() error
}

func (p *continuationSource) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if p.beforeLookup != nil {
		f := p.beforeLookup
		p.beforeLookup = nil
		if err := f(); err != nil {
			return nil, err
		}
	}
	msg, err := p.SignalTransport.LastInvocation(ctx, subject)
	if err == nil && p.forged {
		msg.Header.Set(journal.GraphStartTokenHeader, "forged")
	}
	return msg, err
}

type continuationRecovery struct {
	c             *client.Client
	held, unknown bool
	before        func() error
}

func (p *continuationRecovery) GraphRepairBlocked(context.Context, string, string) (bool, error) {
	if p.unknown {
		return false, context.DeadlineExceeded
	}
	return p.held, nil
}
func (p *continuationRecovery) RepairContinuationAttempt(ctx context.Context, typ, id, token string, inv, seq uint64) (bool, error) {
	if p.before != nil {
		f := p.before
		p.before = nil
		if err := f(); err != nil {
			return false, err
		}
	}
	return p.c.RepairContinuationAttempt(ctx, typ, id, token, inv, seq)
}

func TestGraphContinuationRecoveryDecisions(t *testing.T) {
	for _, testMode := range []string{"boundary", "before-suspension", "held", "lease-unknown", "source-forged", "publish-before", "publish-ack", "partial-publish", "authority-unknown", "later-wait", "terminal", "retired", "purging", "wait-race", "post-source-wait-race", "unindexed-boundary", "unindexed-held", "unindexed-lease-unknown", "unindexed-source-forged", "unindexed-publish-before", "unindexed-publish-ack", "unindexed-later-wait", "unindexed-wait-race", "unindexed-post-source-wait-race", "unindexed-ordinary", "unindexed-completion-race", "unindexed-pointer-race"} {
		t.Run(testMode, func(t *testing.T) {
			unindexed := strings.HasPrefix(testMode, "unindexed-")
			mode := strings.TrimPrefix(testMode, "unindexed-")
			ctx := context.Background()
			scheduler := sim.NewScheduler(31)
			model := sim.NewGraphPublicationTransport(scheduler)
			port := &continuationMetadataPort{GraphPublicationTransport: model}
			protocol := model.Protocol()
			protocol.Port = port
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true})
			if err != nil {
				t.Fatal(err)
			}
			source := &continuationSource{SignalTransport: sim.NewSignalTransport(scheduler)}
			c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
			if err != nil {
				t.Fatal(err)
			}
			budget := 1
			if mode == "partial-publish" {
				if _, err := c.Start(ctx, "test", "earlier", []byte(`7`)); err != nil {
					t.Fatal(err)
				}
				budget = 2
			}
			h, err := c.Start(ctx, "test", "resume", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			status, err := graph.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			index := uint64(0)
			appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) error {
				var err error
				tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: index, Epoch: 3, Kind: kind, Payload: payload}, tail, objects, nil)
				if err == nil {
					index++
				}
				return err
			}
			mustAppend := func(kind journal.Kind, payload []byte, objects ...[]byte) {
				t.Helper()
				if err := appendRecord(kind, payload, objects...); err != nil {
					t.Fatal(err)
				}
			}
			started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
			mustAppend(journal.Started, started, []byte(`7`))
			locals := json.RawMessage(`{"count":1}`)
			digest := sha256.Sum256(locals)
			frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: 2, Epoch: 3}, StepPosition: 2}
			encoded, hash, err := checkpoint.Encode(frame)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
			completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
			if mode == "ordinary" {
				request = []byte(`{"kind":"run","name":"ordinary"}`)
				completion = []byte(`{"result":42}`)
			}
			mustAppend(journal.StepRequested, request)
			mustAppend(journal.StepCompleted, completion, encoded)
			view, err := graph.OpenExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			found, err := view.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if !unindexed {
				if err := graph.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
					t.Fatal(err)
				}
			}
			if !unindexed && mode != "before-suspension" {
				mustAppend(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`))
			}
			laterWait := func() error {
				if err := appendRecord(journal.StepRequested, []byte(`{"kind":"timer","name":"sleep"}`)); err != nil {
					return err
				}
				return appendRecord(journal.Suspended, []byte(`{"waiting_on":"timer"}`))
			}
			if mode == "later-wait" {
				if err := laterWait(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "terminal" || mode == "retired" || mode == "purging" {
				outcome, _ := json.Marshal(map[string]any{"inv_seq": h.InvSeq, "result": json.RawMessage(`42`)})
				mustAppend(journal.Completed, outcome)
				if mode == "retired" {
					if err := graph.Retire(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "purging" {
					if err := graph.FencePurge(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
						t.Fatal(err)
					}
				}
			}
			recovery := &continuationRecovery{c: c, held: mode == "held", unknown: mode == "lease-unknown"}
			if mode == "wait-race" {
				recovery.before = func() error {
					port.metadataOnly = false
					defer func() { port.metadataOnly = true }()
					return laterWait()
				}
			}
			if mode == "post-source-wait-race" {
				source.beforeLookup = func() error {
					port.metadataOnly = false
					defer func() { port.metadataOnly = true }()
					return laterWait()
				}
			}
			if mode == "completion-race" || mode == "pointer-race" {
				recovery.before = func() error {
					port.metadataOnly = false
					defer func() { port.metadataOnly = true }()
					if mode == "pointer-race" {
						return graph.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail)
					}
					if err := appendRecord(journal.StepRequested, []byte(`{"kind":"run","name":"later"}`)); err != nil {
						return err
					}
					return appendRecord(journal.StepCompleted, []byte(`{"result":42}`))
				}
			}
			source.forged = mode == "source-forged"
			scan, err := reconcile.NewCanonicalContinuationScanWithPort(graph, recovery, recovery)
			if err != nil {
				t.Fatal(err)
			}
			port.metadataOnly = true
			before := len(source.Runs())
			dry, err := scan.Scan(ctx, 1, budget, true)
			if err != nil && mode != "lease-unknown" {
				t.Fatal("dry run", dry, err)
			}
			if len(source.Runs()) != before {
				t.Fatal("dry run dispatched")
			}
			if mode == "publish-before" || mode == "publish-ack" || mode == "partial-publish" {
				kind := "drop_before_commit"
				if mode == "publish-ack" {
					kind = "lose_ack_after_commit"
				}
				if err := source.QueueFault(sim.StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "authority-unknown" {
				if err := model.QueueFault("read_root", sim.DropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			result, err := scan.Scan(ctx, 1, budget, false)
			runs := len(source.Runs()) - before
			switch mode {
			case "boundary", "before-suspension", "ordinary", "pointer-race":
				if err != nil || result.Reenqueued != 1 || runs != 1 || len(dry.Candidates) != 1 {
					t.Fatal(result, err, runs, dry)
				}
				retry, err := scan.Scan(ctx, 1, 1, false)
				if err != nil || retry.Reenqueued != 1 || len(source.Runs())-before != 2 {
					t.Fatal("dedup suppressed recovery", retry, err)
				}
			case "held", "later-wait", "terminal", "retired", "purging", "wait-race", "post-source-wait-race", "completion-race":
				if err != nil || result.Reenqueued != 0 || runs != 0 {
					t.Fatal(result, err, runs)
				}
			case "partial-publish":
				if !errors.Is(err, journal.ErrUnknown) || result.Inspected != 2 || result.NextSequence <= 1 || result.RetrySequence != result.NextSequence || runs != 0 {
					t.Fatal("confirmed prefix not preserved", result, err, runs)
				}
				// Quorum witnesses may move the earlier noncandidate into the
				// refreshed catalog bound. Consume that bounded entry too.
				retry, err := scan.Scan(ctx, result.RetrySequence, 8, false)
				if err != nil || retry.Reenqueued != 1 || len(source.Runs())-before != 1 {
					t.Fatal("prefix recovery retry", retry, err)
				}
			default:
				want := 0
				if mode == "publish-ack" {
					want = 1
				}
				if !errors.Is(err, journal.ErrUnknown) || result.NextSequence != 1 || runs != want {
					t.Fatal("uncertainty advanced cursor or authorized dispatch", result, err, runs)
				}
				if mode == "publish-before" || mode == "publish-ack" {
					retry, err := scan.Scan(ctx, 1, 1, false)
					if err != nil || retry.Reenqueued != 1 || len(source.Runs())-before != want+1 {
						t.Fatal("publication retry", retry, err)
					}
				}
			}
			if port.forbiddenReads != 0 || port.forbiddenMutations != 0 {
				t.Fatal("recovery touched payload or root ownership", port.forbiddenReads, port.forbiddenMutations)
			}
			t.Logf("mode=%s runs=%d result=%+v error=%v", testMode, runs, result, err)
		})
	}
}

func TestGraphContinuationRecoveryRequiresIndex(t *testing.T) {
	ctx := context.Background()
	model := sim.NewGraphPublicationTransport(sim.NewScheduler(31))
	v4, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
	if err != nil {
		t.Fatal(err)
	}
	port := &continuationRecovery{}
	for _, graph := range []*journal.GraphStore{nil, v4} {
		if _, err := reconcile.NewCanonicalContinuationScanWithPort(graph, port, port); err == nil {
			t.Fatal("accepted missing checkpoint index")
		}
	}
	if err := reconcile.RunRepairLoopWithGraphJournal(ctx, nil, "test", "graph-continuation", 1, 1, v4, nil, nil, nil); err == nil {
		t.Fatal("loop accepted v4")
	}
}
