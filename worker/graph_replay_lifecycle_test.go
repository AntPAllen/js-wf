package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type replayLifecyclePort struct {
	*sim.GraphPublicationTransport
	hook  func() error
	after bool
	fired bool
}

func (p *replayLifecyclePort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	hook := p.hook
	if hook != nil && len(root.Readers) > 0 {
		p.hook = nil // Lifecycle mutations must not recursively trigger this cut.
		p.fired = true
		if !p.after {
			if err := hook(); err != nil {
				return graphpublication.Root{}, err
			}
		}
	}
	result, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	if err == nil && hook != nil && p.fired && p.after {
		if e := hook(); e != nil {
			return graphpublication.Root{}, e
		}
	}
	return result, err
}

func TestGraphReplaySnapshotLifecycleCut(t *testing.T) {
	for _, after := range []bool{false, true} {
		for _, replace := range []bool{false, true} {
			name := "before-pin/retire"
			if after {
				name = "after-pin/retire"
			}
			if replace {
				name += "-replace"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				scheduler := sim.NewScheduler(41)
				port := &replayLifecyclePort{GraphPublicationTransport: sim.NewGraphPublicationTransport(scheduler), after: after}
				store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: graphpublication.Protocol{Port: port}, CanonicalStarts: true, CanonicalSignals: true})
				if err != nil {
					t.Fatal(err)
				}
				transport := sim.NewWorkerTransport(scheduler, 3*time.Second)
				c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
				if err != nil {
					t.Fatal(err)
				}
				appendHistory := func(h client.Handle, input, result string, epoch uint64) error {
					status, e := store.InspectStart(ctx, h.Type, h.ID)
					if e != nil {
						return e
					}
					tail, e := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
					if e != nil {
						return e
					}
					started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
					tail, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Epoch: epoch, Payload: started}, tail, [][]byte{[]byte(input)}, nil)
					if e != nil {
						return e
					}
					terminal, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: []byte(result)})
					_, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Epoch: epoch, Index: 1, Payload: terminal}, tail, nil, nil)
					return e
				}
				h, err := c.Start(ctx, "test", "lifecycle-export", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				if err = appendHistory(h, "7", "42", 3); err != nil {
					t.Fatal(err)
				}
				source, err := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
				if err != nil {
					t.Fatal(err)
				}
				var replacement client.Handle
				port.hook = func() error {
					if e := store.Retire(ctx, h.Type, h.ID, h.InvSeq, 2); e != nil {
						return e
					}
					if replace {
						// The native invocation subject is removed last by purge.
						// Reuse must not overwrite its retained write-once pointer.
						transport.PurgeInvocation(identity.InvocationSubject(h.Type, h.ID))
						var e error
						replacement, e = c.Start(ctx, h.Type, h.ID, []byte(`8`))
						if e != nil {
							return e
						}
						return appendHistory(replacement, "8", "99", 0)
					}
					return nil
				}
				snapshot, err := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
				if !port.fired {
					t.Fatal("lifecycle cut did not execute")
				}
				checkEmpty := func(s worker.GraphReplaySnapshot) {
					t.Helper()
					if s.Format != "" || s.InputHash != "" || s.Input != nil || s.Records != nil || s.Objects != nil || s.PendingSignal != nil {
						t.Fatal("failed export leaked partial snapshot", s)
					}
				}
				check := func(s worker.GraphReplaySnapshot, input, result string, invocation, epoch uint64) {
					t.Helper()
					if s.Format != wf.ReplayFormatGraphV1 || string(s.Input) != input || len(s.Records) != 2 || s.Records[0].Index != 0 || s.Records[0].Epoch != epoch {
						t.Fatal("mixed-generation snapshot", s)
					}
					var out wf.Outcome
					if json.Unmarshal(s.Records[1].Payload, &out) != nil || out.InvSeq != invocation || string(out.Result) != result {
						t.Fatal("mixed-generation terminal", out)
					}
				}
				if after {
					if err != nil {
						t.Fatal(err)
					}
					check(snapshot, "7", "42", h.InvSeq, 3)
				} else {
					if !errors.Is(err, journal.ErrStale) {
						t.Fatal("fresh reader admitted retired generation", err)
					}
					checkEmpty(snapshot)
				}
				stale, err := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
				if !errors.Is(err, journal.ErrStale) {
					t.Fatal("old source reacquired after retirement", err)
				}
				checkEmpty(stale)
				if replace {
					if replacement.InvSeq == h.InvSeq {
						t.Fatal("generation did not change")
					}
					current, e := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
					if e != nil {
						t.Fatal(e)
					}
					fresh, e := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, current)
					if e != nil {
						t.Fatal(e)
					}
					check(fresh, "8", "99", replacement.InvSeq, 0)
				}
			})
		}
	}
}
