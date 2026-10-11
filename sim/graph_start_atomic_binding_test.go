package sim

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

type atomicStartBindingPort struct {
	graphpublication.Port
	mode                           string
	armed, committed               bool
	writes, bindings, cleanupReads int
}

func (p *atomicStartBindingPort) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.armed && p.mode == "committed_unknown" && p.committed {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	if p.armed && p.mode == "cleanup_observation" {
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= 3*time.Second {
			p.cleanupReads++
			return graphpublication.Root{}, context.DeadlineExceeded
		}
	}
	return p.Port.ReadRoot(ctx, key)
}
func (p *atomicStartBindingPort) Get(ctx context.Context, l retainedgraph.Link, n int) ([]byte, error) {
	if p.armed && p.mode == "corrupt_input" {
		return []byte("invalid owned leaf"), nil
	}
	return p.Port.Get(ctx, l, n)
}
func (p *atomicStartBindingPort) CASRoot(ctx context.Context, key string, n uint64, next graphpublication.Root) (graphpublication.Root, error) {
	if !p.armed {
		return p.Port.CASRoot(ctx, key, n, next)
	}
	p.writes++
	var app struct {
		Invocation uint64 `json:"invocation"`
	}
	if err := json.Unmarshal(next.Application, &app); err != nil {
		return graphpublication.Root{}, err
	}
	binding := app.Invocation == 9
	if binding {
		p.bindings++
		if p.mode == "rejected" {
			return graphpublication.Root{}, context.DeadlineExceeded
		}
		if p.mode == "conflict" {
			return graphpublication.Root{}, graphpublication.ErrConflict
		}
	}
	root, err := p.Port.CASRoot(ctx, key, n, next)
	if err == nil && binding {
		p.committed = true
		if p.mode == "committed_lost" || p.mode == "committed_unknown" {
			return graphpublication.Root{}, context.DeadlineExceeded
		}
	}
	return root, err
}

func TestGraphStartBindingAndReaderReleaseAtomic(t *testing.T) {
	for _, mode := range []string{"success", "cleanup_observation", "wrong_token", "corrupt_input", "conflict", "rejected", "committed_lost", "committed_unknown"} {
		t.Run(mode, func(t *testing.T) {
			model := NewGraphPublicationTransport(NewScheduler(1))
			port := &atomicStartBindingPort{Port: model, mode: mode}
			protocol := model.Protocol()
			protocol.Port = port
			config := journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, Now: func() time.Time { return time.Unix(0, 0).UTC() }, PinTTL: time.Minute, IntentTTL: time.Second}
			graph, err := journal.NewGraphStore(config)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pending, err := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "test", ID: "atomic"}, []byte("7"))
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := graph.OpenStart(ctx, "test", "atomic")
			if err != nil {
				t.Fatal(err)
			}
			token := pending.Start.Token
			if mode == "wrong_token" {
				token = "wrong"
			}
			port.armed = true
			err = graph.BindStart(ctx, "test", "atomic", token, 9)
			bound := mode == "success" || mode == "cleanup_observation" || mode == "committed_lost" || mode == "committed_unknown"
			known := bound && mode != "committed_unknown"
			if known && err != nil {
				t.Fatal("binding failed", err)
			}
			if !known {
				cause := error(context.DeadlineExceeded)
				if mode == "wrong_token" || mode == "conflict" {
					cause = journal.ErrStale
				}
				if mode == "corrupt_input" {
					cause = retainedgraph.ErrInvalid
				}
				if !errors.Is(err, cause) {
					t.Fatal("classification lost", err, cause)
				}
			}
			if ctx.Err() != nil {
				t.Fatal("request error expired parent")
			}
			wantBindings := 1
			if mode == "wrong_token" || mode == "corrupt_input" {
				wantBindings = 0
			}
			if port.bindings != wantBindings {
				t.Fatal("binding retried or skipped validation", port.bindings, wantBindings)
			}
			if known && (port.writes != 2 || port.cleanupReads != 0) {
				t.Fatal("successful binding used separate release transaction", port.writes, port.cleanupReads)
			}
			port.armed = false
			status, err := graph.InspectStart(ctx, "test", "atomic")
			if err != nil || status == nil || status.State.Start != pending.Start {
				t.Fatal("Start descriptor changed", status, err)
			}
			if bound && (status.State.Pending || status.State.Invocation != 9) || !bound && (!status.State.Pending || status.State.Invocation != 0) {
				t.Fatal("wrong authoritative binding", status.State)
			}
			if got, err := foreign.StartInput(ctx); err != nil || string(got) != "7" {
				t.Fatal("foreign reader lost input", string(got), err)
			}
			if err = foreign.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if bound {
				reopened, err := journal.NewGraphStore(config)
				if err != nil {
					t.Fatal(err)
				}
				if err = reopened.BindStart(ctx, "test", "atomic", pending.Start.Token, 9); err != nil {
					t.Fatal("explicit recovery of exact binding", err)
				}
				if err = reopened.BindStart(ctx, "test", "atomic", pending.Start.Token, 10); !errors.Is(err, journal.ErrStale) {
					t.Fatal("replacement sequence admitted", err)
				}
			}
			if err = model.CheckReferences(); err != nil {
				t.Fatal(err)
			}
			keys, err := model.RootKeys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				root, err := model.ReadRoot(ctx, key)
				if err != nil || len(root.Readers) != 0 {
					t.Fatal("reader leak", root, err)
				}
			}
		})
	}
}
