package sim

import (
	"context"
	"errors"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"testing"
	"time"
)

type closeReadCounter struct {
	graphpublication.Port
	mode             string
	reads, attempts  int
	armed, committed bool
}

func (p *closeReadCounter) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.armed {
		p.reads++
		if p.mode == "observe_conflict" {
			return graphpublication.Root{}, graphpublication.ErrConflict
		}
		if p.mode == "commit_unknown" && p.committed {
			return graphpublication.Root{}, context.DeadlineExceeded
		}
	}
	return p.Port.ReadRoot(ctx, key)
}
func (p *closeReadCounter) CASRoot(ctx context.Context, key string, head uint64, next graphpublication.Root) (graphpublication.Root, error) {
	if !p.armed {
		return p.Port.CASRoot(ctx, key, head, next)
	}
	p.attempts++
	if p.mode == "cas_conflict" && p.attempts == 1 {
		old, err := p.Port.ReadRoot(ctx, key)
		if err != nil {
			return old, err
		}
		if _, err = p.Port.CASRoot(ctx, key, old.Head, old); err != nil {
			return graphpublication.Root{}, err
		}
		return graphpublication.Root{}, graphpublication.ErrConflict
	}
	if p.mode == "cas_drop" {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	ack, err := p.Port.CASRoot(ctx, key, head, next)
	if err != nil {
		return ack, err
	}
	if p.mode == "commit_lost" || p.mode == "commit_unknown" || p.mode == "overtaken_commit" {
		p.committed = true
		if p.mode == "overtaken_commit" {
			if _, err = p.Port.CASRoot(ctx, key, ack.Head, ack); err != nil {
				return graphpublication.Root{}, err
			}
		}
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return ack, nil
}
func TestGraphViewCloseSingleAuthorityObservation(t *testing.T) {
	for _, mode := range []string{"valid", "observe_conflict", "cas_conflict", "cas_drop", "commit_lost", "commit_unknown", "overtaken_commit"} {
		t.Run(mode, func(t *testing.T) {
			model := NewGraphPublicationTransport(NewScheduler(1))
			port := &closeReadCounter{Port: model, mode: mode}
			protocol := model.Protocol()
			protocol.Port = port
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, Now: func() time.Time { return time.Unix(0, 0).UTC() }, PinTTL: time.Minute, IntentTTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if _, err = graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "test", ID: "close"}, []byte(`7`)); err != nil {
				t.Fatal(err)
			}
			view, err := graph.OpenStart(ctx, "test", "close")
			if err != nil {
				t.Fatal(err)
			}
			port.armed = true
			err = view.Close(ctx)
			reads, attempts := 1, 1
			if mode == "observe_conflict" {
				attempts = 0
			} else if mode != "valid" {
				reads = 2
			}
			if mode == "cas_conflict" {
				attempts = 2
			}
			if port.reads != reads || port.attempts != attempts {
				t.Fatalf("mode=%s observations=%d/%d attempts=%d/%d", mode, port.reads, reads, port.attempts, attempts)
			}
			if mode == "valid" || mode == "cas_conflict" || mode == "commit_lost" {
				if err != nil {
					t.Fatal(err)
				}
				if err = view.Close(ctx); err != nil || port.reads != reads || port.attempts != attempts {
					t.Fatal("closed view repeated authority work", err)
				}
			} else if mode == "observe_conflict" {
				if !errors.Is(err, graphpublication.ErrConflict) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			keys, e := model.RootKeys(ctx)
			if e != nil {
				t.Fatal(e)
			}
			pins := 0
			for _, key := range keys {
				root, e := model.ReadRoot(ctx, key)
				if e != nil {
					t.Fatal(e)
				}
				pins += len(root.Readers)
			}
			wantPins := 0
			if mode == "observe_conflict" || mode == "cas_drop" {
				wantPins = 1
			}
			if pins != wantPins {
				t.Fatalf("pins=%d want=%d", pins, wantPins)
			}
		})
	}
}
