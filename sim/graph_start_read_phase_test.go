package sim

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

// These component controls distinguish the phases formerly collapsed into
// "input: context deadline exceeded". They do not model a NATS server cause.
type startReadPhasePort struct {
	graphpublication.Port
	mode             string
	armed            bool
	releaseCommitted bool
}

func (p *startReadPhasePort) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.armed && (p.mode == "open" || (p.mode == "release_committed_unknown" && p.releaseCommitted)) {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.Port.ReadRoot(ctx, key)
}

func (p *startReadPhasePort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if p.armed && (p.mode == "input" || p.mode == "input_and_release") {
		return nil, context.Canceled
	}
	return p.Port.Get(ctx, link, limit)
}

func (p *startReadPhasePort) CASRoot(ctx context.Context, key string, expected uint64, next graphpublication.Root) (graphpublication.Root, error) {
	if p.armed && len(next.Readers) == 0 && (p.mode == "release" || p.mode == "release_committed" || p.mode == "release_committed_unknown" || p.mode == "input_and_release") {
		if p.mode == "release_committed" || p.mode == "release_committed_unknown" {
			if _, err := p.Port.CASRoot(ctx, key, expected, next); err != nil {
				return graphpublication.Root{}, err
			}
			p.releaseCommitted = true
		}
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.Port.CASRoot(ctx, key, expected, next)
}

func TestGraphStartReadErrorPhases(t *testing.T) {
	for _, mode := range []string{"valid", "open", "input", "release", "release_committed", "release_committed_unknown", "input_and_release"} {
		t.Run(mode, func(t *testing.T) {
			model := NewGraphPublicationTransport(NewScheduler(1))
			port := &startReadPhasePort{Port: model, mode: mode}
			protocol := model.Protocol()
			protocol.Port = port
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, Now: func() time.Time { return time.Unix(0, 0).UTC() }, PinTTL: time.Minute, IntentTTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			before, err := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "test", ID: "phase"}, []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			port.armed = true
			state, input, err := graph.ReadStart(ctx, "test", "phase")
			if mode == "valid" || mode == "release_committed" {
				if err != nil || state != before || string(input) != "7" {
					t.Fatalf("valid read: %v %s %v", state, input, err)
				}
			} else {
				phase, cause := "release start reader", error(context.DeadlineExceeded)
				if mode == "open" {
					phase = "open start reader"
				}
				if mode == "input" || mode == "input_and_release" {
					phase, cause = "read start input", context.Canceled
				}
				if !errors.Is(err, cause) || !strings.Contains(err.Error(), phase) {
					t.Fatalf("phase/cause lost: %v", err)
				}
				if (mode == "release" || mode == "release_committed_unknown") && (state != before || string(input) != "7") {
					t.Fatal("release failure lost validated state/input")
				}
			}
			port.armed = false
			status, err := graph.InspectStart(ctx, "test", "phase")
			if err != nil || status == nil || status.State != before {
				t.Fatalf("read changed Start: %v %v", status, err)
			}
			keys, err := model.RootKeys(ctx)
			if err != nil {
				t.Fatal(err)
			}
			pins := 0
			for _, key := range keys {
				root, err := model.ReadRoot(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				pins += len(root.Readers)
			}
			want := 0
			if mode == "release" || mode == "input_and_release" {
				want = 1
			}
			if pins != want {
				t.Fatalf("reader outcome: pins=%d want=%d", pins, want)
			}
		})
	}
}
