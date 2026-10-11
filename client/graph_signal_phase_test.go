package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type signalPhasePort struct {
	*sim.GraphPublicationTransport
	mode                    string
	armed, fired, committed bool
}

func (p *signalPhasePort) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.armed && p.committed && strings.HasSuffix(p.mode, "unknown") {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.ReadRoot(ctx, key)
}
func (p *signalPhasePort) CASBlob(ctx context.Context, key string, rev uint64, f graphpublication.Fence) (graphpublication.Record, error) {
	if p.armed && !p.fired && strings.HasSuffix(p.mode, "prepare") {
		p.fired = true
		return graphpublication.Record{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.CASBlob(ctx, key, rev, f)
}
func (p *signalPhasePort) CASRoot(ctx context.Context, key string, head uint64, next graphpublication.Root) (graphpublication.Root, error) {
	var cursor struct {
		Inputs   uint64 `json:"signal_inputs"`
		Bindings uint64 `json:"signal_bindings"`
	}
	if err := json.Unmarshal(next.Application, &cursor); err != nil {
		return graphpublication.Root{}, err
	}
	target := strings.HasPrefix(p.mode, "reserve_") && cursor.Inputs == 1 || strings.HasPrefix(p.mode, "binding_") && cursor.Bindings == 1
	if p.armed && !p.fired && !strings.HasSuffix(p.mode, "prepare") && target {
		p.fired = true
		if strings.HasSuffix(p.mode, "lost") || strings.HasSuffix(p.mode, "unknown") {
			if _, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, next); err != nil {
				return graphpublication.Root{}, err
			}
			p.committed = true
		}
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, next)
}

type signalPhaseSource struct {
	*sim.SignalTransport
	port *signalPhasePort
}

func (p *signalPhaseSource) PublishSignal(ctx context.Context, m *nats.Msg, id string) (client.SignalPublishAck, error) {
	ack, err := p.SignalTransport.PublishSignal(ctx, m, id)
	if err == nil && strings.HasPrefix(p.port.mode, "binding_") {
		p.port.armed = true
	}
	return ack, err
}
func TestCanonicalSignalMutationErrorPhases(t *testing.T) {
	for _, mode := range []string{"reserve_prepare", "reserve_commit", "reserve_commit_lost", "reserve_commit_unknown", "binding_prepare", "binding_commit", "binding_commit_lost", "binding_commit_unknown"} {
		t.Run(mode, func(t *testing.T) {
			s := sim.NewScheduler(1)
			model := sim.NewGraphPublicationTransport(s)
			port := &signalPhasePort{GraphPublicationTransport: model, mode: mode}
			protocol := model.Protocol()
			protocol.Port = port
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, PinTTL: time.Hour, Now: func() time.Time { return time.Unix(1000, 0).UTC() }})
			if err != nil {
				t.Fatal(err)
			}
			source := &signalPhaseSource{SignalTransport: sim.NewSignalTransport(s), port: port}
			c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			h, err := c.Start(ctx, "signal-phase", "flow", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(mode, "reserve_") {
				port.armed = true
			}
			body := []byte("owned signal input")
			sequence, signalErr := c.Signal(ctx, h.Type, h.ID, "go", body, "key")
			if !port.fired {
				t.Fatal("mutation cut not reached")
			}
			positive := strings.HasSuffix(mode, "lost")
			if positive {
				if signalErr != nil || sequence == 0 {
					t.Fatalf("exact readback not accepted: %d %v", sequence, signalErr)
				}
			} else {
				if !errors.Is(signalErr, client.ErrSignalUnknown) || !errors.Is(signalErr, context.DeadlineExceeded) || sequence != 0 {
					t.Fatalf("unknown outcome/cause lost: %d %v", sequence, signalErr)
				}
				phase := "reserve signal input: commit signal input"
				if mode == "reserve_prepare" {
					phase = "reserve signal input: prepare signal input"
				}
				if mode == "binding_prepare" {
					phase = "prepare signal binding"
				}
				if mode == "binding_commit" {
					phase = "commit signal binding"
				}
				if mode == "binding_commit_unknown" {
					phase = "read signal binding outcome"
				}
				if !strings.Contains(signalErr.Error(), phase) {
					t.Fatalf("phase lost: %s: %v", mode, signalErr)
				}
			}
			port.armed = false
			request := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "go", Key: "key"}
			input, data, found, err := graph.ReadSignalInput(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			expectedInput := !strings.HasPrefix(mode, "reserve_") || port.committed
			if found != expectedInput {
				t.Fatalf("reservation: found=%v expected=%v", found, expectedInput)
			}
			sources := len(source.SignalFor(h.Type, h.ID, "go"))
			wantSources := 0
			if !strings.HasPrefix(mode, "reserve_") || positive {
				wantSources = 1
			}
			if sources != wantSources {
				t.Fatal("uncertain input published source", sources, wantSources)
			}
			wantRuns := 1
			if positive {
				wantRuns = 2
			}
			if len(source.Runs()) != wantRuns {
				t.Fatal("uncertain outcome enqueued", len(source.Runs()))
			}
			if !found {
				if _, err = c.RecoverSignal(ctx, request, ""); !errors.Is(err, client.ErrNotFound) {
					t.Fatal("absent reservation recovered", err)
				}
			} else {
				if !bytes.Equal(data, body) {
					t.Fatal("owned input changed")
				}
				recovered, err := c.RecoverSignal(ctx, request, input.Token)
				if err != nil || recovered == 0 || sequence != 0 && sequence != recovered {
					t.Fatal("original-token recovery", recovered, err)
				}
				binding, data, found, err := graph.ReadSignalBinding(ctx, request)
				if err != nil || !found || binding.Input != input || binding.Sequence != recovered || !bytes.Equal(data, body) {
					t.Fatal("binding changed", binding, err)
				}
				if len(source.SignalFor(h.Type, h.ID, "go")) != 1 || len(source.Runs()) != 2 {
					t.Fatal("recovery duplicated source/wakeup")
				}
			}
			if err = model.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
