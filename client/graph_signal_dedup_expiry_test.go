package client_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/sim"
)

func TestCanonicalSignalRecoveryAfterDedupExpiry(t *testing.T) {
	for _, mode := range []string{"binding_commit", "binding_commit_unknown"} {
		t.Run(mode, func(t *testing.T) {
			for _, window := range []struct {
				name   string
				millis int64
			}{{"within", 119999}, {"boundary", 120000}} {
				t.Run(window.name, func(t *testing.T) {
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
					h, err := c.Start(ctx, "dedup-expiry", "flow", []byte(`7`))
					if err != nil {
						t.Fatal(err)
					}
					body := []byte("owned signal")
					seq, err := c.Signal(ctx, h.Type, h.ID, "go", body, "key")
					if seq != 0 || !errors.Is(err, client.ErrSignalUnknown) || !port.fired {
						t.Fatal("unknown cut", seq, err)
					}
					port.armed = false
					request := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "go", Key: "key"}
					input, data, found, err := graph.ReadSignalInput(ctx, request)
					if err != nil || !found || !bytes.Equal(data, body) {
						t.Fatal("original input", input, err)
					}
					messages := source.SignalFor(h.Type, h.ID, "go")
					if len(messages) != 1 || len(source.Runs()) != 1 {
						t.Fatal("initial source/wakeup")
					}
					first := messages[0].Sequence
					before, _, bound, err := graph.ReadSignalBinding(ctx, request)
					if err != nil || bound != (mode == "binding_commit_unknown") {
						t.Fatal("commit outcome", before, bound, err)
					}
					if err = s.AdvanceMillis(window.millis); err != nil {
						t.Fatal(err)
					}
					seq, err = c.RecoverSignal(ctx, request, input.Token)
					if err != nil || seq != first {
						t.Fatal("original-token recovery", seq, err)
					}
					messages = source.SignalFor(h.Type, h.ID, "go")
					want := 1
					if mode == "binding_commit" && window.name == "boundary" {
						want = 2
					}
					if len(messages) != want || len(source.Runs()) != 2 {
						t.Fatal("source/wakeup counts", len(messages), want, len(source.Runs()))
					}
					for _, m := range messages {
						raw, e := source.SignalBySequence(ctx, m.Sequence)
						if e != nil || !input.MatchesSignal(raw) {
							t.Fatal("duplicate provenance", m)
						}
					}
					binding, data, found, err := graph.ReadSignalBinding(ctx, request)
					if err != nil || !found || binding.Input != input || binding.Sequence != first || binding.Index != 0 || !bytes.Equal(data, body) {
						t.Fatal("first binding", binding, err)
					}
					through := messages[len(messages)-1].Sequence
					if want == 2 {
						progress, err := graph.BindNextSignal(ctx, h.Type, h.ID, h.InvSeq, through, source)
						if err != nil || !progress {
							t.Fatal("duplicate frontier", progress, err)
						}
					}
					progress, err := graph.BindNextSignal(ctx, h.Type, h.ID, h.InvSeq, through, source)
					if err != nil || progress {
						t.Fatal("exhausted frontier", progress, err)
					}
					after, data, found, err := graph.ReadSignalBinding(ctx, request)
					if err != nil || !found || after != binding || !bytes.Equal(data, body) {
						t.Fatal("duplicate replaced first binding", after, err)
					}
					retained, data, found, err := graph.ReadSignalInput(ctx, request)
					if err != nil || !found || retained != input || !bytes.Equal(data, body) {
						t.Fatal("reservation changed", retained, err)
					}
					if err = model.CheckReferences(); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
