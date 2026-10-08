package journal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

func signalInputFixture(t *testing.T, seed int64) (*journal.GraphStore, *sim.GraphPublicationTransport, *time.Time) {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	m := sim.NewGraphPublicationTransport(sim.NewScheduler(seed))
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: func() time.Time { return now }, PinTTL: time.Minute, IntentTTL: time.Second, CanonicalStarts: true, CanonicalSignals: true})
	if err != nil {
		t.Fatal(err)
	}
	start, err := store.ReserveStart(context.Background(), journal.GraphStartRequest{Type: "flow", ID: "id"}, []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindStart(context.Background(), "flow", "id", start.Start.Token, 9); err != nil {
		t.Fatal(err)
	}
	return store, m, &now
}

func TestGraphCanonicalSignalInputsIndexedIdentityAndReopen(t *testing.T) {
	ctx := context.Background()
	for seed := int64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			s, m, now := signalInputFixture(t, seed)
			expected := make([]journal.GraphSignalInput, 0, 64)
			for i := 0; i < 64; i++ {
				r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: fmt.Sprintf("name%d", i%3), Key: fmt.Sprintf("key%d", i)}
				body := []byte(fmt.Sprintf("body:%d", i))
				input, err := s.ReserveSignal(ctx, r, body, false)
				if err != nil || input.Index != uint64(i) {
					t.Fatal(input, err)
				}
				expected = append(expected, input)
				duplicate, err := s.ReserveSignal(ctx, r, body, false)
				if err != nil || duplicate != input {
					t.Fatal("duplicate changed reservation", duplicate, err)
				}
				if _, err = s.ReserveSignal(ctx, r, []byte("mismatch"), false); !errors.Is(err, journal.ErrSignalMismatch) {
					t.Fatal("mismatch adopted", err)
				}
			}
			// No runtime history is needed to own pending inputs; reopened storage
			// resolves a key through a bounded index, not a full history scan.
			reopened, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: func() time.Time { return *now }, CanonicalStarts: true, CanonicalSignals: true})
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range expected {
				r := want.Request
				got, body, found, e := reopened.ReadSignalInput(ctx, r)
				if e != nil || !found || got != want || string(body) != fmt.Sprintf("body:%d", want.Index) {
					t.Fatal(got, found, e)
				}
			}
			missing := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "name", Key: "missing"}
			if _, _, found, e := reopened.ReadSignalInput(ctx, missing); e != nil || found {
				t.Fatal(found, e)
			}
			status, e := s.InspectRetirement(ctx, "flow", "id")
			if e != nil || status.Tail != 0 || status.Invocation != 9 {
				t.Fatal("reservation changed journal", status, e)
			}
			if e = m.CheckReferences(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestGraphCanonicalSignalInputsRetirementAndReplacement(t *testing.T) {
	ctx := context.Background()
	s, m, now := signalInputFixture(t, 21)
	r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "same"}
	body := bytes.Repeat([]byte("owned input"), 1000)
	input, err := s.ReserveSignal(ctx, r, body, false)
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.Open(ctx, r.Type, r.ID, r.Invocation)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := s.Begin(ctx, r.Type, r.ID, r.Invocation)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, r.Type, r.ID, r.Invocation, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, r.Type, r.ID, r.Invocation, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveSignal(ctx, r, body, true); !errors.Is(err, journal.ErrSignalNotRunning) {
		t.Fatal("terminal require-running admitted", err)
	}
	if _, err = s.ReserveSignal(ctx, r, body, false); err != nil {
		t.Fatal("terminal duplicate rejected", err)
	}
	if err = s.FencePurge(ctx, r.Type, r.ID, r.Invocation, tail); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveSignal(ctx, r, body, false); !errors.Is(err, journal.ErrStale) {
		t.Fatal("purging input admitted", err)
	}
	if err = s.Retire(ctx, r.Type, r.ID, r.Invocation, tail); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = s.ReadSignalInput(ctx, r); !errors.Is(err, journal.ErrStale) {
		t.Fatal("retired input freshly opened", err)
	}
	*now = now.Add(2 * time.Second)
	if _, err = m.Protocol().SweepWithReaders(ctx, *now); err != nil {
		t.Fatal(err)
	}
	retained, data, found, err := view.SignalInput(ctx, r)
	if err != nil || !found || retained != input || !bytes.Equal(data, body) {
		t.Fatal("retained input lost", found, err)
	}
	if err = view.Close(ctx); err != nil {
		t.Fatal(err)
	}
	start, err := s.ReserveStart(ctx, journal.GraphStartRequest{Type: r.Type, ID: r.ID}, []byte(`8`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveSignal(ctx, r, body, false); !errors.Is(err, journal.ErrStale) {
		t.Fatal("pending replacement accepted old input", err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, start.Start.Token, 10); err != nil {
		t.Fatal(err)
	}
	r.Invocation = 10
	next, err := s.ReserveSignal(ctx, r, []byte("new"), false)
	if err != nil || next.Index != 0 || next.Token == input.Token {
		t.Fatal("replacement inherited old reservation", next, err)
	}
	if _, _, _, err = view.SignalInput(ctx, r); err == nil {
		t.Fatal("closed old view accepted replacement")
	}
	if err = m.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

type signalCommitFault struct {
	*sim.GraphPublicationTransport
	mode  string
	fired bool
	hook  func() error
}

func (p *signalCommitFault) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if !p.fired {
		for _, forest := range root.Streams {
			if forest.Name == "signal-input" && forest.Graph.Count == 1 {
				p.fired = true
				if p.hook != nil {
					if err := p.hook(); err != nil {
						return root, err
					}
					break
				}
				fault := sim.DropBeforeCommit
				if p.mode == "lost_readback" {
					fault = sim.LoseAckAfterCommit
					p.PauseBefore("cas_root", func() error { return p.QueueFault("read_root", sim.DropBeforeCommit) })
				}
				if err := p.QueueFault("cas_root", fault); err != nil {
					return root, err
				}
				break
			}
		}
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

func TestGraphCanonicalSignalInputsUnknownAndReadFailures(t *testing.T) {
	for _, mode := range []string{"drop", "lost_readback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			m := sim.NewGraphPublicationTransport(sim.NewScheduler(22))
			port := &signalCommitFault{GraphPublicationTransport: m, mode: mode}
			protocol := m.Protocol()
			protocol.Port = port
			s, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true})
			if err != nil {
				t.Fatal(err)
			}
			start, err := s.ReserveStart(ctx, journal.GraphStartRequest{Type: "flow", ID: "id"}, []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			if err = s.BindStart(ctx, "flow", "id", start.Start.Token, 9); err != nil {
				t.Fatal(err)
			}
			r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "key"}
			attempt, err := s.ReserveSignal(ctx, r, []byte("input"), false)
			if !errors.Is(err, journal.ErrUnknown) || !port.fired {
				t.Fatal("uncertain commit called successful", attempt, err)
			}
			reopened, e := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
			if e != nil {
				t.Fatal(e)
			}
			got, data, found, e := reopened.ReadSignalInput(ctx, r)
			if e != nil || found != (mode == "lost_readback") || found && (got != attempt || string(data) != "input") {
				t.Fatal("uncertain reservation not resolved exactly", got, found, e)
			}
			if !found {
				if _, e = reopened.ReserveSignal(ctx, r, []byte("input"), false); e != nil {
					t.Fatal(e)
				}
			}
			if e = m.QueueFault("get", sim.DropBeforeCommit); e != nil {
				t.Fatal(e)
			}
			if _, _, found, e = reopened.ReadSignalInput(ctx, r); e == nil || found {
				t.Fatal("failed referenced read became absence/success", found, e)
			}
			if e = m.CheckReferences(); e != nil {
				t.Fatal(e)
			}
		})
	}
}

// Both publishers prepare against the same head; only the canonical winner may
// own an input. Pausing actual CAS makes this a reproducible production API cut.
func TestGraphCanonicalSignalInputsPublicationInterleavings(t *testing.T) {
	for _, mode := range []string{"same_key", "different_key", "retire"} {
		for seed := int64(1); seed <= 16; seed++ {
			t.Run(fmt.Sprintf("%s/seed%d", mode, seed), func(t *testing.T) {
				ctx := context.Background()
				winner, m, now := signalInputFixture(t, seed)
				if mode == "retire" {
					tail, e := winner.Begin(ctx, "flow", "id", 9)
					if e != nil {
						t.Fatal(e)
					}
					tail, e = winner.Append(ctx, "flow", "id", 9, journal.Entry{Kind: journal.Started}, tail, nil, nil)
					if e != nil {
						t.Fatal(e)
					}
					_, e = winner.Append(ctx, "flow", "id", 9, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
					if e != nil {
						t.Fatal(e)
					}
				}
				r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "key"}
				other := r
				if mode == "different_key" {
					other.Key = "other"
				}
				var committed journal.GraphSignalInput
				port := &signalCommitFault{GraphPublicationTransport: m}
				port.hook = func() (err error) {
					if mode == "retire" {
						return winner.Retire(ctx, r.Type, r.ID, r.Invocation, 2)
					}
					committed, err = winner.ReserveSignal(ctx, other, []byte("winner"), false)
					return err
				}
				protocol := m.Protocol()
				protocol.Port = port
				loser, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Now: func() time.Time { return *now }, CanonicalStarts: true, CanonicalSignals: true})
				if err != nil {
					t.Fatal(err)
				}
				_, err = loser.ReserveSignal(ctx, r, []byte("loser"), false)
				if !port.fired || !errors.Is(err, journal.ErrStale) {
					t.Fatal("stale prepared input committed", port.fired, err)
				}
				if mode == "retire" {
					if _, _, _, err = winner.ReadSignalInput(ctx, r); !errors.Is(err, journal.ErrStale) {
						t.Fatal(err)
					}
				} else {
					got, data, found, e := winner.ReadSignalInput(ctx, other)
					if e != nil || !found || got != committed || string(data) != "winner" {
						t.Fatal(got, found, e)
					}
					if mode == "same_key" {
						if _, err = loser.ReserveSignal(ctx, r, []byte("loser"), false); !errors.Is(err, journal.ErrSignalMismatch) {
							t.Fatal(err)
						}
					} else {
						got, _, found, e = winner.ReadSignalInput(ctx, r)
						if e != nil || found {
							t.Fatal("uncommitted staged input adopted", got, found, e)
						}
						got, err = loser.ReserveSignal(ctx, r, []byte("loser"), false)
						if err != nil || got.Index != 1 {
							t.Fatal(got, err)
						}
					}
				}
				if err = m.CheckReferences(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestGraphCanonicalSignalInputsSchemaAndExpiry(t *testing.T) {
	ctx := context.Background()
	s, m, now := signalInputFixture(t, 77)
	for _, starts := range []bool{false, true} {
		old, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalStarts: starts})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = old.InspectRetirement(ctx, "flow", "id"); !errors.Is(err, journal.ErrGap) {
			t.Fatal("old schema reader admitted new root", starts, err)
		}
	}
	if _, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalSignals: true}); err == nil {
		t.Fatal("Signal without Start accepted")
	}
	if _, err := journal.NativeGraphStreamConfigs(journal.NativeGraphConfig{AuthorityStream: "SIGNAL_AUTH", AuthorityPrefix: "wf.graph.signal", ObjectBucket: "SIGNAL_OBJECTS", CanonicalSignals: true}, 1); err == nil {
		t.Fatal("native Signal without Start accepted")
	}
	r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "key"}
	input, err := s.ReserveSignal(ctx, r, []byte("input"), false)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Open(ctx, r.Type, r.ID, r.Invocation)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Minute)
	for _, key := range []string{"key", "missing"} {
		r.Key = key
		if _, _, found, e := v.SignalInput(ctx, r); e == nil || found {
			t.Fatal("expired pin returned presence or absence", found, e)
		}
	}
	if input.Index != 0 {
		t.Fatal(input)
	}
}

func TestGraphCanonicalSignalInputsNamesPointersAndReadExpiry(t *testing.T) {
	ctx := context.Background()
	s, m, now := signalInputFixture(t, 88)
	r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "first", Key: "same"}
	first, err := s.ReserveSignal(ctx, r, []byte("input"), false)
	if err != nil {
		t.Fatal(err)
	}
	r.Name = "second"
	second, err := s.ReserveSignal(ctx, r, []byte("other"), false)
	if err != nil || second.Index != 1 || second.Token == first.Token {
		t.Fatal("name not part of identity", second, err)
	}
	message := func() *jetstream.RawStreamMsg {
		return &jetstream.RawStreamMsg{Subject: "wf.sig.flow.id.first", Sequence: 1, Data: first.PointerBytes(), Header: nats.Header{journal.GraphSignalTokenHeader: []string{first.Token}, "Wf-Input-SHA256": []string{first.InputSHA256}, "Wf-Inv-Seq": []string{"9"}}}
	}
	if !first.MatchesSignal(message()) {
		t.Fatal("exact pointer rejected")
	}
	mutations := []func(*jetstream.RawStreamMsg){
		func(m *jetstream.RawStreamMsg) { m.Sequence = 0 },
		func(m *jetstream.RawStreamMsg) { m.Subject = "wf.sig.flow.id.second" },
		func(m *jetstream.RawStreamMsg) { m.Data = second.PointerBytes() },
		func(m *jetstream.RawStreamMsg) { m.Header.Set(journal.GraphSignalTokenHeader, second.Token) },
		func(m *jetstream.RawStreamMsg) { m.Header.Set("Wf-Input-SHA256", second.InputSHA256) },
		func(m *jetstream.RawStreamMsg) { m.Header.Set("Wf-Inv-Seq", "10") },
		func(m *jetstream.RawStreamMsg) { m.Header.Set("Wf-Signal-Ref", "legacy") },
	}
	for i, mutate := range mutations {
		msg := message()
		mutate(msg)
		if first.MatchesSignal(msg) {
			t.Fatal("altered source matched", i)
		}
	}
	if first.MatchesSignal(nil) {
		t.Fatal("nil source matched")
	}
	view, err := s.Open(ctx, "flow", "id", 9)
	if err != nil {
		t.Fatal(err)
	}
	m.PauseBefore("get", func() error { *now = now.Add(2 * time.Minute); return nil })
	if _, _, found, e := view.SignalInput(ctx, first.Request); e == nil || found {
		t.Fatal("expired during owned read returned success", found, e)
	}
}
