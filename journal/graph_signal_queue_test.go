package journal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/sim"
)

func queueSource(input journal.GraphSignalInput) *nats.Msg {
	r := input.Request
	return &nats.Msg{Subject: "wf.sig." + r.Type + "." + r.ID + "." + r.Name, Data: input.PointerBytes(), Header: nats.Header{journal.GraphSignalTokenHeader: []string{input.Token}, "Wf-Input-SHA256": []string{input.InputSHA256}, "Wf-Inv-Seq": []string{strconv.FormatUint(r.Invocation, 10)}}}
}
func TestGraphCanonicalSignalQueueOrderAndPersistentDuplicates(t *testing.T) {
	for seed := int64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			store, m, _ := signalInputFixture(t, seed)
			source := sim.NewSignalTransport(sim.NewScheduler(seed))
			inputs := make([]journal.GraphSignalInput, 32)
			for i := range inputs {
				r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: fmt.Sprint(i)}
				var err error
				inputs[i], err = store.ReserveSignal(ctx, r, []byte(fmt.Sprint(i)), false)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Publication order is deliberately the reverse of reservation order.
			for i := len(inputs) - 1; i >= 0; i-- {
				source.CommitSignal(queueSource(inputs[i]))
			}
			for i := range inputs {
				if progress, err := store.BindNextSignal(ctx, "flow", "id", 9, 32, source); err != nil || !progress {
					t.Fatal(i, progress, err)
				}
			}
			v, err := store.Open(ctx, "flow", "id", 9)
			if err != nil {
				t.Fatal(err)
			}
			if v.SignalQueueCount() != 32 || v.SignalSourceSequence() != 32 {
				t.Fatal(v.SignalQueueCount(), v.SignalSourceSequence())
			}
			for i := range inputs {
				b, data, e := v.SignalAt(ctx, uint64(i))
				want := inputs[len(inputs)-1-i]
				descriptor, descriptorErr := v.SignalBindingAt(ctx, uint64(i))
				if descriptorErr != nil || descriptor != b {
					t.Fatal("metadata differs from owned queue binding", i, descriptor, descriptorErr)
				}
				if e != nil || b.Input != want || b.Sequence != uint64(i+1) || string(data) != fmt.Sprint(want.Index) {
					t.Fatal(i, b, e)
				}
			}
			if _, err = v.SignalBindingAt(ctx, 32); !errors.Is(err, journal.ErrGap) {
				t.Fatal("out-of-range queue metadata accepted", err)
			}
			if err = v.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = v.SignalBindingAt(ctx, 0); err == nil {
				t.Fatal("closed queue reader admitted metadata")
			}
			// New source sequence after native dedup expiry does not add another queue
			// operation. Purging all sources does not erase canonical identity/bodies.
			source.CommitSignal(queueSource(inputs[0]))
			if progress, e := store.BindNextSignal(ctx, "flow", "id", 9, 33, source); e != nil || !progress {
				t.Fatal(progress, e)
			}
			for seq := uint64(1); seq <= 33; seq++ {
				source.PurgeSignal(seq)
			}
			reopened, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
			if err != nil {
				t.Fatal(err)
			}
			for i, input := range inputs {
				b, data, found, e := reopened.ReadSignalBinding(ctx, input.Request)
				if e != nil || !found || b.Input != input || b.Sequence != uint64(32-i) || !bytes.Equal(data, []byte(fmt.Sprint(i))) {
					t.Fatal(i, b, found, e)
				}
			}
			v, err = reopened.Open(ctx, "flow", "id", 9)
			if err != nil {
				t.Fatal(err)
			}
			if v.SignalQueueCount() != 32 || v.SignalSourceSequence() != 33 {
				t.Fatal(v.SignalQueueCount(), v.SignalSourceSequence())
			}
			if err = v.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err = m.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type queueReadFault struct {
	journal.GraphSignalSource
	calls int
}

func (p *queueReadFault) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	p.calls++
	return nil, errors.New("uncertain source read")
}
func TestGraphCanonicalSignalQueueNeverSkipsUnknownOrForgedSource(t *testing.T) {
	ctx := context.Background()
	s, m, _ := signalInputFixture(t, 99)
	r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "key"}
	input, err := s.ReserveSignal(ctx, r, []byte("input"), false)
	if err != nil {
		t.Fatal(err)
	}
	source := sim.NewSignalTransport(sim.NewScheduler(99))
	forged := queueSource(input)
	forged.Header.Set(journal.GraphSignalTokenHeader, "wrong")
	source.CommitSignal(forged)
	source.CommitSignal(queueSource(input))
	unknown := &queueReadFault{GraphSignalSource: source}
	if progress, e := s.BindNextSignal(ctx, "flow", "id", 9, 2, unknown); e == nil || progress {
		t.Fatal(progress, e)
	}
	if progress, e := s.BindNextSignal(ctx, "flow", "id", 9, 2, source); !errors.Is(e, journal.ErrGap) || progress {
		t.Fatal(progress, e)
	}
	v, err := s.Open(ctx, "flow", "id", 9)
	if err != nil {
		t.Fatal(err)
	}
	if v.SignalSourceSequence() != 0 || v.SignalQueueCount() != 0 {
		t.Fatal("skipped unread/forged first source")
	}
	if err = v.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Removing the bad source changes the witnessed source history explicitly.
	source.PurgeSignal(1)
	if progress, e := s.BindNextSignal(ctx, "flow", "id", 9, 2, source); e != nil || !progress {
		t.Fatal(progress, e)
	}
	b, _, found, err := s.ReadSignalBinding(ctx, r)
	if err != nil || !found || b.Sequence != 2 {
		t.Fatal(b, found, err)
	}
	if err = m.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

type queueSourceHook struct {
	journal.GraphSignalSource
	hook func() error
}

func (p *queueSourceHook) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	msg, err := p.GraphSignalSource.NextSignal(ctx, from, subject)
	if err == nil && p.hook != nil {
		hook := p.hook
		p.hook = nil
		if e := hook(); e != nil {
			return nil, e
		}
	}
	return msg, err
}
func TestGraphCanonicalSignalQueueConcurrentBindingAndRetirement(t *testing.T) {
	for _, mode := range []string{"concurrent", "retire"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, m, _ := signalInputFixture(t, 111)
			source := sim.NewSignalTransport(sim.NewScheduler(111))
			r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "first"}
			first, err := s.ReserveSignal(ctx, r, []byte("first"), false)
			if err != nil {
				t.Fatal(err)
			}
			source.CommitSignal(queueSource(first))
			r.Key = "second"
			second, err := s.ReserveSignal(ctx, r, []byte("second"), false)
			if err != nil {
				t.Fatal(err)
			}
			source.CommitSignal(queueSource(second))
			tail := uint64(0)
			if mode == "retire" {
				tail, err = s.Begin(ctx, "flow", "id", 9)
				if err != nil {
					t.Fatal(err)
				}
				tail, err = s.Append(ctx, "flow", "id", 9, journal.Entry{Kind: journal.Started}, tail, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				tail, err = s.Append(ctx, "flow", "id", 9, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			hooked := &queueSourceHook{GraphSignalSource: source}
			hooked.hook = func() error {
				if mode == "retire" {
					return s.Retire(ctx, "flow", "id", 9, tail)
				}
				progress, e := s.BindNextSignal(ctx, "flow", "id", 9, 2, source)
				if e == nil && !progress {
					return errors.New("inner binder did not advance")
				}
				return e
			}
			if progress, e := s.BindNextSignal(ctx, "flow", "id", 9, 2, hooked); !errors.Is(e, journal.ErrStale) || progress {
				t.Fatal("captured frontier changed but stale binder committed", progress, e)
			}
			if hooked.hook != nil {
				t.Fatal("cut not reached")
			}
			if mode == "concurrent" {
				if progress, e := s.BindNextSignal(ctx, "flow", "id", 9, 2, source); e != nil || !progress {
					t.Fatal(progress, e)
				}
				v, e := s.Open(ctx, "flow", "id", 9)
				if e != nil {
					t.Fatal(e)
				}
				if v.SignalQueueCount() != 2 {
					t.Fatal(v.SignalQueueCount())
				}
				for i, want := range []journal.GraphSignalInput{first, second} {
					b, _, e := v.SignalAt(ctx, uint64(i))
					if e != nil || b.Input != want || b.Sequence != uint64(i+1) {
						t.Fatal(i, b, e)
					}
				}
				if e = v.Close(ctx); e != nil {
					t.Fatal(e)
				}
			} else if _, _, _, e := s.ReadSignalBinding(ctx, first.Request); !errors.Is(e, journal.ErrStale) {
				t.Fatal(e)
			}
			if err = m.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGraphCanonicalSignalQueueReplacementAndFutureFence(t *testing.T) {
	ctx := context.Background()
	s, m, now := signalInputFixture(t, 128)
	source := sim.NewSignalTransport(sim.NewScheduler(128))
	r := journal.GraphSignalRequest{Type: "flow", ID: "id", Invocation: 9, Name: "signal", Key: "key"}
	old, err := s.ReserveSignal(ctx, r, []byte("old"), false)
	if err != nil {
		t.Fatal(err)
	}
	source.CommitSignal(queueSource(old))
	if progress, e := s.BindNextSignal(ctx, r.Type, r.ID, 9, 1, source); e != nil || !progress {
		t.Fatal(progress, e)
	}
	retained, err := s.Open(ctx, r.Type, r.ID, 9)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := s.Begin(ctx, r.Type, r.ID, 9)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, r.Type, r.ID, 9, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, r.Type, r.ID, 9, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Retire(ctx, r.Type, r.ID, 9, tail); err != nil {
		t.Fatal(err)
	}
	start, err := s.ReserveStart(ctx, journal.GraphStartRequest{Type: r.Type, ID: r.ID}, []byte(`8`))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, start.Start.Token, 10); err != nil {
		t.Fatal(err)
	}
	r.Invocation = 10
	fresh, err := s.ReserveSignal(ctx, r, []byte("new"), false)
	if err != nil {
		t.Fatal(err)
	}
	source.CommitSignal(queueSource(fresh))
	future := queueSource(fresh)
	future.Header.Set("Wf-Inv-Seq", "11")
	source.CommitSignal(future)
	source.CommitSignal(queueSource(fresh))
	for i := 0; i < 2; i++ {
		if progress, e := s.BindNextSignal(ctx, r.Type, r.ID, 10, 4, source); e != nil || !progress {
			t.Fatal(i, progress, e)
		}
	}
	if progress, e := s.BindNextSignal(ctx, r.Type, r.ID, 10, 4, source); !errors.Is(e, journal.ErrStale) || progress {
		t.Fatal("future generation source skipped", progress, e)
	}
	v, err := s.Open(ctx, r.Type, r.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if v.SignalQueueCount() != 1 || v.SignalSourceSequence() != 2 {
		t.Fatal("replacement inherited or skipped operations", v.SignalQueueCount(), v.SignalSourceSequence())
	}
	b, data, err := v.SignalAt(ctx, 0)
	if err != nil || b.Input != fresh || string(data) != "new" {
		t.Fatal(b, err)
	}
	if err = v.Close(ctx); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Second)
	if _, err = m.Protocol().SweepWithReaders(ctx, *now); err != nil {
		t.Fatal(err)
	}
	b, data, err = retained.SignalAt(ctx, 0)
	if err != nil || b.Input != old || string(data) != "old" {
		t.Fatal("retained old queue lost", b, err)
	}
	if err = retained.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = m.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}
