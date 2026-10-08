package journal_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/sim"
)

func TestGraphCanonicalStartPendingBindingAndRetainedInput(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	m := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
	s, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: func() time.Time { return now }, PinTTL: time.Minute, IntentTTL: time.Second, CanonicalStarts: true})
	if err != nil {
		t.Fatal(err)
	}
	r := journal.GraphStartRequest{Type: "flow", ID: "id", ParentType: "parent", ParentID: "parent", ParentInvocation: 8, SignalName: "result"}
	input := bytes.Repeat([]byte("owned input"), 100)
	first, err := s.ReserveStart(ctx, r, input)
	if err != nil || !first.Pending || first.Invocation != 0 {
		t.Fatal(first, err)
	}
	if _, err = s.Begin(ctx, r.Type, r.ID, 9); !errors.Is(err, journal.ErrStale) {
		t.Fatal("pending start executed", err)
	}
	view, err := s.OpenStart(ctx, r.Type, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := view.StartInput(ctx); err != nil || !bytes.Equal(got, input) {
		t.Fatal(err)
	}
	if duplicate, e := s.ReserveStart(ctx, r, input); e != nil || duplicate.Start != first.Start {
		t.Fatal(duplicate, e)
	}
	if _, err = s.ReserveStart(ctx, r, []byte("other")); !errors.Is(err, journal.ErrStartMismatch) {
		t.Fatal(err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, "wrong-token", 9); !errors.Is(err, journal.ErrStale) {
		t.Fatal(err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, first.Start.Token, 9); err != nil {
		t.Fatal(err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, first.Start.Token, 10); !errors.Is(err, journal.ErrStale) {
		t.Fatal(err)
	}
	tail, err := s.Begin(ctx, r.Type, r.ID, 9)
	if err != nil || tail != 0 {
		t.Fatal(tail, err)
	}
	tail, err = s.Append(ctx, r.Type, r.ID, 9, journal.Entry{Kind: journal.Started, Epoch: 1}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, r.Type, r.ID, 9, journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 1}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.FencePurge(ctx, r.Type, r.ID, 9, tail); err != nil {
		t.Fatal(err)
	}
	if _, err = s.OpenStart(ctx, r.Type, r.ID); !errors.Is(err, journal.ErrStale) {
		t.Fatal(err)
	}
	if err = s.Retire(ctx, r.Type, r.ID, 9, tail); err != nil {
		t.Fatal(err)
	}
	second, err := s.ReserveStart(ctx, r, []byte("replacement"))
	if err != nil || !second.Pending || second.Start.Token == first.Start.Token {
		t.Fatal(second, err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, second.Start.Token, 9); !errors.Is(err, journal.ErrStale) {
		t.Fatal("generation high-water reset", err)
	}
	if err = s.BindStart(ctx, r.Type, r.ID, second.Start.Token, 10); err != nil {
		t.Fatal(err)
	}
	if next, e := s.Begin(ctx, r.Type, r.ID, 10); e != nil || next != tail {
		t.Fatal("logical tail reset", next, e)
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got, e := view.StartInput(ctx); e != nil || !bytes.Equal(got, input) {
		t.Fatal("old input lost", e)
	}
	if err = view.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = m.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphCanonicalStartRejectsLegacyAndBounds(t *testing.T) {
	ctx := context.Background()
	m := sim.NewGraphPublicationTransport(sim.NewScheduler(2))
	s, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), CanonicalStarts: true, PayloadReadLimit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReserveStart(ctx, journal.GraphStartRequest{Type: "flow", ID: "id"}, make([]byte, 11)); !errors.Is(err, journal.ErrTooLong) {
		t.Fatal(err)
	}
	if _, err = s.ReserveStart(ctx, journal.GraphStartRequest{Type: "flow", ID: "id", ParentType: "parent"}, nil); err == nil {
		t.Fatal("partial parent accepted")
	}
	if _, err = s.ReserveStart(ctx, journal.GraphStartRequest{Type: "flow", ID: "id"}, nil); err != nil {
		t.Fatal(err)
	}
	legacy, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.InspectRetirement(ctx, "flow", "id"); !errors.Is(err, journal.ErrGap) {
		t.Fatal("legacy cursor admitted v4", err)
	}
}
