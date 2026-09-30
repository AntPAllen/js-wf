package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSelectManySuspendsReplaysChoiceAndRetainsLosers(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var entries []Entry
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	outcome, _ := json.Marshal(Outcome{Result: []byte(`42`)})
	signals := []Signal{{Sequence: 3, Name: "ready", Payload: []byte(`true`)}, {Sequence: 7, Name: "child_0", Payload: outcome}}
	build := func(wakeup time.Time, signals ...Signal) (*Context, *TimerHandle, *TimerHandle) {
		c := NewContext(context.Background(), entries, appendEntry, signals...)
		c.SetTimerSupport(wakeup, func(context.Context) (time.Time, error) { return base, nil }, func(context.Context, uint64, time.Time) error { return nil })
		one, err := c.Timer("one", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		two, err := c.Timer("two", 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return c, one, two
	}
	c, one, two := build(base)
	p := Promise{SignalName: "child_0"}
	if i, _, err := Select(c, SignalAwaitable("ready"), one, p, two); i != -1 || !errors.Is(err, ErrSuspended) || c.WaitingOn() != "select_many" {
		t.Fatalf("suspend index=%d wait=%s err=%v", i, c.WaitingOn(), err)
	}
	c, one, two = build(base.Add(3*time.Second), signals...)
	i, data, err := Select(c, SignalAwaitable("ready"), one, p, two)
	if i != 0 || string(data) != "true" || err != nil {
		t.Fatalf("priority index=%d data=%s err=%v", i, data, err)
	}
	if i, data, err := Select(c, p, two); i != 0 || string(data) != "42" || err != nil {
		t.Fatalf("promise index=%d data=%s err=%v", i, data, err)
	}
	if data, err := AwaitPromise(c, p); err != nil || string(data) != "42" {
		t.Fatalf("selected promise reuse=%s err=%v", data, err)
	}
	if i, _, err := Select(c, p, one); i != 0 || err != nil {
		t.Fatalf("resolved promise select=%d %v", i, err)
	}
	if i, _, err := Select(c, one, two); i != 0 || err != nil {
		t.Fatalf("first timer=%d %v", i, err)
	}
	if err := two.Await(); err != nil {
		t.Fatalf("losing timer unavailable: %v", err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	// Replay has a different clock and no available fresh event for the timers.
	c, one, two = build(base, signals...)
	if i, _, err := Select(c, SignalAwaitable("ready"), one, p, two); i != 0 || err != nil {
		t.Fatal(i, err)
	}
	if _, _, err := Select(c, p, two); err != nil {
		t.Fatal(err)
	}
	if _, err := AwaitPromise(c, p); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Select(c, p, one); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Select(c, one, two); err != nil {
		t.Fatal(err)
	}
	if err := two.Await(); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	c, one, two = build(base, signals...)
	if _, _, err := Select(c, one, SignalAwaitable("ready"), p, two); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("case reorder accepted: %v", err)
	}
}

func TestSelectManyRejectsInvalidCasesAndCorruptCompletion(t *testing.T) {
	c := NewContext(context.Background(), nil, func(context.Context, Kind, json.RawMessage) error { return nil })
	for _, cases := range [][]Awaitable{nil, {nil}, {SignalAwaitable("bad.name")}, {(*TimerHandle)(nil)}, {(*Promise)(nil)}} {
		if _, _, err := Select(c, cases...); err == nil {
			t.Fatal("invalid select accepted")
		}
	}
	request, _ := json.Marshal(selectRequest{Kind: "select_many", Cases: []selectCase{{Kind: "signal", Name: "ready"}}})
	for _, done := range []string{`{}`, `{"case_index":-1}`, `{"case_index":1}`, `{"case_index":0}`, `{"case_index":0,"signal_seq":99}`} {
		c := NewContext(context.Background(), []Entry{{Kind: StepRequested, Payload: request}, {Kind: StepCompleted, Payload: json.RawMessage(done)}}, nil)
		if _, _, err := Select(c, SignalAwaitable("ready")); !errors.Is(err, ErrCorruptJournal) {
			t.Fatalf("corrupt completion accepted %s: %v", done, err)
		}
	}
}

func TestSelectManyUnknownCompletionReplaysRecordedWinner(t *testing.T) {
	var entries []Entry
	lost := errors.New("completion acknowledgement lost")
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Kind: kind, Payload: payload})
		if kind == StepCompleted {
			return lost
		}
		return nil
	}
	c := NewContext(context.Background(), nil, appendEntry, Signal{Sequence: 7, Name: "second", Payload: []byte(`42`)})
	if _, _, err := Select(c, SignalAwaitable("first"), SignalAwaitable("second")); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	// A newly buffered higher-priority signal cannot change a committed choice.
	c = NewContext(context.Background(), entries, nil, Signal{Sequence: 3, Name: "first", Payload: []byte(`1`)}, Signal{Sequence: 7, Name: "second", Payload: []byte(`42`)})
	if i, data, err := Select(c, SignalAwaitable("first"), SignalAwaitable("second")); i != 1 || string(data) != "42" || err != nil {
		t.Fatalf("replay winner=%d data=%s err=%v", i, data, err)
	}
	if c.usedSignals[3] || !c.usedSignals[7] {
		t.Fatal("replay consumed losing signal")
	}
}
