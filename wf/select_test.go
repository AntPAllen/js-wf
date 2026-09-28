package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTimerSelectSignalSuspendsAndReplaysChoice(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var entries []Entry
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	newContext := func(at time.Time, signals ...Signal) *Context {
		c := NewContext(context.Background(), entries, appendEntry, signals...)
		c.SetTimerSupport(at, func(context.Context) (time.Time, error) { return base, nil }, func(context.Context, uint64, time.Time) error { return nil })
		return c
	}
	c := newContext(base)
	timer, err := c.Timer("timeout", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := timer.SelectSignal("ready"); !errors.Is(err, ErrSuspended) || c.WaitingOn() != "select:timeout:ready" {
		t.Fatalf("select suspension: waiting=%q err=%v", c.WaitingOn(), err)
	}
	signal := Signal{Sequence: 7, Name: "ready", Payload: []byte(`true`)}
	c = newContext(base.Add(2*time.Second), signal)
	timer, err = c.Timer("timeout", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	choice, data, err := timer.SelectSignal("ready")
	if err != nil || choice != SignalSelected || string(data) != "true" {
		t.Fatalf("buffered signal priority: choice=%q data=%s err=%v", choice, data, err)
	}
	if err := timer.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	c = NewContext(context.Background(), entries, nil, signal)
	timer, err = c.Timer("timeout", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	choice, data, err = timer.SelectSignal("ready")
	if err != nil || choice != SignalSelected || string(data) != "true" {
		t.Fatalf("replayed choice: choice=%q data=%s err=%v", choice, data, err)
	}
	if err := timer.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	c = NewContext(context.Background(), entries, nil, signal)
	timer, err = c.Timer("timeout", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := timer.SelectSignal("changed"); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("changed select signal: %v", err)
	}
}

func TestTimerSelectSignalTimerBranchReplaysWithoutFiringAgain(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var entries []Entry
	var fires int
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	c := NewContext(context.Background(), entries, appendEntry)
	c.SetTimerSupport(base.Add(2*time.Second), func(context.Context) (time.Time, error) { return base, nil }, func(context.Context, uint64, time.Time) error { return nil })
	c.SetTimerObserver(func(time.Time, time.Time) { fires++ })
	timer, err := c.Timer("timeout", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	choice, data, err := timer.SelectSignal("ready")
	if err != nil || choice != TimerSelected || data != nil || fires != 1 {
		t.Fatalf("timer branch: choice=%q data=%s fires=%d err=%v", choice, data, fires, err)
	}
	c = NewContext(context.Background(), entries, nil)
	timer, err = c.Timer("timeout", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	choice, data, err = timer.SelectSignal("ready")
	if err != nil || choice != TimerSelected || data != nil || fires != 1 {
		t.Fatalf("timer replay: choice=%q data=%s fires=%d err=%v", choice, data, fires, err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
}
