package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTimerHandlesCancelAndCoalesceOnReplay(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var entries []Entry
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	scheduled := 0
	newContext := func(at time.Time) *Context {
		c := NewContext(context.Background(), entries, appendEntry)
		c.SetTimerSupport(at, func(context.Context) (time.Time, error) { return base, nil }, func(context.Context, uint64, time.Time) error {
			scheduled++
			return nil
		})
		return c
	}
	c := newContext(base)
	first, err := c.Timer("first", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Timer("second", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := first.Await(); !errors.Is(err, ErrTimerCancelled) {
		t.Fatalf("cancelled await: %v", err)
	}
	if err := second.Await(); !errors.Is(err, ErrSuspended) {
		t.Fatalf("early await: %v", err)
	}
	if scheduled != 2 {
		t.Fatalf("schedules=%d", scheduled)
	}
	c = newContext(base.Add(6 * time.Second))
	first, err = c.Timer("first", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err = c.Timer("second", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := second.Await(); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	if scheduled != 2 {
		t.Fatalf("replay rescheduled timers: %d", scheduled)
	}
	c = NewContext(context.Background(), entries, nil)
	first, err = c.Timer("first", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err = c.Timer("second", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := second.Await(); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	c = NewContext(context.Background(), entries, nil)
	if _, err := c.Timer("first", 4*time.Second); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("changed timer duration: %v", err)
	}
}

func TestTimerHandleRetriesScheduleAfterRequest(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var entries []Entry
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	c := NewContext(context.Background(), entries, appendEntry)
	c.SetTimerSupport(base, func(context.Context) (time.Time, error) { return base, nil }, func(context.Context, uint64, time.Time) error {
		return errors.New("publisher unavailable")
	})
	if _, err := c.Timer("retry", time.Second); !errors.Is(err, ErrTimerSchedule) || len(entries) != 1 {
		t.Fatalf("first attempt: entries=%d err=%v", len(entries), err)
	}
	c = NewContext(context.Background(), entries, appendEntry)
	c.SetTimerSupport(base, nil, func(_ context.Context, step uint64, at time.Time) error {
		if step != 0 || !at.Equal(base.Add(time.Second)) {
			t.Fatalf("replayed schedule: step=%d at=%v", step, at)
		}
		return nil
	})
	if _, err := c.Timer("retry", time.Second); err != nil || len(entries) != 2 {
		t.Fatalf("retry: entries=%d err=%v", len(entries), err)
	}
}
