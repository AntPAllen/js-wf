package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSleepUsesServerClockAndReplays(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	var entries []Entry
	appendFn := func(_ context.Context, k Kind, p json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
		return nil
	}
	c := NewContext(context.Background(), nil, appendFn)
	var scheduled time.Time
	c.SetTimerSupport(base, func(context.Context) (time.Time, error) { return base, nil }, func(_ context.Context, _ uint64, at time.Time) error { scheduled = at; return nil })
	if err := Sleep(c, "wait", 5*time.Second); !errors.Is(err, ErrSuspended) {
		t.Fatalf("sleep: %v", err)
	}
	if !scheduled.Equal(base.Add(5 * time.Second)) {
		t.Fatalf("scheduled for %v", scheduled)
	}
	c = NewContext(context.Background(), entries, appendFn)
	c.SetTimerSupport(base.Add(6*time.Second), nil, nil)
	if err := Sleep(c, "wait", 5*time.Second); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(entries) != 2 || entries[1].Kind != StepCompleted {
		t.Fatalf("entries: %+v", entries)
	}
	c = NewContext(context.Background(), entries, nil)
	if err := Sleep(c, "wait", 6*time.Second); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("changed duration: %v", err)
	}
}

func TestNonpositiveSleepCompletesImmediately(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		var entries []Entry
		c := NewContext(context.Background(), nil, func(_ context.Context, k Kind, p json.RawMessage) error {
			entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
			return nil
		})
		if err := Sleep(c, "immediate", d); err != nil {
			t.Fatalf("duration %v: %v", d, err)
		}
		if len(entries) != 2 {
			t.Fatalf("duration %v: entries=%d", d, len(entries))
		}
	}
}
