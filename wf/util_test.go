package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRunOnceRetryKeepsDeduplicationKey(t *testing.T) {
	var entries []Entry
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	c := NewContext(context.Background(), nil, appendFn)
	if _, err := RunOnce(c, "charge", 10, func(context.Context, string) (int, error) { return 0, nil }); !errors.Is(err, ErrInvocationIdentity) {
		t.Fatalf("identity: %v", err)
	}
	c.SetChildSupport("orders", "42", 17, nil)
	var first string
	if _, err := RunOnce(c, "charge", 10, func(_ context.Context, key string) (int, error) {
		first = key
		return 10, nil
	}); err != nil {
		t.Fatal(err)
	}
	if first == "" || len(entries) != 2 {
		t.Fatalf("key=%q entries=%d", first, len(entries))
	}
	otherGeneration := NewContext(context.Background(), nil, appendFn)
	otherGeneration.SetChildSupport("orders", "42", 18, nil)
	var otherKey string
	if _, err := RunOnce(otherGeneration, "charge", 10, func(_ context.Context, key string) (int, error) {
		otherKey = key
		return 10, nil
	}); err != nil || otherKey == first {
		t.Fatalf("generation key reused: first=%q second=%q err=%v", first, otherKey, err)
	}
	retry := NewContext(context.Background(), entries[:1], appendFn)
	retry.SetChildSupport("orders", "42", 17, nil)
	var second string
	if _, err := RunOnce(retry, "charge", 10, func(_ context.Context, key string) (int, error) {
		second = key
		return 10, nil
	}); err != nil || second != first {
		t.Fatalf("retry: key=%q want=%q err=%v", second, first, err)
	}
	replay := NewContext(context.Background(), entries[:2], nil)
	replay.SetChildSupport("orders", "42", 17, nil)
	if _, err := RunOnce(replay, "charge", 10, func(context.Context, string) (int, error) {
		t.Fatal("effect executed on replay")
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	wrong := NewContext(context.Background(), entries[:2], nil)
	if _, err := Run(wrong, "charge", 10, func(context.Context) (int, error) { return 0, nil }); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("Run replaced RunOnce: %v", err)
	}
}

func TestJournaledNowAndRandomReplay(t *testing.T) {
	var entries []Entry
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	clockCalls := 0
	c := NewContext(context.Background(), nil, appendFn)
	c.SetTimerSupport(time.Time{}, func(context.Context) (time.Time, error) { clockCalls++; return now, nil }, nil)
	firstTime, err := Now(c)
	if err != nil {
		t.Fatal(err)
	}
	firstRandom, err := Random(c)
	if err != nil {
		t.Fatal(err)
	}
	replay := NewContext(context.Background(), entries, nil)
	replay.SetTimerSupport(time.Time{}, func(context.Context) (time.Time, error) {
		t.Fatal("server clock called during replay")
		return time.Time{}, nil
	}, nil)
	replayedTime, err := Now(replay)
	if err != nil || !replayedTime.Equal(firstTime) || clockCalls != 1 {
		t.Fatalf("time: %v vs %v calls=%d err=%v", replayedTime, firstTime, clockCalls, err)
	}
	replayedRandom, err := Random(replay)
	if err != nil || replayedRandom != firstRandom {
		t.Fatalf("random: %d vs %d err=%v", replayedRandom, firstRandom, err)
	}
	if err := replay.CheckComplete(); err != nil {
		t.Fatal(err)
	}
}
