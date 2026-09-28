package wf

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestStepRecovery(t *testing.T) {
	var recorded []Entry
	appendFn := func(_ context.Context, k Kind, p json.RawMessage) error {
		recorded = append(recorded, Entry{Index: uint64(len(recorded) + 1), Kind: k, Payload: p})
		return nil
	}
	var effects int
	effect := func(context.Context) (int, error) { effects++; return 42, nil }
	ctx := NewContext(context.Background(), nil, appendFn)
	v, err := Run(ctx, "calculate", map[string]int{"n": 1}, effect)
	if err != nil || v != 42 || effects != 1 || len(recorded) != 2 {
		t.Fatalf("first run: value=%d err=%v effects=%d entries=%d", v, err, effects, len(recorded))
	}
	ctx = NewContext(context.Background(), recorded, nil)
	v, err = Run(ctx, "calculate", map[string]int{"n": 1}, effect)
	if err != nil || v != 42 || effects != 1 {
		t.Fatalf("replay: value=%d err=%v effects=%d", v, err, effects)
	}
	if err := ctx.CheckComplete(); err != nil {
		t.Fatal(err)
	}
	ctx = NewContext(context.Background(), recorded[:1], appendFn)
	_, err = Run(ctx, "calculate", map[string]int{"n": 1}, effect)
	if err != nil || effects != 2 {
		t.Fatalf("requested only: err=%v effects=%d", err, effects)
	}
	ctx = NewContext(context.Background(), recorded[:2], nil)
	_, err = Run(ctx, "changed", map[string]int{"n": 1}, effect)
	if !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("name mismatch: %v", err)
	}
	ctx = NewContext(context.Background(), recorded[:2], nil)
	_, err = Run(ctx, "calculate", map[string]int{"n": 2}, effect)
	if !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("input mismatch: %v", err)
	}
}

func TestEffectPanicIsJournaled(t *testing.T) {
	var entries []Entry
	c := NewContext(context.Background(), nil, func(_ context.Context, k Kind, p json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
		return nil
	})
	_, err := Run(c, "panic", nil, func(context.Context) (string, error) { panic("boom") })
	if err == nil || len(entries) != 2 {
		t.Fatalf("panic: err=%v entries=%d", err, len(entries))
	}
	c = NewContext(context.Background(), entries, nil)
	_, replayed := Run(c, "panic", nil, func(context.Context) (string, error) { t.Fatal("effect ran on replay"); return "", nil })
	if replayed == nil || replayed.Error() != err.Error() {
		t.Fatalf("replay error: %v", replayed)
	}
}

func TestEffectGoexitIsJournaled(t *testing.T) {
	var entries []Entry
	appendFn := func(_ context.Context, k Kind, p json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
		return nil
	}
	c := NewContext(context.Background(), nil, appendFn)
	_, err := Run(c, "goexit", nil, func(context.Context) (int, error) {
		runtime.Goexit()
		return 0, nil
	})
	if err == nil || !strings.Contains(err.Error(), "runtime.Goexit") || len(entries) != 2 || entries[1].Kind != StepCompleted {
		t.Fatalf("Goexit result: err=%v entries=%+v", err, entries)
	}
	replayed := NewContext(context.Background(), entries, nil)
	_, replayErr := Run(replayed, "goexit", nil, func(context.Context) (int, error) {
		t.Fatal("Goexit effect reran on replay")
		return 0, nil
	})
	if replayErr == nil || replayErr.Error() != err.Error() {
		t.Fatalf("replayed Goexit result: %v", replayErr)
	}
}

func TestBlockedEffectReturnsOnCancellationWithoutCompletion(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	var entries []Entry
	appendFn := func(_ context.Context, k Kind, p json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
		return nil
	}
	c := NewContext(base, nil, appendFn)
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	result := make(chan error, 1)
	go func() {
		_, err := Run(c, "blocked", nil, func(context.Context) (int, error) {
			close(entered)
			<-release
			return 1, nil
		})
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("effect did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled effect: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked effect trapped the workflow")
	}
	if len(entries) != 1 || entries[0].Kind != StepRequested {
		t.Fatalf("cancelled effect journal: %+v", entries)
	}
	retry := NewContext(context.Background(), entries, appendFn)
	value, err := Run(retry, "blocked", nil, func(context.Context) (int, error) { return 2, nil })
	if err != nil || value != 2 || len(entries) != 2 || entries[1].Kind != StepCompleted {
		t.Fatalf("retry after cancellation: value=%d err=%v entries=%+v", value, err, entries)
	}
}
