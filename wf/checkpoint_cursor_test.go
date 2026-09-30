package wf

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

// A continuation must retain the same external identities after dropping its
// completed prefix. This tests the cursor foundation, not durable checkpoints.
func TestCheckpointCursorPreservesSDKIdentitiesAndReplay(t *testing.T) {
	var full []Entry
	appender := func(records *[]Entry, offset uint64) Appender {
		return func(_ context.Context, kind Kind, payload json.RawMessage) error {
			*records = append(*records, Entry{Index: offset + uint64(len(*records)) + 1, Kind: kind, Payload: append(json.RawMessage(nil), payload...)})
			return nil
		}
	}
	original := NewContext(context.Background(), nil, appender(&full, 0))
	if err := original.SetState("prefix", 42); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(original, "prefix-work", 1, func(context.Context) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	original.SetChildSupport("cursor", "parent", 17, func(context.Context, string, string, []byte, string) error { return nil })
	prefixChild, err := CallAsync(original, "child", []byte(`23`))
	if err != nil {
		t.Fatal(err)
	}
	cut := len(full)
	type observations struct {
		Key         string
		Child       Promise
		TimerStep   uint64
		SleepStep   uint64
		Effects     int
		ChildStarts int
	}
	continueWorkflow := func(c *Context) (observations, error) {
		var observed observations
		c.SetChildSupport("cursor", "parent", 17, func(context.Context, string, string, []byte, string) error { observed.ChildStarts++; return nil })
		c.SetTimerSupport(time.Time{}, func(context.Context) (time.Time, error) { return time.Unix(100, 0), nil }, func(_ context.Context, step uint64, _ time.Time) error {
			observed.SleepStep = step
			return nil
		})
		_, err := RunOnce(c, "external", 23, func(_ context.Context, key string) (int, error) {
			observed.Key = key
			observed.Effects++
			return 46, nil
		})
		if err != nil {
			return observed, err
		}
		observed.Child, err = CallAsync(c, "child", []byte(`23`))
		if err != nil {
			return observed, err
		}
		handle, err := c.Timer("handle", time.Second)
		if err != nil {
			return observed, err
		}
		observed.TimerStep = handle.step
		err = Sleep(c, "sleep", 2*time.Second)
		return observed, err
	}
	expected, err := continueWorkflow(original)
	if !errors.Is(err, ErrSuspended) {
		t.Fatal(err)
	}
	if expected.Child.ChildID == prefixChild.ChildID || expected.Child.SignalName == prefixChild.SignalName {
		t.Fatal("continuation child reused a prefix identity")
	}
	var suffix []Entry
	resumed := NewContext(context.Background(), nil, appender(&suffix, uint64(cut)))
	resumed.stepOffset = uint64(cut)
	actual, err := continueWorkflow(resumed)
	if !errors.Is(err, ErrSuspended) || actual != expected {
		t.Fatalf("resumed=%+v expected=%+v err=%v", actual, expected, err)
	}
	if !reflect.DeepEqual(full[cut:], suffix) {
		t.Fatalf("suffix records differ: full=%+v resumed=%+v", full[cut:], suffix)
	}
	replay := NewContext(context.Background(), suffix, nil)
	replay.stepOffset = uint64(cut)
	replayed, err := continueWorkflow(replay)
	if !errors.Is(err, ErrSuspended) || replayed.Effects != 0 || replayed.ChildStarts != 0 || replayed.Child != expected.Child || replayed.TimerStep != expected.TimerStep || replayed.SleepStep != expected.SleepStep {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	// Forgetting the absolute base changes RunOnce's declared key and must be
	// rejected before any effect or child starts.
	lostBase := NewContext(context.Background(), suffix, nil)
	if lost, err := continueWorkflow(lostBase); !errors.Is(err, ErrNonDeterministic) || lost.Effects != 0 || lost.ChildStarts != 0 {
		t.Fatalf("lost cursor accepted: %v", err)
	}
}
