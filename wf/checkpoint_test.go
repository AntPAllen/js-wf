package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func checkpointTestContext(records *[]Entry, signals ...Signal) *Context {
	c := NewContext(context.Background(), nil, func(_ context.Context, k Kind, p json.RawMessage) error {
		*records = append(*records, Entry{Index: uint64(len(*records) + 1), Kind: k, Payload: append(json.RawMessage(nil), p...)})
		return nil
	}, signals...)
	c.SetChildSupport("parent", "materialized", 17, func(context.Context, string, string, []byte, string) error { return nil })
	return c
}

func TestCheckpointStateMatchesUninterruptedContinuation(t *testing.T) {
	var records []Entry
	outcome, _ := json.Marshal(Outcome{InvSeq: 23, Result: []byte("child-result")})
	signals := []Signal{{Sequence: 3, Name: "child_4", Payload: outcome}, {Sequence: 7, Name: "input", Payload: []byte("first")}, {Sequence: 9, Name: "input", Payload: []byte("second")}}
	c := checkpointTestContext(&records, signals...)
	if err := c.SetState("balance", 23); err != nil {
		t.Fatal(err)
	}
	if _, err := AwaitSignal(c, "input"); err != nil {
		t.Fatal(err)
	}
	p, err := CallAsync(c, "child", []byte(`23`))
	if err != nil {
		t.Fatal(err)
	}
	if p.SignalName != "child_4" {
		t.Fatal(p)
	}
	if result, err := AwaitPromise(c, p); err != nil || string(result) != "child-result" {
		t.Fatalf("%q %v", result, err)
	}
	timer, err := c.Timer("canceled", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := timer.Cancel(); err != nil {
		t.Fatal(err)
	}
	cut := len(records)
	anchor := uint64(cut + 2)
	raw, hash, err := c.CaptureCheckpoint("after_child_v1", p, anchor, 51, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != cut || c.position != cut {
		t.Fatal("capture mutated journal or cursor")
	}
	location := CheckpointLocation{Type: "parent", ID: "materialized", InvSeq: 17, Index: anchor, Epoch: 51, Hash: hash}
	restored, info, err := NewCheckpointContext(context.Background(), nil, func(_ context.Context, k Kind, p json.RawMessage) error { return nil }, raw, location, signals...)
	if err != nil {
		t.Fatal(err)
	}
	var local Promise
	if json.Unmarshal(info.Data, &local) != nil || local != p || info.Stage != "after_child_v1" || info.StepPosition != anchor || info.PanicAttempts != 2 || !reflect.DeepEqual(info.CancelledTimers, []uint64{8}) {
		t.Fatalf("info=%+v", info)
	}
	if restored.promiseResultBytes != 0 || restored.promiseResults[p.SignalName].cached {
		t.Fatal("derived promise cache persisted")
	}
	// Simulate the committed checkpoint pair in the uninterrupted control. The
	// real worker publication/manifest path is outside this SDK contract test.
	c.entries = append(c.entries, Entry{Index: anchor - 1, Kind: StepRequested, Payload: json.RawMessage(`{}`)}, Entry{Index: anchor, Kind: StepCompleted, Payload: json.RawMessage(`{}`)})
	c.position += 2
	type observed struct {
		Balance int
		Signal  string
		Child   string
		Key     string
		Step    uint64
	}
	runSuffix := func(c *Context) (observed, error) {
		var got observed
		found, err := c.GetState("balance", &got.Balance)
		if err != nil || !found {
			return got, err
		}
		next, err := AwaitSignal(c, "input")
		if err != nil {
			return got, err
		}
		got.Signal = string(next)
		child, err := AwaitPromise(c, local)
		if err != nil {
			return got, err
		}
		got.Child = string(child)
		_, err = RunOnce(c, "external", 23, func(_ context.Context, key string) (int, error) { got.Key = key; return 46, nil })
		if err != nil {
			return got, err
		}
		got.Step = c.stepPosition()
		return got, nil
	}
	want, err := runSuffix(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := runSuffix(restored)
	if err != nil || got != want {
		t.Fatalf("restored=%+v want=%+v err=%v", got, want, err)
	}
	if got.Balance != 23 || got.Signal != "second" || got.Child != "child-result" || got.Key == "" {
		t.Fatal(got)
	}
	if !reflect.DeepEqual(c.entries[cut+2:], restored.entries) {
		t.Fatal("SDK suffix differs")
	}
	// A second capture preserves earlier cancellation and signal-consumption
	// facts even though their journal entries were absent from this segment.
	raw2, hash2, err := restored.CaptureCheckpoint("done_v1", nil, restored.stepPosition()+2, 52, info.PanicAttempts)
	if err != nil {
		t.Fatal(err)
	}
	loc2 := location
	loc2.Index = restored.stepPosition() + 2
	loc2.Epoch = 52
	loc2.Hash = hash2
	again, info2, err := NewCheckpointContext(context.Background(), nil, nil, raw2, loc2, signals...)
	if err != nil || !again.usedSignals[3] || !again.usedSignals[7] || !again.usedSignals[9] || !reflect.DeepEqual(info2.CancelledTimers, info.CancelledTimers) {
		t.Fatalf("second restore info=%+v err=%v", info2, err)
	}
}

func TestCheckpointRejectsUnfinishedOrLiveTimerBoundary(t *testing.T) {
	t.Run("pending-request", func(t *testing.T) {
		var entries []Entry
		c := checkpointTestContext(&entries)
		_, err := Run(c, "pending", nil, func(context.Context) (int, error) { return 0, context.Canceled })
		if err == nil {
			t.Fatal("expected effect error")
		}
		// A failed recorded effect has a complete pair and is a valid boundary.
		if _, _, err := c.CaptureCheckpoint("next_v1", nil, 4, 1, 0); err != nil {
			t.Fatal(err)
		}
		c.position--
		c.entries = c.entries[:len(c.entries)-1]
		if _, _, err := c.CaptureCheckpoint("next_v1", nil, 4, 1, 0); !errors.Is(err, ErrCheckpointBoundary) {
			t.Fatal(err)
		}
	})
	t.Run("suspended", func(t *testing.T) {
		var entries []Entry
		c := checkpointTestContext(&entries)
		if _, err := AwaitSignal(c, "absent"); !errors.Is(err, ErrSuspended) {
			t.Fatal(err)
		}
		if _, _, err := c.CaptureCheckpoint("next_v1", nil, 4, 1, 0); !errors.Is(err, ErrCheckpointBoundary) {
			t.Fatal(err)
		}
	})
	t.Run("timer", func(t *testing.T) {
		var entries []Entry
		c := checkpointTestContext(&entries)
		timer, err := c.Timer("ready", 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.CaptureCheckpoint("next_v1", nil, 4, 1, 0); !errors.Is(err, ErrCheckpointBoundary) {
			t.Fatal(err)
		}
		if err := timer.Await(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := c.CaptureCheckpoint("next_v1", nil, 6, 1, 0); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCheckpointRejectsRuntimeObjectsInLocalsBeforeWrites(t *testing.T) {
	var entries []Entry
	c := checkpointTestContext(&entries)
	handle := &TimerHandle{c: c}
	for _, locals := range []any{c, *c, context.Background(), handle, *handle, map[string]any{"nested": []any{handle}}, struct{ Handle *TimerHandle }{handle}} {
		if _, _, err := c.CaptureCheckpoint("next_v1", locals, 2, 1, 0); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("locals=%T err=%v", locals, err)
		}
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	if _, _, err := c.CaptureCheckpoint("next_v1", cyclic, 2, 1, 0); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	if len(entries) != 0 || c.position != 0 {
		t.Fatal("invalid locals produced entries")
	}
}

func TestCheckpointRestoreRejectsGenerationAndSuffixBeforeEffects(t *testing.T) {
	var entries []Entry
	c := checkpointTestContext(&entries)
	raw, hash, err := c.CaptureCheckpoint("next_v1", nil, 2, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	location := CheckpointLocation{Type: "parent", ID: "materialized", InvSeq: 17, Index: 2, Epoch: 1, Hash: hash}
	writes := 0
	appender := func(context.Context, Kind, json.RawMessage) error { writes++; return nil }
	stale := location
	stale.InvSeq++
	if got, _, err := NewCheckpointContext(context.Background(), nil, appender, raw, stale); !errors.Is(err, ErrInvalidCheckpoint) || got != nil {
		t.Fatalf("%v %v", got, err)
	}
	for _, suffix := range [][]Entry{
		{{Index: 2, Kind: StepRequested, Payload: json.RawMessage(`{}`)}},
		{{Index: 3, Kind: StepCompleted, Payload: json.RawMessage(`{}`)}},
		{{Index: 3, Kind: StepRequested, Payload: json.RawMessage(`{}`)}, {Index: 3, Kind: StepCompleted, Payload: json.RawMessage(`{}`)}},
		{{Index: 3, Kind: StepRequested, Payload: json.RawMessage(`{`)}},
	} {
		if got, _, err := NewCheckpointContext(context.Background(), suffix, appender, raw, location); !errors.Is(err, ErrInvalidCheckpoint) || got != nil {
			t.Fatalf("%v %v", got, err)
		}
	}
	// Correctly bound partial requests may resume; payload buffers are detached.
	suffix := []Entry{{Index: 3, Kind: StepRequested, Payload: json.RawMessage(`{}`)}}
	got, _, err := NewCheckpointContext(context.Background(), suffix, appender, raw, location)
	if err != nil {
		t.Fatal(err)
	}
	suffix[0].Payload[0] = '['
	if string(got.entries[0].Payload) != "{}" || writes != 0 {
		t.Fatal("restore mutated data or wrote")
	}
	// A matching hash cannot turn malformed state into a partially restored SDK.
	bad := []byte(`{"version":1}`)
	sum := sha256.Sum256(bad)
	location.Hash = hex.EncodeToString(sum[:])
	if got, _, err := NewCheckpointContext(context.Background(), nil, appender, bad, location); !errors.Is(err, ErrInvalidCheckpoint) || got != nil {
		t.Fatalf("%v %v", got, err)
	}
}

func TestCheckpointPositiveTimerMustBeCanceledEvenAfterSignalSelection(t *testing.T) {
	var entries []Entry
	c := checkpointTestContext(&entries, Signal{Sequence: 1, Name: "ready", Payload: []byte("yes")})
	c.SetTimerSupport(time.Time{}, func(context.Context) (time.Time, error) { return time.Unix(100, 0), nil }, func(context.Context, uint64, time.Time) error { return nil })
	handle, err := c.Timer("later", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if selected, _, err := handle.SelectSignal("ready"); err != nil || selected != SignalSelected {
		t.Fatal(selected, err)
	}
	if _, _, err := c.CaptureCheckpoint("next_v1", nil, 6, 1, 0); !errors.Is(err, ErrCheckpointBoundary) {
		t.Fatal(err)
	}
	if err := handle.Cancel(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.CaptureCheckpoint("next_v1", nil, 8, 1, 0); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointRestoredPromiseVerifiesReferencedOutcome(t *testing.T) {
	data := []byte("verified-child-result")
	sum := sha256.Sum256(data)
	payload, _ := json.Marshal(Outcome{InvSeq: 23, ResultRef: "child-result", ResultHash: hex.EncodeToString(sum[:])})
	var entries []Entry
	c := checkpointTestContext(&entries, Signal{Sequence: 3, Name: "child_0", Payload: payload})
	c.SetResultStore(nil, func(context.Context, string) ([]byte, error) { return append([]byte(nil), data...), nil })
	promise := Promise{SignalName: "child_0"}
	if _, err := AwaitPromise(c, promise); err != nil {
		t.Fatal(err)
	}
	raw, hash, err := c.CaptureCheckpoint("next_v1", promise, 4, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	location := CheckpointLocation{Type: "parent", ID: "materialized", InvSeq: 17, Index: 4, Epoch: 1, Hash: hash}
	for _, bad := range []bool{false, true} {
		restored, _, err := NewCheckpointContext(context.Background(), nil, nil, raw, location)
		if err != nil {
			t.Fatal(err)
		}
		loads := 0
		restored.SetResultStore(nil, func(_ context.Context, key string) ([]byte, error) {
			loads++
			if key != "child-result" {
				t.Fatal(key)
			}
			if bad {
				return []byte("corrupt"), nil
			}
			return data, nil
		})
		result, err := AwaitPromise(restored, promise)
		if loads != 1 || restored.position != 0 || !restored.usedSignals[3] {
			t.Fatal("promise was consumed again or cache persisted")
		}
		if bad {
			if !errors.Is(err, ErrCorruptJournal) || result != nil {
				t.Fatalf("%q %v", result, err)
			}
		} else if err != nil || string(result) != string(data) {
			t.Fatalf("%q %v", result, err)
		}
	}
}

func TestCheckpointLocalValidationVisitsOverlappingSlices(t *testing.T) {
	var entries []Entry
	c := checkpointTestContext(&entries)
	values := []any{nil, &TimerHandle{c: c}}
	// Same backing pointer, different lengths: visiting the short slice must
	// not hide the runtime handle in the longer one.
	locals := struct {
		Short []any
		Long  []any
	}{values[:1], values}
	if _, _, err := c.CaptureCheckpoint("next_v1", locals, 2, 1, 0); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
}

func TestCheckpointRestoresNullableEmptyStateAsWritableMap(t *testing.T) {
	var entries []Entry
	c := checkpointTestContext(&entries)
	raw, _, err := c.CaptureCheckpoint("next_v1", nil, 2, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	frame["state"] = json.RawMessage(`null`)
	raw, err = json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	location := CheckpointLocation{Type: "parent", ID: "materialized", InvSeq: 17, Index: 2, Epoch: 1, Hash: hex.EncodeToString(sum[:])}
	restored, _, err := NewCheckpointContext(context.Background(), nil, func(context.Context, Kind, json.RawMessage) error { return nil }, raw, location)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.SetState("created", 23); err != nil {
		t.Fatal(err)
	}
	var result int
	if found, err := restored.GetState("created", &result); err != nil || !found || result != 23 {
		t.Fatalf("%v %d %v", found, result, err)
	}
}
