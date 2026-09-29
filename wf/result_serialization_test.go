package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
)

func assertUnserializableStepResult[T any](t *testing.T, value T) {
	t.Helper()
	var entries []Entry
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: append(json.RawMessage(nil), payload...)})
		return nil
	}
	var effects int
	run := func(c *Context) error {
		_, err := Run(c, "unserializable", nil, func(context.Context) (T, error) {
			effects++
			return value, nil
		})
		return err
	}
	first := NewContext(context.Background(), nil, appendFn)
	firstErr := run(first)
	if !errors.Is(firstErr, ErrStepResultNotSerializable) || len(entries) != 2 || effects != 1 {
		t.Fatalf("first run: error=%v entries=%d effects=%d", firstErr, len(entries), effects)
	}
	var done completion
	if entries[0].Kind != StepRequested || entries[1].Kind != StepCompleted || json.Unmarshal(entries[1].Payload, &done) != nil || done.ErrorKind != "result_not_serializable" || done.Error != firstErr.Error() {
		t.Fatalf("retained completion: %+v entries=%+v", done, entries)
	}
	replay := NewContext(context.Background(), entries, nil)
	replayErr := run(replay)
	if !errors.Is(replayErr, ErrStepResultNotSerializable) || replayErr.Error() != firstErr.Error() || effects != 1 || replay.CheckComplete() != nil {
		t.Fatalf("replay: error=%v first=%v effects=%d cursor=%v", replayErr, firstErr, effects, replay.CheckComplete())
	}
}

func TestUnserializableStepResultsAreDurable(t *testing.T) {
	t.Run("channel", func(t *testing.T) { assertUnserializableStepResult(t, make(chan int)) })
	t.Run("function", func(t *testing.T) { assertUnserializableStepResult(t, func() {}) })
	t.Run("cyclic value", func(t *testing.T) {
		type node struct{ Next *node }
		value := &node{}
		value.Next = value
		assertUnserializableStepResult(t, value)
	})
}

func TestUnknownStepResultErrorKindFailsClosed(t *testing.T) {
	digest := sha256.Sum256([]byte("null"))
	requestBytes, err := json.Marshal(request{Kind: "run", Name: "step", InputHash: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{Index: 1, Kind: StepRequested, Payload: requestBytes},
		{Index: 2, Kind: StepCompleted, Payload: json.RawMessage(`{"error":"bad","error_kind":"unrecognized"}`)},
	}
	c := NewContext(context.Background(), entries, nil)
	_, err = Run(c, "step", nil, func(context.Context) (int, error) { t.Fatal("effect ran"); return 0, nil })
	if !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("unknown error kind: %v", err)
	}
}
