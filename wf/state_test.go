package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestJournaledStateReplayAndDivergence(t *testing.T) {
	var entries []Entry
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	workflow := func(c *Context, first int) (int, error) {
		var missing int
		found, err := c.GetState("count", &missing)
		if err != nil {
			return 0, err
		}
		if found {
			return 0, errors.New("initial state was not empty")
		}
		if err := c.SetState("count", first); err != nil {
			return 0, err
		}
		var got int
		found, err = c.GetState("count", &got)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, errors.New("state missing after write")
		}
		return got, nil
	}
	c := NewContext(context.Background(), nil, appendFn)
	value, err := workflow(c, 42)
	if err != nil || value != 42 || len(entries) != 6 {
		t.Fatalf("first run: value=%d entries=%d err=%v", value, len(entries), err)
	}
	replay := NewContext(context.Background(), entries, nil)
	value, err = workflow(replay, 42)
	if err != nil || value != 42 || replay.CheckComplete() != nil {
		t.Fatalf("replay: value=%d err=%v", value, err)
	}
	changed := NewContext(context.Background(), entries, nil)
	if _, err := workflow(changed, 43); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("changed state write: %v", err)
	}
	// A recorded state read cannot silently return a different value.
	var corrupted []Entry
	for _, e := range entries {
		corrupted = append(corrupted, Entry{Index: e.Index, Kind: e.Kind, Payload: append(json.RawMessage(nil), e.Payload...)})
	}
	corrupted[5].Payload = json.RawMessage(`{"result":{"found":true,"value":99}}`)
	if _, err := workflow(NewContext(context.Background(), corrupted, nil), 42); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("corrupt state read: %v", err)
	}
}
