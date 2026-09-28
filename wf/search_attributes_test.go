package wf

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestSearchAttributesReplayAndValidation(t *testing.T) {
	var entries []Entry
	c := NewContext(context.Background(), nil, func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	})
	first := map[string]string{"team": "alpha", "priority": "high"}
	if err := c.SetSearchAttributes(first); err != nil {
		t.Fatal(err)
	}
	first["team"] = "changed after append"
	if err := c.SetSearchAttributes(map[string]string{"team": "beta"}); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("journal entries=%d", len(entries))
	}
	replay := NewContext(context.Background(), entries, nil)
	if err := replay.SetSearchAttributes(map[string]string{"priority": "high", "team": "alpha"}); err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if err := replay.SetSearchAttributes(map[string]string{"team": "beta"}); err != nil || replay.CheckComplete() != nil {
		t.Fatalf("second replay: %v", err)
	}
	if err := NewContext(context.Background(), entries, nil).SetSearchAttributes(map[string]string{"team": "gamma", "priority": "high"}); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("changed attributes: %v", err)
	}
	corrupt := append([]Entry(nil), entries...)
	corrupt[1].Payload = json.RawMessage(`{"result":{"team":"wrong","priority":"high"}}`)
	if err := NewContext(context.Background(), corrupt, nil).SetSearchAttributes(map[string]string{"team": "alpha", "priority": "high"}); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("corrupt completion: %v", err)
	}
	for _, attributes := range []map[string]string{
		{"bad.key": "value"},
		{"team": strings.Repeat("x", MaxSearchAttributeValue+1)},
		{"team": "\xff"},
	} {
		if err := c.SetSearchAttributes(attributes); err == nil {
			t.Fatalf("accepted invalid attributes %+v", attributes)
		}
	}
}
