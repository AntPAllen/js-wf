package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestVersionReplaysDecision(t *testing.T) {
	var entries []Entry
	c := NewContext(context.Background(), nil, func(_ context.Context, k Kind, p json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
		return nil
	})
	v, err := Version(c, "migration", 0, 1)
	if err != nil || v != 1 {
		t.Fatalf("first version=%d err=%v", v, err)
	}
	c = NewContext(context.Background(), entries, nil)
	v, err = Version(c, "migration", 0, 2)
	if err != nil || v != 1 {
		t.Fatalf("replayed version=%d err=%v", v, err)
	}
	c = NewContext(context.Background(), entries, nil)
	if _, err := Version(c, "renamed", 0, 2); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("renamed hook: %v", err)
	}
	c = NewContext(context.Background(), entries, nil)
	if _, err := Version(c, "migration", 2, 3); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("removed old branch: %v", err)
	}
}
