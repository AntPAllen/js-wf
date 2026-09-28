package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"js-wf/journal"
)

func TestOfflineReplayWithSignalAndObjectResult(t *testing.T) {
	var emitted []Entry
	objects := map[string][]byte{}
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		emitted = append(emitted, Entry{Kind: kind, Payload: payload})
		return nil
	}
	c := NewContext(context.Background(), nil, appendFn, Signal{Sequence: 7, Name: "go", Payload: []byte(`"yes"`)})
	c.SetResultStore(func(_ context.Context, data []byte) (string, error) {
		digest := sha256.Sum256(data)
		name := "large-" + hex.EncodeToString(digest[:])
		objects[name] = data
		return name, nil
	}, nil)
	if _, err := Run(c, "large", 1, func(context.Context) (string, error) { return strings.Repeat("x", MaxInlineResult), nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := AwaitSignal(c, "go"); err != nil {
		t.Fatal(err)
	}
	signalPayload, _ := json.Marshal(struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Payload  []byte `json:"payload"`
	}{7, "go", []byte(`"yes"`)})
	records := []journal.Record{
		{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1},
		{Entry: journal.Entry{Index: 1, Kind: journal.SignalConsumed, Payload: signalPayload}, Sequence: 2},
	}
	for _, e := range emitted {
		records = append(records, journal.Record{Entry: journal.Entry{Index: uint64(len(records)), Kind: journal.Kind(e.Kind), Payload: e.Payload}, Sequence: uint64(len(records) + 1)})
	}
	records = append(records, journal.Record{Entry: journal.Entry{Index: uint64(len(records)), Kind: journal.Completed}, Sequence: uint64(len(records) + 1)})
	journalBytes, _ := json.Marshal(records)
	workflow := func(c *Context) (int, error) {
		value, err := Run(c, "large", 1, func(context.Context) (string, error) {
			t.Fatal("effect ran during replay")
			return "", nil
		})
		if err != nil {
			return 0, err
		}
		signal, err := AwaitSignal(c, "go")
		if err != nil || string(signal) != `"yes"` {
			return 0, errors.New("signal changed during replay")
		}
		return len(value), nil
	}
	value, err := Replay(journalBytes, workflow, ReplayOptions{Objects: objects})
	if err != nil || value != MaxInlineResult {
		t.Fatalf("replay: value=%d err=%v", value, err)
	}
	if _, err := Replay(journalBytes, workflow); !errors.Is(err, ErrReplayObjectMissing) {
		t.Fatalf("missing object: %v", err)
	}
	for name := range objects {
		objects[name] = []byte(`"corrupt"`)
	}
	if _, err := Replay(journalBytes, workflow, ReplayOptions{Objects: objects}); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("corrupt object: %v", err)
	}
	if _, err := Replay(journalBytes, func(c *Context) (int, error) {
		_, err := Run(c, "renamed", 1, func(context.Context) (string, error) { return "", nil })
		return 0, err
	}, ReplayOptions{Objects: objects}); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("renamed step: %v", err)
	}
}
