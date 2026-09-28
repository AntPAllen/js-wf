package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
)

func TestPromiseResolvesLargeChildResult(t *testing.T) {
	data := []byte(`{"child":42}`)
	digest := sha256.Sum256(data)
	out := Outcome{ResultRef: "child-result", ResultHash: hex.EncodeToString(digest[:])}
	payload, _ := json.Marshal(out)
	var entries []Entry
	c := NewContext(context.Background(), nil, func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}, Signal{Sequence: 7, Name: "child_0", Payload: payload})
	c.SetResultStore(nil, func(_ context.Context, name string) ([]byte, error) {
		if name != out.ResultRef {
			t.Fatalf("object name=%q", name)
		}
		return data, nil
	})
	promise := Promise{SignalName: "child_0"}
	got, err := AwaitPromise(c, promise)
	if err != nil || string(got) != string(data) {
		t.Fatalf("child result=%s err=%v", got, err)
	}
	replay := NewContext(context.Background(), entries, nil, Signal{Sequence: 7, Name: "child_0", Payload: payload})
	replay.SetResultStore(nil, func(context.Context, string) ([]byte, error) { return data, nil })
	got, err = AwaitPromise(replay, promise)
	if err != nil || string(got) != string(data) {
		t.Fatalf("replay child result=%s err=%v", got, err)
	}
	corrupt := NewContext(context.Background(), entries, nil, Signal{Sequence: 7, Name: "child_0", Payload: payload})
	corrupt.SetResultStore(nil, func(context.Context, string) ([]byte, error) { return []byte("bad"), nil })
	if _, err := AwaitPromise(corrupt, promise); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("corrupt child result: %v", err)
	}
}
