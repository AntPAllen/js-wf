package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
)

func TestPromiseRepeatedAwaitReplaysOneConsumptionAndCopiesResult(t *testing.T) {
	out, _ := json.Marshal(Outcome{Result: []byte(`42`)})
	signal := Signal{Sequence: 7, Name: "child_0", Payload: out}
	promise := Promise{SignalName: signal.Name}
	var entries []Entry
	appendEntry := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}
	first := NewContext(context.Background(), nil, appendEntry, signal)
	for i := 0; i < 2; i++ {
		c := first
		if i != 0 {
			c = NewContext(context.Background(), entries, nil, signal)
		}
		one, err := AwaitPromise(c, promise)
		if err != nil || string(one) != "42" {
			t.Fatalf("first await=%s %v", one, err)
		}
		one[0] = '9'
		two, err := AwaitPromise(c, promise)
		if err != nil || string(two) != "42" {
			t.Fatalf("repeat await=%s %v", two, err)
		}
		if len(entries) != 2 {
			t.Fatalf("promise consumed more than one step: %d", len(entries))
		}
		if err := c.CheckComplete(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPromiseObjectReadRetryDoesNotConsumeAnotherSignal(t *testing.T) {
	data := []byte(`{"value":42}`)
	digest := sha256.Sum256(data)
	out, _ := json.Marshal(Outcome{ResultRef: "child-object", ResultHash: hex.EncodeToString(digest[:])})
	var entries []Entry
	c := NewContext(context.Background(), nil, func(_ context.Context, kind Kind, payload json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: payload})
		return nil
	}, Signal{Sequence: 9, Name: "child_0", Payload: out})
	lost := errors.New("temporary object read failure")
	calls := 0
	c.SetResultStore(nil, func(context.Context, string) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, lost
		}
		return data, nil
	})
	promise := Promise{SignalName: "child_0"}
	if _, err := AwaitPromise(c, promise); !errors.Is(err, lost) {
		t.Fatalf("first object read: %v", err)
	}
	got, err := AwaitPromise(c, promise)
	if err != nil || string(got) != string(data) {
		t.Fatalf("retry result=%s %v", got, err)
	}
	data[0] = '!'
	got, err = AwaitPromise(c, promise)
	if err != nil || string(got) != `{"value":42}` || calls != 2 || len(entries) != 2 {
		t.Fatalf("stable retry result=%s calls=%d entries=%d err=%v", got, calls, len(entries), err)
	}
}

func TestPromiseRepeatedChildFailureAndCorruptOutcome(t *testing.T) {
	for _, payload := range [][]byte{[]byte(`{"error":"child failed"}`), []byte(`bad`)} {
		var writes int
		c := NewContext(context.Background(), nil, func(context.Context, Kind, json.RawMessage) error { writes++; return nil }, Signal{Sequence: 1, Name: "child_0", Payload: payload})
		for i := 0; i < 2; i++ {
			_, err := AwaitPromise(c, Promise{SignalName: "child_0"})
			if string(payload) == "bad" {
				if !errors.Is(err, ErrCorruptJournal) {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != "child failed" {
				t.Fatal(err)
			}
		}
		if writes != 2 {
			t.Fatalf("repeated outcome added steps: %d", writes)
		}
	}
}

func TestPromiseResultCacheRemainsBounded(t *testing.T) {
	data := make([]byte, 6<<20)
	digest := sha256.Sum256(data)
	out, _ := json.Marshal(Outcome{ResultRef: "large", ResultHash: hex.EncodeToString(digest[:])})
	signals := []Signal{{Sequence: 1, Name: "child_0", Payload: out}, {Sequence: 2, Name: "child_1", Payload: out}, {Sequence: 3, Name: "child_2", Payload: out}}
	writes, loads := 0, 0
	c := NewContext(context.Background(), nil, func(context.Context, Kind, json.RawMessage) error { writes++; return nil }, signals...)
	c.SetResultStore(nil, func(context.Context, string) ([]byte, error) { loads++; return data, nil })
	for _, signal := range signals {
		p := Promise{SignalName: signal.Name}
		for i := 0; i < 2; i++ {
			got, err := AwaitPromise(c, p)
			if err != nil || len(got) != len(data) {
				t.Fatalf("large result=%d err=%v", len(got), err)
			}
		}
	}
	if c.promiseResultBytes > maxCachedPromiseResultBytes || loads != 4 || writes != 6 {
		t.Fatalf("cache bytes=%d loads=%d writes=%d", c.promiseResultBytes, loads, writes)
	}
}
