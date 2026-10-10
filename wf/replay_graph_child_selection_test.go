package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"js-wf/journal"
)

// Requests/completions come from the SDK. A consumed ordinary signal is then
// labeled canonical; choosing it as a child's result is forbidden even though
// its name happens to match a later child request.
func TestReplayGraphChildSelectedSignal(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, mode := range []string{"legacy", "unbound", "bound"} {
			t.Run(fmt.Sprintf("async=%t/%s", async, mode), func(t *testing.T) {
				childOutcome, _ := json.Marshal(Outcome{InvSeq: 2, Result: []byte(`42`)})
				event := map[string]any{"sig_seq": 1, "name": "child_0", "payload": childOutcome}
				if mode != "legacy" {
					event["canonical_signal"] = map[string]any{"index": 0, "token": "owned"}
				}
				body, _ := json.Marshal(event)
				records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: body}}}
				appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
					records = append(records, journal.Record{Entry: journal.Entry{Kind: journal.Kind(kind), Payload: append(json.RawMessage(nil), payload...)}})
					return nil
				}
				fn := func(c *Context) (json.RawMessage, error) {
					if !async {
						raw, err := Call(c, "child", []byte(`7`))
						return json.RawMessage(raw), err
					}
					promise, err := CallAsync(c, "child", []byte(`7`))
					if err != nil {
						return nil, err
					}
					raw, err := AwaitPromise(c, promise)
					return json.RawMessage(raw), err
				}
				c := NewContext(context.Background(), nil, appendFn, Signal{Sequence: 1, Name: "child_0", Payload: childOutcome})
				c.SetChildSupport("parent", "id", 1, func(context.Context, string, string, []byte, string) error { return nil })
				result, err := fn(c)
				if err != nil || string(result) != "42" {
					t.Fatal("SDK fixture", string(result), err)
				}
				if mode == "bound" {
					var request struct {
						Type string `json:"child_type"`
						ID   string `json:"child_id"`
					}
					if json.Unmarshal(records[2].Payload, &request) != nil || request.Type != "child" || request.ID == "" {
						t.Fatal("child request fixture")
					}
					event["graph_child"] = map[string]any{"type": request.Type, "id": request.ID, "inv_seq": 2}
					body, _ = json.Marshal(event)
					signal := records[1]
					signal.Payload = body
					// Keep the SDK step order, placing consumption after its declaration.
					records = append(records[:1], records[2:]...)
					records = append(records[:2], append([]journal.Record{signal}, records[2:]...)...)
				}
				terminal, _ := json.Marshal(Outcome{InvSeq: 1, Result: result})
				records = append(records, journal.Record{Entry: journal.Entry{Kind: journal.Completed, Payload: terminal}})
				for i := range records {
					records[i].Index = uint64(i)
					records[i].Epoch = 1
					records[i].Sequence = uint64(i + 1)
				}
				raw, _ := json.Marshal(records)
				result, err = Replay(raw, fn, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1})
				if mode == "unbound" {
					if !errors.Is(err, ErrCorruptJournal) {
						t.Fatalf("ordinary signal became child result=%s err=%v", result, err)
					}
				} else if err != nil || string(result) != "42" {
					t.Fatalf("%s result=%s err=%v", mode, result, err)
				}
			})
		}
	}
}

func TestReplayGraphChildSelectedAfterCheckpoint(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, child := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%t/child=%t", legacy, child), func(t *testing.T) {
				outcome, _ := json.Marshal(Outcome{InvSeq: 2, Result: []byte(`42`)})
				event := map[string]any{"sig_seq": 1, "name": "child_2", "payload": outcome}
				if !legacy {
					event["canonical_signal"] = map[string]any{"index": 0, "token": "owned"}
				}
				body, _ := json.Marshal(event)
				records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: body}}}
				add := func(kind journal.Kind, payload json.RawMessage) {
					records = append(records, journal.Record{Entry: journal.Entry{Kind: kind, Payload: append(json.RawMessage(nil), payload...)}})
				}
				appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
					add(journal.Kind(kind), payload)
					return nil
				}
				objects := map[string][]byte{}
				configure := func(c *Context) {
					c.SetChildSupport("parent", "id", 1, func(context.Context, string, string, []byte, string) error { return nil })
					c.SetResultStore(func(_ context.Context, data []byte) (string, error) {
						sum := sha256.Sum256(data)
						name := "step-result-" + hex.EncodeToString(sum[:])
						objects[name] = append([]byte(nil), data...)
						return name, nil
					}, func(_ context.Context, name string) ([]byte, error) { return objects[name], nil })
					c.SetContinuationSupport(func(stage string) bool { return stage == "finish_v1" }, func(uint64, bool) (ContinuationAnchor, error) {
						return ContinuationAnchor{Index: uint64(len(records) + 1), Epoch: 1, SignalCursor: 1}, nil
					})
				}
				initial := func(c *Context) (json.RawMessage, error) { return nil, Continue(c, "finish_v1", nil) }
				finish := func(c *Context, _ json.RawMessage) (json.RawMessage, error) {
					if !child {
						raw, err := AwaitSignal(c, "child_2")
						if err != nil {
							return nil, err
						}
						var out Outcome
						if json.Unmarshal(raw, &out) != nil {
							return nil, ErrCorruptJournal
						}
						return json.RawMessage(out.Result), nil
					}
					promise, err := CallAsync(c, "child", []byte(`7`))
					if err != nil {
						return nil, err
					}
					raw, err := AwaitPromise(c, promise)
					return json.RawMessage(raw), err
				}
				c := NewContext(context.Background(), nil, appendFn, Signal{Sequence: 1, Name: "child_2", Payload: outcome})
				configure(c)
				if _, err := initial(c); !errors.Is(err, ErrContinuation) {
					t.Fatal("SDK checkpoint", err)
				}
				point, ok := c.Continuation()
				if !ok {
					t.Fatal("missing checkpoint")
				}
				add(journal.Suspended, json.RawMessage(`{"waiting_on":"continuation:finish_v1"}`))
				c, _, err := NewCheckpointContext(context.Background(), nil, appendFn, objects[point.Object], CheckpointLocation{Type: "parent", ID: "id", InvSeq: 1, Index: point.Index, Epoch: point.Epoch, Hash: point.SHA256})
				if err != nil {
					t.Fatal(err)
				}
				configure(c)
				result, err := finish(c, nil)
				if err != nil || string(result) != "42" {
					t.Fatal("SDK suffix fixture", string(result), err)
				}
				terminal, _ := json.Marshal(Outcome{InvSeq: 1, Result: result})
				add(journal.Completed, terminal)
				for i := range records {
					records[i].Index = uint64(i)
					records[i].Epoch = 1
					records[i].Sequence = uint64(i + 1)
				}
				raw, _ := json.Marshal(records)
				result, err = ReplayWithContinuations(raw, initial, map[string]ReplayContinuation[json.RawMessage]{"finish_v1": finish}, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1, Objects: objects})
				if child && !legacy {
					if !errors.Is(err, ErrCorruptJournal) {
						t.Fatalf("restored ordinary signal became child result=%s err=%v", result, err)
					}
				} else if err != nil || string(result) != "42" {
					t.Fatal("valid restored signal", string(result), err)
				}
			})
		}
	}
}
