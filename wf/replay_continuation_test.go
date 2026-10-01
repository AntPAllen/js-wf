package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"js-wf/journal"
)

type replayContinuationFixture struct {
	records                       []journal.Record
	objects                       map[string][]byte
	firstBoundary, secondBoundary int
	effects                       int
}

func buildReplayContinuationFixture(t *testing.T) *replayContinuationFixture {
	t.Helper()
	f := &replayContinuationFixture{objects: map[string][]byte{}}
	epoch := uint64(51)
	add := func(kind journal.Kind, payload json.RawMessage) error {
		index := uint64(len(f.records))
		f.records = append(f.records, journal.Record{Entry: journal.Entry{Index: index, Epoch: epoch, Kind: kind, Payload: bytes.Clone(payload), WorkerID: fmt.Sprintf("owner-%d", epoch)}, Sequence: index + 1})
		return nil
	}
	_ = add(journal.Started, nil)
	addSignal := func(seq uint64, value string) {
		payload, _ := json.Marshal(map[string]any{"sig_seq": seq, "name": "buffered", "payload": []byte(value)})
		_ = add(journal.SignalConsumed, payload)
	}
	addSignal(3, "first")
	configure := func(c *Context) {
		c.SetChildSupport("parent", "replay", 17, nil)
		c.SetResultStore(func(_ context.Context, raw []byte) (string, error) {
			hash := sha256.Sum256(raw)
			key := "step-result-" + hex.EncodeToString(hash[:])
			f.objects[key] = bytes.Clone(raw)
			return key, nil
		}, func(_ context.Context, key string) ([]byte, error) { return bytes.Clone(f.objects[key]), nil })
		c.SetContinuationSupport(func(stage string) bool { return stage == "middle_v1" || stage == "finish_v1" }, func(completed uint64, recorded bool) (ContinuationAnchor, error) {
			limit := uint64(len(f.records))
			if completed != 0 {
				limit = completed + 1
			}
			anchor := ContinuationAnchor{Index: uint64(len(f.records) + 1), Epoch: epoch}
			if recorded {
				anchor.Index--
			}
			for _, record := range f.records[:limit] {
				if record.Kind == journal.Attempt {
					anchor.PanicAttempts++
				}
				if record.Kind == journal.SignalConsumed {
					var event struct {
						Sequence uint64 `json:"sig_seq"`
					}
					_ = json.Unmarshal(record.Payload, &event)
					anchor.SignalCursor = event.Sequence
				}
			}
			if completed != 0 {
				anchor.Index, anchor.Epoch = completed, f.records[completed].Epoch
			}
			return anchor, nil
		})
	}
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		return add(journal.Kind(kind), payload)
	}
	c := NewContext(context.Background(), nil, appendFn, Signal{Sequence: 3, Name: "buffered", Payload: []byte("first")})
	configure(c)
	if err := c.SetState("total", 23); err != nil {
		t.Fatal(err)
	}
	if _, err := RunOnce(c, "prefix", 23, func(context.Context, string) (int, error) { f.effects++; return 46, nil }); err != nil {
		t.Fatal(err)
	}
	if err := Continue(c, "middle_v1", 45); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
	f.firstBoundary = len(f.records)
	_ = add(journal.Suspended, json.RawMessage(`{"waiting_on":"continuation:middle_v1"}`))
	point, _ := c.Continuation()
	epoch = 77
	_ = add(journal.Attempt, json.RawMessage(`{"count":1,"error":"prior stage panic"}`))
	addSignal(7, "second")
	var err error
	c, _, err = NewCheckpointContext(context.Background(), nil, appendFn, f.objects[point.Object], CheckpointLocation{Type: "parent", ID: "replay", InvSeq: 17, Index: point.Index, Epoch: point.Epoch, Hash: point.SHA256}, Signal{Sequence: 7, Name: "buffered", Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	configure(c)
	if value, err := AwaitSignal(c, "buffered"); err != nil || string(value) != "first" {
		t.Fatalf("signal=%q err=%v", value, err)
	}
	if err := c.SetState("total", 50); err != nil {
		t.Fatal(err)
	}
	if err := Continue(c, "finish_v1", 67); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
	f.secondBoundary = len(f.records)
	_ = add(journal.Suspended, json.RawMessage(`{"waiting_on":"continuation:finish_v1"}`))
	point, _ = c.Continuation()
	epoch = 99
	c, _, err = NewCheckpointContext(context.Background(), nil, appendFn, f.objects[point.Object], CheckpointLocation{Type: "parent", ID: "replay", InvSeq: 17, Index: point.Index, Epoch: point.Epoch, Hash: point.SHA256})
	if err != nil {
		t.Fatal(err)
	}
	configure(c)
	if value, err := AwaitSignal(c, "buffered"); err != nil || string(value) != "second" {
		t.Fatalf("signal=%q err=%v", value, err)
	}
	if _, err := RunOnce(c, "suffix", 23, func(context.Context, string) (int, error) { f.effects++; return 46, nil }); err != nil {
		t.Fatal(err)
	}
	var total int
	if found, err := c.GetState("total", &total); err != nil || !found || total != 50 {
		t.Fatalf("total=%d err=%v", total, err)
	}
	outcome, _ := json.Marshal(Outcome{InvSeq: 17, Result: []byte(`50`)})
	_ = add(journal.Completed, outcome)
	return f
}

func replayContinuationHandlers(effects, initial, middle, finish *int) (func(*Context) (int, error), map[string]ReplayContinuation[int]) {
	effect := func(context.Context, string) (int, error) { *effects++; return 46, nil }
	first := func(c *Context) (int, error) {
		*initial++
		if err := c.SetState("total", 23); err != nil {
			return 0, err
		}
		if _, err := RunOnce(c, "prefix", 23, effect); err != nil {
			return 0, err
		}
		return 0, Continue(c, "middle_v1", 45)
	}
	stages := map[string]ReplayContinuation[int]{
		"middle_v1": func(c *Context, locals json.RawMessage) (int, error) {
			*middle++
			if string(locals) != "45" {
				return 0, fmt.Errorf("middle locals %s", locals)
			}
			if value, err := AwaitSignal(c, "buffered"); err != nil || string(value) != "first" {
				return 0, fmt.Errorf("middle signal %q: %v", value, err)
			}
			if err := c.SetState("total", 50); err != nil {
				return 0, err
			}
			return 0, Continue(c, "finish_v1", 67)
		},
		"finish_v1": func(c *Context, locals json.RawMessage) (int, error) {
			*finish++
			if string(locals) != "67" {
				return 0, fmt.Errorf("finish locals %s", locals)
			}
			if value, err := AwaitSignal(c, "buffered"); err != nil || string(value) != "second" {
				return 0, fmt.Errorf("finish signal %q: %v", value, err)
			}
			if _, err := RunOnce(c, "suffix", 23, effect); err != nil {
				return 0, err
			}
			var total int
			_, err := c.GetState("total", &total)
			return total, err
		},
	}
	return first, stages
}

func TestReplayAcrossContinuationFramesAndHistoricalFacts(t *testing.T) {
	f := buildReplayContinuationFixture(t)
	var effects, initial, middle, finish int
	first, stages := replayContinuationHandlers(&effects, &initial, &middle, &finish)
	raw, _ := json.Marshal(f.records)
	var observation ReplayObservation
	value, err := ReplayWithContinuations(raw, first, stages, ReplayOptions{Type: "parent", ID: "replay", InvSeq: 17, Objects: f.objects, Observation: &observation})
	if err != nil || value != 50 || effects != 0 || initial != 1 || middle != 1 || finish != 1 || observation.Continuations != 2 || observation.Stage != "finish_v1" || observation.PlayedSteps != observation.RecordedSteps {
		t.Fatalf("value=%d err=%v calls=%d/%d/%d effects=%d observation=%+v", value, err, initial, middle, finish, effects, observation)
	}
	if f.effects != 2 {
		t.Fatal(f.effects)
	}
	for _, cut := range []int{f.firstBoundary, f.firstBoundary + 1, f.secondBoundary, f.secondBoundary + 1} {
		effects, initial, middle, finish = 0, 0, 0, 0
		raw, _ := json.Marshal(f.records[:cut])
		_, err := ReplayWithContinuations(raw, first, stages, ReplayOptions{Type: "parent", ID: "replay", InvSeq: 17, Objects: f.objects, Observation: &observation})
		if !errors.Is(err, ErrContinuation) || effects != 0 || finish != 0 || observation.WaitingOn == "" {
			t.Fatalf("cut=%d err=%v effects=%d finish=%d observation=%+v", cut, err, effects, finish, observation)
		}
	}
}

func TestReplayContinuationRejectsMissingObjectsUnknownStagesAndChanges(t *testing.T) {
	f := buildReplayContinuationFixture(t)
	raw, _ := json.Marshal(f.records)
	var effects, initial, middle, finish int
	first, stages := replayContinuationHandlers(&effects, &initial, &middle, &finish)
	opts := ReplayOptions{Type: "parent", ID: "replay", InvSeq: 17, Objects: f.objects}
	missing := map[string]ReplayContinuation[int]{"middle_v1": stages["middle_v1"]}
	if _, err := ReplayWithContinuations(raw, first, missing, opts); !errors.Is(err, ErrUnknownContinuation) || initial != 0 {
		t.Fatalf("unknown stage ran prefix: %v calls=%d", err, initial)
	}
	without := opts
	without.Objects = nil
	if _, err := ReplayWithContinuations(raw, first, stages, without); !errors.Is(err, ErrReplayObjectMissing) {
		t.Fatal(err)
	}
	changed := func(c *Context) (int, error) {
		if err := c.SetState("total", 23); err != nil {
			return 0, err
		}
		if _, err := RunOnce(c, "prefix", 23, func(context.Context, string) (int, error) { effects++; return 46, nil }); err != nil {
			return 0, err
		}
		return 0, Continue(c, "middle_v1", 46)
	}
	if _, err := ReplayWithContinuations(raw, changed, stages, opts); !errors.Is(err, ErrNonDeterministic) {
		t.Fatal(err)
	}
	for name, raw := range f.objects {
		f.objects[name] = append(bytes.Clone(raw), byte(' '))
		break
	}
	if _, err := ReplayWithContinuations(raw, first, stages, opts); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	if effects != 0 {
		t.Fatal("offline effect ran")
	}
}

func TestReplayContinuationPendingCorruptionAndStagePanic(t *testing.T) {
	f := buildReplayContinuationFixture(t)
	var effects, initial, middle, finish int
	first, stages := replayContinuationHandlers(&effects, &initial, &middle, &finish)
	opts := ReplayOptions{Type: "parent", ID: "replay", InvSeq: 17, Objects: f.objects}
	pending, _ := json.Marshal(f.records[:f.firstBoundary-1])
	if _, err := ReplayWithContinuations(pending, first, stages, opts); !errors.Is(err, ErrReplayPendingStep) || effects != 0 {
		t.Fatalf("pending checkpoint ran effect: %v effects=%d", err, effects)
	}
	broken := append([]journal.Record(nil), f.records[:f.firstBoundary+1]...)
	broken[len(broken)-1].Payload = json.RawMessage(`{"waiting_on":"continuation:wrong"}`)
	raw, _ := json.Marshal(broken)
	initial = 0
	if _, err := ReplayWithContinuations(raw, first, stages, opts); !errors.Is(err, ErrCorruptJournal) || initial != 0 {
		t.Fatalf("corrupt history entered handler: %v calls=%d", err, initial)
	}
	raw, _ = json.Marshal(f.records)
	opts.Observation = &ReplayObservation{}
	stages["finish_v1"] = func(*Context, json.RawMessage) (int, error) { panic("stage panic") }
	if _, err := ReplayWithContinuations(raw, first, stages, opts); err == nil || !opts.Observation.Panicked || opts.Observation.Stage != "finish_v1" {
		t.Fatalf("stage panic err=%v observation=%+v", err, opts.Observation)
	}
	if _, err := ReplayWithContinuations(raw, first, stages); !errors.Is(err, ErrInvocationIdentity) {
		t.Fatal(err)
	}
}

func TestReplayContinuationRejectsRebuiltStateMismatch(t *testing.T) {
	f := buildReplayContinuationFixture(t)
	var effects, initial, middle, finish int
	_, stages := replayContinuationHandlers(&effects, &initial, &middle, &finish)
	initialHandler := func(c *Context) (int, error) {
		if err := c.SetState("total", 23); err != nil {
			return 0, err
		}
		if _, err := RunOnce(c, "prefix", 23, func(context.Context, string) (int, error) { effects++; return 46, nil }); err != nil {
			return 0, err
		}
		// Model an SDK state-application defect while declarations still match.
		// Loading a trusted frame directly would conceal this wrong rebuilt state.
		c.state["total"] = json.RawMessage(`99`)
		return 0, Continue(c, "middle_v1", 45)
	}
	raw, _ := json.Marshal(f.records)
	if _, err := ReplayWithContinuations(raw, initialHandler, stages, ReplayOptions{Type: "parent", ID: "replay", InvSeq: 17, Objects: f.objects}); !errors.Is(err, ErrNonDeterministic) || effects != 0 || middle != 0 {
		t.Fatalf("rebuilt state mismatch concealed: err=%v effects=%d middle=%d", err, effects, middle)
	}
}

func TestReplayContinuationChecksTerminalResultAndObjectHash(t *testing.T) {
	f := buildReplayContinuationFixture(t)
	var effects, initial, middle, finish int
	first, stages := replayContinuationHandlers(&effects, &initial, &middle, &finish)
	opts := ReplayOptions{Type: "parent", ID: "replay", InvSeq: 17, Objects: f.objects}
	original := stages["finish_v1"]
	stages["finish_v1"] = func(c *Context, locals json.RawMessage) (int, error) {
		value, err := original(c, locals)
		return value + 1, err
	}
	raw, _ := json.Marshal(f.records)
	if _, err := ReplayWithContinuations(raw, first, stages, opts); !errors.Is(err, ErrNonDeterministic) {
		t.Fatalf("changed terminal accepted: %v", err)
	}
	stages["finish_v1"] = original
	result := []byte(`50`)
	hash := sha256.Sum256(result)
	name := "terminal-result-" + hex.EncodeToString(hash[:])
	f.objects[name] = result
	f.records[len(f.records)-1].Payload, _ = json.Marshal(Outcome{InvSeq: 17, ResultRef: name, ResultHash: hex.EncodeToString(hash[:])})
	raw, _ = json.Marshal(f.records)
	if value, err := ReplayWithContinuations(raw, first, stages, opts); err != nil || value != 50 {
		t.Fatalf("terminal blob value=%d err=%v", value, err)
	}
	f.objects[name] = []byte(`51`)
	if _, err := ReplayWithContinuations(raw, first, stages, opts); !errors.Is(err, ErrCorruptJournal) {
		t.Fatalf("corrupt terminal object accepted: %v", err)
	}
	delete(f.objects, name)
	if _, err := ReplayWithContinuations(raw, first, stages, opts); !errors.Is(err, ErrReplayObjectMissing) {
		t.Fatalf("missing terminal object accepted: %v", err)
	}
	if effects != 0 {
		t.Fatal("offline effect ran")
	}
}
