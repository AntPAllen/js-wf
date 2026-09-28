package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/journal"
)

func TestOfflineReplayNeverRunsIncompleteEffect(t *testing.T) {
	var requested json.RawMessage
	ctx := NewContext(context.Background(), nil, func(_ context.Context, kind Kind, payload json.RawMessage) error {
		if kind == StepRequested {
			requested = append([]byte(nil), payload...)
		}
		return errors.New("stop before effect")
	})
	if _, err := Run(ctx, "pending", 7, func(context.Context) (int, error) { return 1, nil }); err == nil || len(requested) == 0 {
		t.Fatalf("request fixture: %v", err)
	}
	records := []journal.Record{
		{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1},
		{Entry: journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: requested}, Sequence: 2},
	}
	data, _ := json.Marshal(records)
	var effects atomic.Int32
	var observed ReplayObservation
	_, err := Replay(data, func(c *Context) (int, error) {
		return Run(c, "pending", 7, func(context.Context) (int, error) { effects.Add(1); return 1, nil })
	}, ReplayOptions{Observation: &observed})
	if !errors.Is(err, ErrReplayPendingStep) || effects.Load() != 0 || observed.PlayedSteps != 1 || observed.RecordedSteps != 1 {
		t.Fatalf("pending replay: err=%v effects=%d observation=%+v", err, effects.Load(), observed)
	}
}

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

func TestOfflineReplayObservesSuspensionAndFailure(t *testing.T) {
	encode := func(records []journal.Record) []byte {
		t.Helper()
		data, err := json.Marshal(records)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	started := journal.Record{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1}
	signalRecords := []journal.Record{
		started,
		{Entry: journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"signal","name":"go"}`)}, Sequence: 2},
		{Entry: journal.Entry{Index: 2, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"signal:go"}`)}, Sequence: 3},
	}
	var observed ReplayObservation
	_, err := Replay(encode(signalRecords), func(c *Context) (int, error) {
		_, err := AwaitSignal(c, "go")
		return 0, err
	}, ReplayOptions{Observation: &observed})
	if !errors.Is(err, ErrSuspended) || observed.WaitingOn != "signal:go" || observed.PlayedSteps != 1 || observed.RecordedSteps != 1 {
		t.Fatalf("signal suspension: err=%v observed=%+v", err, observed)
	}
	request, _ := json.Marshal(struct {
		Kind          string    `json:"kind"`
		Name          string    `json:"name"`
		DurationNanos int64     `json:"duration_nanos"`
		FireAt        time.Time `json:"fire_at"`
	}{"timer", "delay", int64(time.Second), time.Now().Add(time.Hour)})
	timerRecords := []journal.Record{
		started,
		{Entry: journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: request}, Sequence: 2},
		{Entry: journal.Entry{Index: 2, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"timer:delay"}`)}, Sequence: 3},
	}
	observed = ReplayObservation{}
	_, err = Replay(encode(timerRecords), func(c *Context) (int, error) { return 0, Sleep(c, "delay", time.Second) }, ReplayOptions{Observation: &observed})
	if !errors.Is(err, ErrSuspended) || observed.WaitingOn != "timer:delay" || observed.PlayedSteps != 1 || observed.RecordedSteps != 1 {
		t.Fatalf("timer suspension: err=%v observed=%+v", err, observed)
	}
	input, _ := json.Marshal(7)
	hash := sha256.Sum256(input)
	failureRequest, _ := json.Marshal(map[string]string{"kind": "run", "name": "fail", "input_hash": hex.EncodeToString(hash[:])})
	failureRecords := []journal.Record{
		started,
		{Entry: journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: failureRequest}, Sequence: 2},
		{Entry: journal.Entry{Index: 2, Kind: journal.StepCompleted, Payload: json.RawMessage(`{"error":"boom"}`)}, Sequence: 3},
		{Entry: journal.Entry{Index: 3, Kind: journal.Failed, Payload: json.RawMessage(`{"inv_seq":1,"error":"boom"}`)}, Sequence: 4},
	}
	observed = ReplayObservation{}
	_, err = Replay(encode(failureRecords), func(c *Context) (int, error) {
		_, err := Run(c, "fail", 7, func(context.Context) (int, error) {
			t.Fatal("recorded failed effect ran during replay")
			return 0, nil
		})
		return 0, err
	}, ReplayOptions{Observation: &observed})
	if err == nil || err.Error() != "boom" || observed.PlayedSteps != 2 || observed.RecordedSteps != 2 {
		t.Fatalf("failed step: err=%v observed=%+v", err, observed)
	}
	panicRecords := []journal.Record{
		started,
		{Entry: journal.Entry{Index: 1, Kind: journal.Attempt, Payload: json.RawMessage(`{"count":1,"error":"workflow panic: boom"}`)}, Sequence: 2},
		{Entry: journal.Entry{Index: 2, Kind: journal.Failed, Payload: json.RawMessage(`{"inv_seq":1,"error":"workflow panic: boom"}`)}, Sequence: 3},
	}
	observed = ReplayObservation{}
	_, err = Replay(encode(panicRecords), func(*Context) (int, error) { panic("boom") }, ReplayOptions{Observation: &observed})
	if err == nil || err.Error() != "workflow panic: boom" || observed.PlayedSteps != 0 || observed.RecordedSteps != 0 {
		t.Fatalf("failed panic: err=%v observed=%+v", err, observed)
	}
}

func TestOfflineReplayObservesChildSuspension(t *testing.T) {
	var emitted []Entry
	c := NewContext(context.Background(), nil, func(_ context.Context, kind Kind, payload json.RawMessage) error {
		emitted = append(emitted, Entry{Kind: kind, Payload: payload})
		return nil
	})
	c.SetChildSupport("parent", "one", 1, func(context.Context, string, string, []byte, string) error { return nil })
	if _, err := Call(c, "child", []byte(`{"id":1}`)); !errors.Is(err, ErrSuspended) {
		t.Fatalf("record child call: %v", err)
	}
	records := []journal.Record{{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1}}
	for _, e := range emitted {
		records = append(records, journal.Record{Entry: journal.Entry{Index: uint64(len(records)), Kind: journal.Kind(e.Kind), Payload: e.Payload}, Sequence: uint64(len(records) + 1)})
	}
	waitingOn := c.WaitingOn()
	tail, _ := json.Marshal(map[string]string{"waiting_on": waitingOn})
	records = append(records, journal.Record{Entry: journal.Entry{Index: uint64(len(records)), Kind: journal.Suspended, Payload: tail}, Sequence: uint64(len(records) + 1)})
	data, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	var observed ReplayObservation
	_, err = Replay(data, func(c *Context) (int, error) {
		_, err := Call(c, "child", []byte(`{"id":1}`))
		return 0, err
	}, ReplayOptions{Type: "parent", ID: "one", InvSeq: 1, Observation: &observed})
	if !errors.Is(err, ErrSuspended) || observed.WaitingOn != waitingOn || observed.PlayedSteps != 1 || observed.RecordedSteps != 1 {
		t.Fatalf("child suspension: err=%v observed=%+v", err, observed)
	}
}
