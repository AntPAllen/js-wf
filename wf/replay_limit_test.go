package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestReplayRetainedLimitRequest(t *testing.T) {
	var declaration json.RawMessage
	original := NewContext(context.Background(), nil, func(_ context.Context, kind Kind, payload json.RawMessage) error {
		if kind == StepRequested {
			declaration = bytes.Clone(payload)
		}
		return journal.ErrTooLong
	})
	_, _ = Run(original, "rejected", 7, func(context.Context) (int, error) { t.Fatal("fixture effect ran"); return 0, nil })
	valid := Outcome{InvSeq: 17, Error: journal.ErrTooLong.Error(), LimitRequest: declaration}
	cases := []struct {
		name     string
		change   func(*Outcome)
		identity bool
		want     error
		calls    int
	}{
		{name: "pending", identity: true, want: ErrReplayPendingStep, calls: 1},
		{name: "identity required", want: ErrInvocationIdentity},
		{name: "wrong generation", identity: true, change: func(o *Outcome) { o.InvSeq++ }, want: ErrCorruptJournal},
		{name: "wrong failure", identity: true, change: func(o *Outcome) { o.Error = "other" }, want: ErrCorruptJournal},
		{name: "null request", identity: true, change: func(o *Outcome) { o.LimitRequest = json.RawMessage(`null`) }, want: ErrCorruptJournal},
		{name: "empty declaration", identity: true, change: func(o *Outcome) { o.LimitRequest = json.RawMessage(`{}`) }, want: ErrCorruptJournal},
		{name: "array request", identity: true, change: func(o *Outcome) { o.LimitRequest = json.RawMessage(`[]`) }, want: ErrCorruptJournal},
		{name: "result ambiguity", identity: true, change: func(o *Outcome) { o.Result = []byte(`1`) }, want: ErrCorruptJournal},
		{name: "entry ambiguity", identity: true, change: func(o *Outcome) { o.LimitEntry = &LimitEntry{Kind: "Suspended", Payload: json.RawMessage(`{}`)} }, want: ErrCorruptJournal},
		{name: "changed declaration", identity: true, change: func(o *Outcome) {
			o.LimitRequest = json.RawMessage(`{"kind":"run","name":"different","input_hash":"changed"}`)
		}, want: ErrNonDeterministic, calls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := valid
			if tc.change != nil {
				tc.change(&outcome)
			}
			payload, _ := json.Marshal(outcome)
			records := []journal.Record{{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1}, {Entry: journal.Entry{Index: 1, Kind: journal.Failed, Payload: payload}, Sequence: 2}}
			raw, _ := json.Marshal(records)
			before := bytes.Clone(raw)
			var obs ReplayObservation
			opts := ReplayOptions{Observation: &obs}
			if tc.identity {
				opts.Type = "limit"
				opts.ID = "test"
				opts.InvSeq = 17
			}
			calls, effects := 0, 0
			_, err := Replay(raw, func(c *Context) (int, error) {
				calls++
				return Run(c, "rejected", 7, func(context.Context) (int, error) { effects++; return 0, nil })
			}, opts)
			if !errors.Is(err, tc.want) || calls != tc.calls || effects != 0 || !bytes.Equal(raw, before) {
				t.Fatalf("err=%v want=%v calls=%d effects=%d", err, tc.want, calls, effects)
			}
			if tc.name == "pending" && (obs.PlayedSteps != 1 || obs.RecordedSteps != 1) {
				t.Fatalf("observation=%+v", obs)
			}
		})
	}
}

func TestReplayLimitCannotHidePendingRequest(t *testing.T) {
	outcome, _ := json.Marshal(Outcome{InvSeq: 17, Error: journal.ErrTooLong.Error(), LimitRequest: json.RawMessage(`{"kind":"run","name":"extra"}`)})
	records := []journal.Record{{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1}, {Entry: journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"run","name":"pending"}`)}, Sequence: 2}, {Entry: journal.Entry{Index: 2, Kind: journal.Failed, Payload: outcome}, Sequence: 3}}
	raw, _ := json.Marshal(records)
	_, err := Replay(raw, func(*Context) (int, error) { t.Fatal("handler entered"); return 0, nil }, ReplayOptions{Type: "limit", ID: "test", InvSeq: 17})
	if !errors.Is(err, ErrCorruptJournal) {
		t.Fatal(err)
	}
}

func TestReplayLimitUnknownContinuationBeforeHandler(t *testing.T) {
	outcome, _ := json.Marshal(Outcome{InvSeq: 17, Error: journal.ErrTooLong.Error(), LimitRequest: json.RawMessage(`{"kind":"checkpoint","name":"missing"}`)})
	records := []journal.Record{{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1}, {Entry: journal.Entry{Index: 1, Kind: journal.Failed, Payload: outcome}, Sequence: 2}}
	raw, _ := json.Marshal(records)
	_, err := ReplayWithContinuations(raw, func(*Context) (int, error) { t.Fatal("handler entered"); return 0, nil }, nil, ReplayOptions{Type: "limit", ID: "test", InvSeq: 17})
	if !errors.Is(err, ErrUnknownContinuation) {
		t.Fatal(err)
	}
}

func TestReplayLimitAtHardJournalCap(t *testing.T) {
	input, _ := json.Marshal(7)
	sum := sha256.Sum256(input)
	declaration, _ := json.Marshal(request{Kind: "run", Name: "step", InputHash: hex.EncodeToString(sum[:])})
	done, _ := json.Marshal(completion{Result: json.RawMessage(`1`)})
	terminal, _ := json.Marshal(Outcome{InvSeq: 17, Error: journal.ErrTooLong.Error(), LimitRequest: declaration})
	records := make([]journal.Record, journal.MaxEntries)
	for i := range records {
		kind := journal.StepRequested
		payload := declaration
		if i == 0 {
			kind = journal.Started
			payload = nil
		} else if i == len(records)-1 {
			kind = journal.Failed
			payload = terminal
		} else if i%2 == 0 {
			kind = journal.StepCompleted
			payload = done
		}
		records[i] = journal.Record{Entry: journal.Entry{Index: uint64(i), Kind: kind, Payload: payload}, Sequence: uint64(i + 1)}
	}
	raw, _ := json.Marshal(records)
	var observed ReplayObservation
	effects := 0
	_, err := Replay(raw, func(c *Context) (int, error) {
		for i := 0; i < journal.MaxEntries/2; i++ {
			n, err := Run(c, "step", 7, func(context.Context) (int, error) { effects++; return 1, nil })
			if err != nil {
				return 0, err
			}
			if n != 1 {
				t.Fatalf("result=%d", n)
			}
		}
		return 0, nil
	}, ReplayOptions{Type: "limit", ID: "test", InvSeq: 17, Observation: &observed})
	if !errors.Is(err, ErrReplayPendingStep) || effects != 0 || observed.PlayedSteps != journal.MaxEntries-1 || observed.PlayedSteps != observed.RecordedSteps {
		t.Fatalf("err=%v effects=%d observation=%+v", err, effects, observed)
	}
}
