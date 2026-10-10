package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestRequestEnvelopeKeepsHashedUserInputOpaque(t *testing.T) {
	input := json.RawMessage(`{"kind":"foreign","kind":"opaque","Name":"user"}`)
	digest := sha256.Sum256(input)
	payload, err := json.Marshal(request{Kind: "run", Name: "step", InputHash: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	c := NewContext(context.Background(), []Entry{
		{Kind: StepRequested, Payload: payload},
		{Kind: StepCompleted, Index: 1, Payload: []byte(`{"result":42}`)},
	}, nil)
	calls := 0
	result, err := Run(c, "step", input, func(context.Context) (int, error) { calls++; return 0, nil })
	if err != nil || result != 42 || calls != 0 {
		t.Fatalf("opaque input replay: result=%d calls=%d err=%v", result, calls, err)
	}
}

func TestReplayRequestEnvelopeBeforeHandler(t *testing.T) {
	for name, payload := range map[string]string{
		"duplicate_kind":  `{"kind":"run","kind":"checkpoint","name":"next","input_hash":"hash"}`,
		"alias_kind":      `{"Kind":"run","kind":"checkpoint","name":"next","input_hash":"hash"}`,
		"escaped_kind":    `{"\u006bind":"run","kind":"checkpoint","name":"next","input_hash":"hash"}`,
		"duplicate_name":  `{"kind":"checkpoint","name":"foreign","name":"next","input_hash":"hash"}`,
		"alias_name":      `{"kind":"checkpoint","Name":"foreign","name":"next","input_hash":"hash"}`,
		"duplicate_hash":  `{"kind":"checkpoint","name":"next","input_hash":"foreign","input_hash":"hash"}`,
		"alias_hash":      `{"kind":"checkpoint","name":"next","Input_hash":"foreign","input_hash":"hash"}`,
		"unknown_field":   `{"kind":"checkpoint","name":"next","input_hash":"hash","foreign":true}`,
		"duplicate_child": `{"kind":"call","name":"next","child_type":"foreign","child_type":"child","child_id":"id"}`,
		"alias_timer":     `{"kind":"timer","name":"next","Timer_step":2,"timer_step":1}`,
		"duplicate_cases": `{"kind":"select_many","cases":[],"cases":[{"kind":"signal","name":"gate"}]}`,
		"nested_case":     `{"kind":"select_many","cases":[{"kind":"signal","name":"foreign","name":"gate"}]}`,
		"nested_alias":    `{"kind":"select_many","cases":[{"Kind":"timer","kind":"signal","name":"gate"}]}`,
		"missing":         ``,
		"null":            `null`,
	} {
		t.Run(name, func(t *testing.T) {
			records := []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: json.RawMessage(payload)}, Sequence: 2},
				{Entry: journal.Entry{Kind: journal.Completed, Index: 2, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 3},
			}
			raw, err := json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			_, err = Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{})
			if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
				t.Fatalf("ambiguous declaration reached handler: calls=%d err=%v", calls, err)
			}
		})
	}
}
