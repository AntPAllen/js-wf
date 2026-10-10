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

func TestReplayCompletionEnvelopeBeforeHandler(t *testing.T) {
	for name, payload := range map[string]string{
		"result":             `{"result":7,"result":42}`,
		"result_alias":       `{"Result":7,"result":42}`,
		"escaped_result":     `{"\u0072esult":7,"result":42}`,
		"reference":          `{"result_ref":"foreign","result_ref":"owned"}`,
		"hash":               `{"result_hash":"foreign","result_hash":"owned"}`,
		"error":              `{"error":"foreign","error":""}`,
		"error_kind":         `{"error_kind":"foreign","error_kind":""}`,
		"signal":             `{"signal_seq":9,"signal_seq":1}`,
		"selected":           `{"selected":"timer","selected":"signal"}`,
		"case":               `{"case_index":9,"case_index":0}`,
		"cancelled":          `{"cancelled":false,"cancelled":true}`,
		"metadata_reference": `{"checkpoint_metadata_ref":"foreign","checkpoint_metadata_ref":"owned"}`,
		"metadata_hash":      `{"checkpoint_metadata_hash":"foreign","checkpoint_metadata_hash":"owned"}`,
		"unknown":            `{"result":42,"foreign":true}`,
		"metadata_object":    `{"checkpoint_metadata_ref":{"foreign":true}}`,
		"metadata_null":      `{"checkpoint_metadata_hash":null}`,
		"missing":            ``,
		"null":               `null`,
	} {
		t.Run(name, func(t *testing.T) {
			records := []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: []byte(`{"kind":"run","name":"step","input_hash":"hash"}`)}, Sequence: 2},
				{Entry: journal.Entry{Kind: journal.StepCompleted, Index: 2, Payload: json.RawMessage(payload)}, Sequence: 3},
				{Entry: journal.Entry{Kind: journal.Completed, Index: 3, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 4},
			}
			raw, err := json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			_, err = Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{})
			if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
				t.Fatalf("completion reached handler: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestCompletionEnvelopeKeepsUserResultOpaque(t *testing.T) {
	digest := sha256.Sum256([]byte(`null`))
	payload, err := json.Marshal(request{Kind: "run", Name: "step", InputHash: hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	c := NewContext(context.Background(), []Entry{
		{Kind: StepRequested, Payload: payload},
		{Kind: StepCompleted, Index: 1, Payload: []byte(`{"result":{"key":1,"key":2,"Name":"opaque"}}`)},
	}, nil)
	calls := 0
	result, err := Run(c, "step", nil, func(context.Context) (json.RawMessage, error) { calls++; return nil, nil })
	if err != nil || string(result) != `{"key":1,"key":2,"Name":"opaque"}` || calls != 0 {
		t.Fatalf("opaque result: %s calls=%d err=%v", result, calls, err)
	}
}
