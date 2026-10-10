package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
	"js-wf/wf"
)

func TestReplayCompletionEnvelopeBeforePlugin(t *testing.T) {
	sum := sha256.Sum256([]byte(`7`))
	cases := []struct{ name, payload string }{
		{"result", `{"result":7,"result":42}`},
		{"reference", `{"result_ref":"foreign","result_ref":"owned"}`},
		{"hash", `{"result_hash":"foreign","result_hash":"owned"}`},
		{"error", `{"error":"foreign","error":""}`},
		{"signal", `{"signal_seq":9,"signal_seq":1}`},
		{"selected", `{"selected":"timer","selected":"signal"}`},
		{"case", `{"case_index":9,"case_index":0}`},
		{"cancelled", `{"cancelled":false,"cancelled":true}`},
		{"unknown", `{"result":42,"foreign":true}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := replayBundle{Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: hex.EncodeToString(sum[:]), Journal: []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: json.RawMessage(`{"kind":"run","name":"step","input_hash":"hash"}`)}, Sequence: 2},
				{Entry: journal.Entry{Kind: journal.StepCompleted, Index: 2, Payload: json.RawMessage(c.payload)}, Sequence: 3},
				{Entry: journal.Entry{Kind: journal.Completed, Index: 3, Payload: json.RawMessage(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 4},
			}}
			_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
			if !errors.Is(err, wf.ErrCorruptJournal) {
				t.Fatalf("legacy completion envelope reached plugin: %v", err)
			}
		})
	}
}
