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

func TestReplayRequestEnvelopeBeforePlugin(t *testing.T) {
	sum := sha256.Sum256([]byte(`7`))
	cases := []struct{ name, payload string }{
		{"kind", `{"kind":"checkpoint","kind":"run","name":"step","input_hash":"hash"}`},
		{"name", `{"kind":"run","Name":"foreign","name":"step","input_hash":"hash"}`},
		{"hash", `{"kind":"run","name":"step","input_hash":"foreign","input_hash":"hash"}`},
		{"timer", `{"kind":"run","name":"step","Timer_step":2,"timer_step":1}`},
		{"child", `{"kind":"run","name":"step","child_id":"foreign","child_id":"id"}`},
		{"cases", `{"kind":"select_many","cases":[],"cases":[{"kind":"signal","name":"gate"}]}`},
		{"nested", `{"kind":"select_many","cases":[{"Kind":"timer","kind":"signal","name":"gate"}]}`},
		{"unknown", `{"kind":"run","name":"step","foreign":true}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := replayBundle{Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: hex.EncodeToString(sum[:]), Journal: []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: json.RawMessage(c.payload)}, Sequence: 2},
				{Entry: journal.Entry{Kind: journal.Completed, Index: 2, Payload: json.RawMessage(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 3},
			}}
			_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
			if !errors.Is(err, wf.ErrCorruptJournal) {
				t.Fatalf("legacy request envelope reached plugin: %v", err)
			}
		})
	}
}
