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

func TestReplayInputBindingBeforePlugin(t *testing.T) {
	hash := func(raw string) string { sum := sha256.Sum256([]byte(raw)); return hex.EncodeToString(sum[:]) }
	for _, format := range []string{"", wf.ReplayFormatGraphV1} {
		t.Run("format="+format, func(t *testing.T) {
			original := hash(`{"value":7}`)
			foreign := []byte(`{"value":7,"ignored":true}`)
			b := replayBundle{Type: "parent", ID: "id", InvSeq: 1, Format: format, Input: foreign, InputHash: hash(string(foreign)), Journal: []journal.Record{{Entry: journal.Entry{Kind: journal.Started, Payload: json.RawMessage(`{"input_sha256":"` + original + `"}`)}}, {Entry: journal.Entry{Kind: journal.Completed, Payload: json.RawMessage(`{"inv_seq":1,"result":"NDI="}`)}}}}
			_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
			if !errors.Is(err, wf.ErrReplayInputMismatch) {
				t.Fatal("changed input reached plugin loading", err)
			}
		})
	}
}
