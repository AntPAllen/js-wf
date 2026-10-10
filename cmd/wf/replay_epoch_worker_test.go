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

func TestReplayEpochWorkerBeforePlugin(t *testing.T) {
	sum := sha256.Sum256([]byte(`7`))
	hash := hex.EncodeToString(sum[:])
	for _, format := range []string{"", wf.ReplayFormatGraphV1} {
		t.Run("format="+format, func(t *testing.T) {
			b := replayBundle{Format: format, Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: hash, Journal: []journal.Record{{Entry: journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1, WorkerID: "one", Payload: json.RawMessage(`{"input_sha256":"` + hash + `"}`)}, Sequence: 1}, {Entry: journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 1, WorkerID: "two", Payload: json.RawMessage(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 2}}}
			_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
			if !errors.Is(err, wf.ErrCorruptJournal) {
				t.Fatalf("ownership conflict reached plugin: %v", err)
			}
		})
	}
}
