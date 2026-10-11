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

func TestReplayTerminalResultBeforePlugin(t *testing.T) {
	sum := sha256.Sum256([]byte(`7`))
	hash := hex.EncodeToString(sum[:])
	for _, format := range []string{"", wf.ReplayFormatGraphV1} {
		t.Run("format="+format, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				kind    journal.Kind
				out     wf.Outcome
				objects map[string][]byte
				want    error
			}{
				{"completed_error", journal.Completed, wf.Outcome{Error: "failed"}, nil, wf.ErrCorruptJournal},
				{"failed_inline", journal.Failed, wf.Outcome{Error: "failed", Result: []byte(`7`)}, nil, wf.ErrCorruptJournal},
				{"failed_external", journal.Failed, wf.Outcome{Error: "failed", ResultRef: "terminal", ResultHash: hash}, map[string][]byte{"terminal": []byte(`7`)}, wf.ErrCorruptJournal},
				{"object_missing", journal.Completed, wf.Outcome{ResultRef: "terminal", ResultHash: hash}, nil, wf.ErrReplayObjectMissing},
				{"object_wrong", journal.Completed, wf.Outcome{ResultRef: "terminal", ResultHash: hash}, map[string][]byte{"terminal": []byte(`8`)}, wf.ErrCorruptJournal},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tc.out.InvSeq = 1
					payload, err := json.Marshal(tc.out)
					if err != nil {
						t.Fatal(err)
					}
					bundle := replayBundle{Format: format, Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: hash, Objects: tc.objects, Journal: []journal.Record{
						{Entry: journal.Entry{Kind: journal.Started, Payload: json.RawMessage(`{"input_sha256":"` + hash + `"}`)}, Sequence: 1},
						{Entry: journal.Entry{Kind: tc.kind, Index: 1, Payload: payload}, Sequence: 2},
					}}
					_, err = runReplayBundle(bundle, "/does-not-exist.so", "Workflow")
					if !errors.Is(err, tc.want) {
						t.Fatalf("terminal reached plugin loading: %v want=%v", err, tc.want)
					}
				})
			}
		})
	}
}
