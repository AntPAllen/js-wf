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

func TestReplayRecordOrderBeforePlugin(t *testing.T) {
	hash := sha256.Sum256([]byte(`7`))
	digest := hex.EncodeToString(hash[:])
	tests := []struct {
		name   string
		change func([]journal.Record) []journal.Record
	}{
		{"index_gap", func(r []journal.Record) []journal.Record { r[1].Index = 2; return r }},
		{"zero_sequence", func(r []journal.Record) []journal.Record { r[0].Sequence = 0; return r }},
		{"repeated_sequence", func(r []journal.Record) []journal.Record { r[1].Sequence = 1; return r }},
		{"backwards_epoch", func(r []journal.Record) []journal.Record { r[0].Epoch = 2; return r }},
		{"repeated_started", func(r []journal.Record) []journal.Record { r[1].Kind = journal.Started; return r }},
		{"after_terminal", func(r []journal.Record) []journal.Record {
			return append(r, journal.Record{Entry: journal.Entry{Index: 2, Epoch: 1, Kind: journal.Suspended}, Sequence: 3})
		}},
		{"unknown_kind", func(r []journal.Record) []journal.Record { r[1].Kind = "foreign"; return r }},
		{"foreign_terminal", func(r []journal.Record) []journal.Record {
			r[1].Payload = []byte(`{"inv_seq":2,"result":"NDI="}`)
			return r
		}},
		{"empty_failure", func(r []journal.Record) []journal.Record { r[1].Kind = journal.Failed; return r }},
	}
	for _, format := range []string{"", wf.ReplayFormatGraphV1} {
		for _, test := range tests {
			t.Run("format="+format+"/"+test.name, func(t *testing.T) {
				b := replayBundle{Format: format, Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: digest, Journal: []journal.Record{{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started, Payload: json.RawMessage(`{"input_sha256":"` + digest + `"}`)}, Sequence: 1}, {Entry: journal.Entry{Index: 1, Epoch: 1, Kind: journal.Completed, Payload: json.RawMessage(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 2}}}
				b.Journal = test.change(b.Journal)
				_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
				if !errors.Is(err, wf.ErrCorruptJournal) {
					t.Fatalf("corrupt history reached plugin: %v", err)
				}
			})
		}
	}
}
