package wf

import (
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestReplayRecordOrderBeforeHandler(t *testing.T) {
	tests := []struct {
		name   string
		change func([]journal.Record) []journal.Record
		reject bool
	}{
		{"valid", func(r []journal.Record) []journal.Record { return r }, false},
		{"physical_sequence_gap", func(r []journal.Record) []journal.Record { r[1].Sequence = 99; return r }, false},
		{"logical_index_gap", func(r []journal.Record) []journal.Record { r[1].Index = 2; return r }, true},
		{"first_index", func(r []journal.Record) []journal.Record { r[0].Index = 1; return r }, true},
		{"zero_sequence", func(r []journal.Record) []journal.Record { r[0].Sequence = 0; return r }, true},
		{"repeated_sequence", func(r []journal.Record) []journal.Record { r[1].Sequence = 1; return r }, true},
		{"backwards_sequence", func(r []journal.Record) []journal.Record { r[0].Sequence = 5; r[1].Sequence = 4; return r }, true},
		{"backwards_epoch", func(r []journal.Record) []journal.Record { r[0].Epoch = 2; return r }, true},
		{"missing_started", func(r []journal.Record) []journal.Record { r[0].Kind = journal.Suspended; return r }, true},
		{"repeated_started", func(r []journal.Record) []journal.Record { r[1].Kind = journal.Started; return r }, true},
		{"after_terminal", func(r []journal.Record) []journal.Record {
			return append(r, journal.Record{Entry: journal.Entry{Index: 2, Epoch: 1, Kind: journal.Suspended}, Sequence: 3})
		}, true},
		{"unknown_kind", func(r []journal.Record) []journal.Record { r[1].Kind = "foreign"; return r }, true},
		{"foreign_terminal", func(r []journal.Record) []journal.Record {
			r[1].Payload = []byte(`{"inv_seq":2,"result":"NDI="}`)
			return r
		}, true},
		{"empty_failure", func(r []journal.Record) []journal.Record { r[1].Kind = journal.Failed; return r }, true},
		{"empty_history", func([]journal.Record) []journal.Record { return nil }, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := test.change([]journal.Record{{Entry: journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, Sequence: 1}, {Entry: journal.Entry{Index: 1, Epoch: 1, Kind: journal.Completed, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 2}})
			err := ValidateReplayGraphHistory(records, nil, "parent", "id", 1, "")
			if (err != nil) != test.reject || test.reject && !errors.Is(err, ErrCorruptJournal) {
				t.Fatalf("pre-handler=%v reject=%t", err, test.reject)
			}
			raw, _ := json.Marshal(records)
			calls := 0
			result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1})
			if test.reject {
				if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
					t.Fatalf("corrupt history reached handler: calls=%d err=%v", calls, err)
				}
			} else if err != nil || result != 42 || calls != 1 {
				t.Fatalf("valid history result=%d calls=%d err=%v", result, calls, err)
			}
		})
	}
}
