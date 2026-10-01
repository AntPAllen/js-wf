package worker

import (
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
	"js-wf/wf"
)

func TestContinuationAnchorUsesHistoricalRuntimeFacts(t *testing.T) {
	records := []journal.Record{
		{Entry: journal.Entry{Index: 20, Epoch: 51, Kind: journal.StepCompleted}},
		{Entry: journal.Entry{Index: 21, Epoch: 51, Kind: journal.Attempt, Payload: json.RawMessage(`{"count":3,"error":"panic"}`)}},
		{Entry: journal.Entry{Index: 22, Epoch: 51, Kind: journal.SignalConsumed, Payload: json.RawMessage(`{"sig_seq":17,"name":"input"}`)}},
		{Entry: journal.Entry{Index: 23, Epoch: 51, Kind: journal.StepRequested}},
		{Entry: journal.Entry{Index: 24, Epoch: 51, Kind: journal.StepCompleted}},
		{Entry: journal.Entry{Index: 25, Epoch: 99, Kind: journal.Attempt, Payload: json.RawMessage(`{"count":4,"error":"later"}`)}},
		{Entry: journal.Entry{Index: 26, Epoch: 99, Kind: journal.SignalConsumed, Payload: json.RawMessage(`{"sig_seq":19,"name":"input"}`)}},
	}
	info := wf.CheckpointInfo{PanicAttempts: 2, SignalCursor: 13}
	anchor, err := continuationAnchor(records, 27, 99, info, 24, true)
	if err != nil || anchor != (wf.ContinuationAnchor{Index: 24, Epoch: 51, PanicAttempts: 3, SignalCursor: 17}) {
		t.Fatalf("historical anchor=%+v err=%v", anchor, err)
	}
	for _, recorded := range []bool{false, true} {
		anchor, err := continuationAnchor(records, 27, 99, info, 0, recorded)
		want := uint64(28)
		if recorded {
			want--
		}
		if err != nil || anchor.Index != want || anchor.Epoch != 99 || anchor.PanicAttempts != 4 || anchor.SignalCursor != 19 {
			t.Fatalf("prospective=%+v err=%v", anchor, err)
		}
	}
	if _, err := continuationAnchor(records, 27, 99, info, 23, true); !errors.Is(err, wf.ErrCorruptJournal) {
		t.Fatal(err)
	}
}
