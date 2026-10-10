package wf

import (
	"encoding/json"
	"errors"
	"testing"

	"js-wf/integrity"
	"js-wf/journal"
)

func TestReplayEpochWorkerOwnership(t *testing.T) {
	tests := []struct {
		name    string
		epochs  []uint64
		workers []string
		reject  bool
	}{
		{"same_worker", []uint64{1, 1, 1}, []string{"one", "one", "one"}, false},
		{"new_epoch", []uint64{1, 2, 2}, []string{"one", "two", "two"}, false},
		{"conflicting_epoch", []uint64{1, 1, 1}, []string{"one", "two", "two"}, true},
		{"conflict_after_missing_id", []uint64{1, 1, 1}, []string{"one", "", "two"}, true},
		{"legacy_missing_ids", []uint64{1, 1, 1}, []string{"", "", ""}, false},
		{"legacy_partial_ids", []uint64{1, 1, 1}, []string{"", "one", ""}, false},
		{"legacy_zero_epoch", []uint64{0, 0, 0}, []string{"one", "two", "three"}, false},
		{"zero_then_owned_epoch", []uint64{0, 1, 1}, []string{"start", "one", "one"}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: []byte(`{"sig_seq":1,"name":"signal","payload":"dHJ1ZQ=="}`)}}, {Entry: journal.Entry{Kind: journal.Completed, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}}}
			for i := range records {
				records[i].Index = uint64(i)
				records[i].Sequence = uint64(i + 1)
				records[i].Epoch = test.epochs[i]
				records[i].WorkerID = test.workers[i]
			}
			_, auditErr := integrity.CheckSnapshot(integrity.Snapshot{Invocations: []string{"wf.inv.parent.id"}, Journals: map[string][]journal.Record{"wf.jrn.parent.id": records}, TerminalState: map[string][]byte{"parent.id": records[len(records)-1].Payload}})
			if (auditErr != nil) != test.reject {
				t.Fatalf("independent integrity audit=%v reject=%t", auditErr, test.reject)
			}
			err := ValidateReplayGraphHistory(records, nil, "parent", "id", 1, "")
			if (err != nil) != test.reject || test.reject && !errors.Is(err, ErrCorruptJournal) {
				t.Fatalf("pre-handler=%v reject=%t", err, test.reject)
			}
			raw, _ := json.Marshal(records)
			calls := 0
			result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1})
			if test.reject {
				if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
					t.Fatalf("ownership conflict reached handler: calls=%d err=%v", calls, err)
				}
			} else if err != nil || result != 42 || calls != 1 {
				t.Fatalf("valid ownership result=%d calls=%d err=%v", result, calls, err)
			}
		})
	}
}
