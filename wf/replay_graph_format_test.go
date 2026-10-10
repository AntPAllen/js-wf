package wf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestReplayDeclaredGraphFormat(t *testing.T) {
	hash := func(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
	makeRecords := func() ([]journal.Record, map[string][]byte) {
		signal, _ := json.Marshal(map[string]any{"sig_seq": 1, "name": "gate", "ref": "signal", "hash": hash([]byte(`true`)), "canonical_signal": map[string]any{"index": 0, "token": "owned"}})
		terminal, _ := json.Marshal(Outcome{InvSeq: 1, Result: []byte(`42`)})
		records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: signal}}, {Entry: journal.Entry{Kind: journal.Completed, Payload: terminal}}}
		for i := range records {
			records[i].Index = uint64(i)
			records[i].Epoch = 1
			records[i].Sequence = uint64(i + 1)
		}
		return records, map[string][]byte{"signal": []byte(`true`)}
	}
	mutateSignal := func(r []journal.Record, fn func(map[string]any)) {
		var e map[string]any
		_ = json.Unmarshal(r[1].Payload, &e)
		fn(e)
		r[1].Payload, _ = json.Marshal(e)
	}
	tests := []struct {
		name, format string
		change       func([]journal.Record, map[string][]byte)
		want         error
	}{
		{"valid", ReplayFormatGraphV1, func([]journal.Record, map[string][]byte) {}, nil},
		{"unversioned_compatibility", "", func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { delete(e, "canonical_signal") })
		}, nil},
		{"unknown_version", "graph-v99", func([]journal.Record, map[string][]byte) {}, ErrReplayFormatUnsupported},
		{"missing_annotation", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { delete(e, "canonical_signal") })
		}, ErrCorruptJournal},
		{"null_annotation", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { e["canonical_signal"] = nil })
		}, ErrCorruptJournal},
		{"wrong_queue_index", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { e["canonical_signal"].(map[string]any)["index"] = 1 })
		}, ErrCorruptJournal},
		{"missing_token", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { delete(e["canonical_signal"].(map[string]any), "token") })
		}, ErrCorruptJournal},
		{"missing_object", ReplayFormatGraphV1, func(_ []journal.Record, o map[string][]byte) { delete(o, "signal") }, ErrReplayObjectMissing},
		{"changed_object", ReplayFormatGraphV1, func(_ []journal.Record, o map[string][]byte) { o["signal"] = []byte(`false`) }, ErrCorruptJournal},
		{"inline_signal", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { e["payload"] = []byte(`true`); delete(e, "ref"); delete(e, "hash") })
		}, ErrCorruptJournal},
		{"zero_sequence", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			mutateSignal(r, func(e map[string]any) { e["sig_seq"] = 0 })
		}, ErrCorruptJournal},
		{"alias_annotation", ReplayFormatGraphV1, func(r []journal.Record, _ map[string][]byte) {
			e := r[1].Payload
			r[1].Payload = append([]byte(`{"Canonical_Signal":{"index":0,"token":"owned"},`), e[1:]...)
		}, ErrCorruptJournal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r, o := makeRecords()
			test.change(r, o)
			err := ValidateReplayGraphHistory(r, o, "parent", "id", 1, test.format)
			if !errors.Is(err, test.want) {
				t.Fatalf("validation=%v want=%v", err, test.want)
			}
			raw, _ := json.Marshal(r)
			calls := 0
			result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1, Objects: o, Format: test.format})
			if !errors.Is(err, test.want) || test.want != nil && calls != 0 || test.want == nil && (calls != 1 || result != 42) {
				t.Fatalf("SDK result=%d calls=%d err=%v", result, calls, err)
			}
		})
	}
	t.Run("checkpoint_metadata_removed", func(t *testing.T) {
		r := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"checkpoint"}`)}}, {Entry: journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"frame"}`)}}}
		if err := ValidateReplayGraphHistory(r, nil, "parent", "id", 1, ReplayFormatGraphV1); !errors.Is(err, ErrCorruptJournal) {
			t.Fatal(err)
		}
		if err := validateReplayGraphFormat(r, nil, ""); err != nil {
			t.Fatal("legacy format contract changed", err)
		}
	})
	t.Run("limit_nested_annotation_removed", func(t *testing.T) {
		r, o := makeRecords()
		var e map[string]any
		_ = json.Unmarshal(r[1].Payload, &e)
		delete(e, "canonical_signal")
		raw, _ := json.Marshal(e)
		terminal, _ := json.Marshal(Outcome{InvSeq: 1, Error: journal.ErrTooLong.Error(), LimitEntry: &LimitEntry{Kind: string(journal.SignalConsumed), Payload: raw}})
		r = append(r[:1], journal.Record{Entry: journal.Entry{Kind: journal.Failed, Payload: terminal}})
		if err := ValidateReplayGraphHistory(r, o, "parent", "id", 1, ReplayFormatGraphV1); !errors.Is(err, ErrCorruptJournal) {
			t.Fatal(err)
		}
	})
}
