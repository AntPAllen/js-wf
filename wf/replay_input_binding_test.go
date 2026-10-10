package wf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestReplayInputBindsStarted(t *testing.T) {
	hash := func(raw string) string { sum := sha256.Sum256([]byte(raw)); return hex.EncodeToString(sum[:]) }
	original, foreign := hash(`{"value":7}`), hash(`{"value":7,"ignored":true}`)
	tests := []struct {
		name, format, expectedHash string
		start                      json.RawMessage
		want                       error
	}{
		{"graph_valid", ReplayFormatGraphV1, original, json.RawMessage(`{"input_sha256":"` + original + `"}`), nil},
		{"graph_rehashed_input", ReplayFormatGraphV1, foreign, json.RawMessage(`{"input_sha256":"` + original + `"}`), ErrReplayInputMismatch},
		{"graph_missing_expected", ReplayFormatGraphV1, "", json.RawMessage(`{"input_sha256":"` + original + `"}`), ErrReplayInputMismatch},
		{"graph_missing_start_hash", ReplayFormatGraphV1, original, json.RawMessage(`{}`), ErrReplayInputMismatch},
		{"graph_malformed_hash", ReplayFormatGraphV1, "bad", json.RawMessage(`{"input_sha256":"bad"}`), ErrReplayInputMismatch},
		{"graph_duplicate_key", ReplayFormatGraphV1, foreign, json.RawMessage(`{"input_sha256":"` + original + `","input_sha256":"` + foreign + `"}`), ErrCorruptJournal},
		{"graph_case_alias", ReplayFormatGraphV1, foreign, json.RawMessage(`{"input_sha256":"` + original + `","INPUT_SHA256":"` + foreign + `"}`), ErrCorruptJournal},
		{"graph_unknown_field", ReplayFormatGraphV1, original, json.RawMessage(`{"input_sha256":"` + original + `","foreign":true}`), ErrCorruptJournal},
		{"unversioned_annotated_valid", "", original, json.RawMessage(`{"input_sha256":"` + original + `"}`), nil},
		{"unversioned_annotated_changed", "", foreign, json.RawMessage(`{"input_sha256":"` + original + `"}`), ErrReplayInputMismatch},
		{"legacy_without_declaration", "", original, json.RawMessage(`null`), nil},
		{"legacy_without_expected", "", "", json.RawMessage(`null`), nil},
		{"legacy_empty_payload", "", original, nil, nil},
		{"legacy_opaque_payload", "", original, json.RawMessage(`7`), nil},
		{"unversioned_null_declaration", "", original, json.RawMessage(`{"input_sha256":null}`), ErrReplayInputMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			terminal, _ := json.Marshal(Outcome{InvSeq: 1, Result: []byte(`42`)})
			r := []journal.Record{{Entry: journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1, Payload: test.start}, Sequence: 1}, {Entry: journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 1, Payload: terminal}, Sequence: 2}}
			err := ValidateReplayGraphHistory(r, nil, "parent", "id", 1, test.format, test.expectedHash)
			if !errors.Is(err, test.want) {
				t.Fatalf("pre-handler=%v want=%v", err, test.want)
			}
			raw, _ := json.Marshal(r)
			calls := 0
			result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1, Format: test.format, InputHash: test.expectedHash})
			if !errors.Is(err, test.want) || test.want != nil && calls != 0 || test.want == nil && (result != 42 || calls != 1) {
				t.Fatalf("input changes ignored by handler: result=%d calls=%d err=%v", result, calls, err)
			}
		})
	}
}
