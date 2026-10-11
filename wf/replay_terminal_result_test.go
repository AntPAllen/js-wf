package wf

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"js-wf/journal"
)

// Admission must finish before the initial handler, even when that handler
// ignores the recorded terminal value. User result bytes remain opaque here.
func TestReplayTerminalResultBeforeHandler(t *testing.T) {
	sum := sha256.Sum256([]byte(`42`))
	hash := hex.EncodeToString(sum[:])
	cases := []struct {
		name    string
		kind    journal.Kind
		out     Outcome
		objects map[string][]byte
		want    error
	}{
		{"inline", journal.Completed, Outcome{Result: []byte(`42`)}, nil, nil},
		{"user_null", journal.Completed, Outcome{Result: []byte(`null`)}, nil, nil},
		{"opaque_user_duplicates", journal.Completed, Outcome{Result: []byte(`{"x":1,"x":2}`)}, nil, nil},
		{"empty_success", journal.Completed, Outcome{}, nil, nil},
		{"external", journal.Completed, Outcome{ResultRef: "terminal", ResultHash: hash}, map[string][]byte{"terminal": []byte(`42`)}, nil},
		{"failed", journal.Failed, Outcome{Error: "failed"}, nil, nil},
		{"completed_error", journal.Completed, Outcome{Error: "failed"}, nil, ErrCorruptJournal},
		{"completed_limit_request", journal.Completed, Outcome{LimitRequest: json.RawMessage(`{"kind":"run","name":"x"}`)}, nil, ErrCorruptJournal},
		{"completed_limit_entry", journal.Completed, Outcome{LimitEntry: &LimitEntry{Kind: "Attempt", Payload: json.RawMessage(`{"count":1,"error":"failed"}`)}}, nil, ErrCorruptJournal},
		{"failed_empty", journal.Failed, Outcome{}, nil, ErrCorruptJournal},
		{"failed_inline", journal.Failed, Outcome{Error: "failed", Result: []byte(`42`)}, nil, ErrCorruptJournal},
		{"failed_external", journal.Failed, Outcome{Error: "failed", ResultRef: "terminal", ResultHash: hash}, map[string][]byte{"terminal": []byte(`42`)}, ErrCorruptJournal},
		{"inline_external", journal.Completed, Outcome{Result: []byte(`42`), ResultRef: "terminal", ResultHash: hash}, map[string][]byte{"terminal": []byte(`42`)}, ErrCorruptJournal},
		{"hash_without_ref", journal.Completed, Outcome{ResultHash: hash}, nil, ErrCorruptJournal},
		{"ref_without_hash", journal.Completed, Outcome{ResultRef: "terminal"}, nil, ErrCorruptJournal},
		{"hash_uppercase", journal.Completed, Outcome{ResultRef: "terminal", ResultHash: strings.ToUpper(hash)}, map[string][]byte{"terminal": []byte(`42`)}, ErrCorruptJournal},
		{"hash_short", journal.Completed, Outcome{ResultRef: "terminal", ResultHash: "ab"}, map[string][]byte{"terminal": []byte(`42`)}, ErrCorruptJournal},
		{"object_missing", journal.Completed, Outcome{ResultRef: "terminal", ResultHash: hash}, nil, ErrReplayObjectMissing},
		{"object_wrong", journal.Completed, Outcome{ResultRef: "terminal", ResultHash: hash}, map[string][]byte{"terminal": []byte(`43`)}, ErrCorruptJournal},
	}
	for _, identity := range []string{"legacy", "identified", "graph"} {
		t.Run(identity, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					opts := ReplayOptions{Objects: tc.objects}
					out := tc.out
					start := json.RawMessage(nil)
					if identity != "legacy" {
						opts.Type, opts.ID, opts.InvSeq = "parent", "id", 1
						out.InvSeq = 1
					}
					if identity == "graph" {
						input := sha256.Sum256([]byte(`null`))
						opts.Format, opts.InputHash = ReplayFormatGraphV1, hex.EncodeToString(input[:])
						start = json.RawMessage(`{"input_sha256":"` + opts.InputHash + `"}`)
					}
					payload, err := json.Marshal(out)
					if err != nil {
						t.Fatal(err)
					}
					records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started, Payload: start}, Sequence: 1}, {Entry: journal.Entry{Kind: tc.kind, Index: 1, Payload: payload}, Sequence: 2}}
					// Both the exported-history guard and SDK admission are required.
					err = ValidateReplayGraphHistory(records, opts.Objects, opts.Type, opts.ID, opts.InvSeq, opts.Format, opts.InputHash)
					if !errors.Is(err, tc.want) {
						t.Errorf("export admission=%v want=%v", err, tc.want)
					}
					raw, err := json.Marshal(records)
					if err != nil {
						t.Fatal(err)
					}
					calls := 0
					result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, opts)
					if !errors.Is(err, tc.want) || tc.want != nil && calls != 0 || tc.want == nil && (calls != 1 || result != 42) {
						t.Fatalf("SDK admission=%v want=%v handler calls=%d result=%d", err, tc.want, calls, result)
					}
				})
			}
		})
	}
}
