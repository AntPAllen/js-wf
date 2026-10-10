package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplayBundleEnvelope(t *testing.T) {
	tests := []struct {
		name, raw string
		reject    bool
	}{
		{"valid", `{"format":"graph-v1","type":"parent","id":"id","inv_seq":1,"input":"Nw==","input_hash":"hash","journal":[],"objects":{"owned":"Nw=="}}`, false},
		{"legacy", `{"type":"parent","journal":[],"objects":{}}`, false},
		{"opaque_payload", `{"journal":[{"kind":"Started","payload":{"user":1,"user":2}}]}`, false},
		{"duplicate_format_downgrade", `{"format":"graph-v1","format":""}`, true},
		{"escaped_duplicate_format", `{"format":"graph-v1","\u0066ormat":""}`, true},
		{"alias_format", `{"format":"graph-v1","Format":""}`, true},
		{"duplicate_input", `{"input":"Nw==","input":"OA=="}`, true},
		{"duplicate_identity", `{"inv_seq":1,"inv_seq":2}`, true},
		{"duplicate_object", `{"objects":{"owned":"Nw==","owned":"OA=="}}`, true},
		{"alias_pending_sequence", `{"pending_signal":{"sequence":1,"Sequence":2}}`, true},
		{"duplicate_pending_header", `{"pending_signal":{"header":{"Wf-Token":["old"],"Wf-Token":["new"]}}}`, true},
		{"unknown_field", `{"foreign":true}`, true},
		{"unknown_journal_field", `{"journal":[{"foreign":true}]}`, true},
		{"trailing_value", `{} {}`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bundle.json")
			if err := os.WriteFile(path, []byte(test.raw), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := loadReplayBundle(path)
			if (err != nil) != test.reject {
				t.Fatalf("decode error=%v reject=%v", err, test.reject)
			}
		})
	}
}

// Opt in to a retained native producer export set, independent of live servers.
func TestReplayBundleRetainedEnvelope(t *testing.T) {
	root := os.Getenv("WF_REPLAY_ENVELOPE_FIXTURE_ROOT")
	if root == "" {
		t.Skip("set WF_REPLAY_ENVELOPE_FIXTURE_ROOT to the native export root")
	}
	paths, err := filepath.Glob(filepath.Join(root, "*", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, path := range paths {
		switch filepath.Base(path) {
		case "completed.json", "suspended.json", "child-pending.json", "missing-objects.json", "missing-child-result.json":
			count++
			t.Run(filepath.Base(filepath.Dir(path))+"/"+filepath.Base(path), func(t *testing.T) {
				if _, err := loadReplayBundle(path); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	if count != 76 {
		t.Fatalf("retained exports=%d want=76", count)
	}
}
