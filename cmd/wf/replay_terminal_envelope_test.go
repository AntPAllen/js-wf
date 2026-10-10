package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"js-wf/journal"
	"js-wf/wf"
	"testing"
)

func TestReplayTerminalEnvelopeBeforePlugin(t *testing.T) {
	cases := []struct{ name, payload string }{
		{"duplicate_invocation", `{"inv_seq":9,"inv_seq":1,"result":"NDI="}`},
		{"alias_invocation", `{"Inv_seq":9,"inv_seq":1,"result":"NDI="}`},
		{"escaped_invocation", `{"\u0069nv_seq":9,"inv_seq":1,"result":"NDI="}`},
		{"duplicate_result", `{"inv_seq":1,"result":"Nw==","result":"NDI="}`},
		{"alias_result", `{"inv_seq":1,"Result":"Nw==","result":"NDI="}`},
		{"duplicate_reference", `{"inv_seq":1,"result_ref":"foreign","result_ref":"","result":"NDI="}`},
		{"alias_reference", `{"inv_seq":1,"Result_ref":"foreign","result_ref":"","result":"NDI="}`},
		{"duplicate_hash", `{"inv_seq":1,"result_hash":"foreign","result_hash":"","result":"NDI="}`},
		{"alias_hash", `{"inv_seq":1,"Result_hash":"foreign","result_hash":"","result":"NDI="}`},
		{"duplicate_error", `{"inv_seq":1,"error":"failure","error":"","result":"NDI="}`},
		{"alias_error", `{"inv_seq":1,"Error":"failure","error":"","result":"NDI="}`},
		{"duplicate_limit_request", `{"inv_seq":1,"limit_request":{},"limit_request":null,"result":"NDI="}`},
		{"alias_limit_request", `{"inv_seq":1,"Limit_request":{},"limit_request":null,"result":"NDI="}`},
		{"duplicate_limit_entry", `{"inv_seq":1,"limit_entry":{},"limit_entry":null,"result":"NDI="}`},
		{"alias_limit_entry", `{"inv_seq":1,"Limit_entry":{},"limit_entry":null,"result":"NDI="}`},
		{"unknown_field", `{"inv_seq":1,"foreign":true,"result":"NDI="}`},
	}

	sum := sha256.Sum256([]byte(`7`))
	hash := hex.EncodeToString(sum[:])
	for _, format := range []string{"", wf.ReplayFormatGraphV1} {
		t.Run("format="+format, func(t *testing.T) {
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					b := replayBundle{Format: format, Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: hash, Journal: []journal.Record{
						{Entry: journal.Entry{Kind: journal.Started, Payload: json.RawMessage(`{"input_sha256":"` + hash + `"}`)}, Sequence: 1},
						{Entry: journal.Entry{Kind: journal.Completed, Index: 1, Payload: []byte(c.payload)}, Sequence: 2},
					}}
					_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
					if !errors.Is(err, wf.ErrCorruptJournal) {
						t.Fatalf("terminal envelope reached plugin: %v", err)
					}
				})
			}
		})
	}
}
