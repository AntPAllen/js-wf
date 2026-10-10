package wf

import (
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestReplayTerminalEnvelopeBeforeHandler(t *testing.T) {
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
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ordinary Outcome
			if err := json.Unmarshal([]byte(c.payload), &ordinary); err != nil || ordinary.InvSeq != 1 || string(ordinary.Result) != "42" || ordinary.Error != "" || ordinary.ResultRef != "" || ordinary.ResultHash != "" || ordinary.LimitRequest != nil && string(ordinary.LimitRequest) != "null" || ordinary.LimitEntry != nil {
				t.Fatalf("ordinary decoder no longer demonstrates final-value acceptance: %+v %v", ordinary, err)
			}
			records := []journal.Record{{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1}, {Entry: journal.Entry{Kind: journal.Completed, Index: 1, Payload: []byte(c.payload)}, Sequence: 2}}
			raw, _ := json.Marshal(records)
			for _, opts := range []ReplayOptions{{}, {Type: "parent", ID: "id", InvSeq: 1}} {
				calls := 0
				_, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, opts)
				if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
					t.Fatalf("terminal reached handler: calls=%d err=%v", calls, err)
				}
			}
			if err := ValidateReplayGraphHistory(records, nil, "parent", "id", 1, ""); !errors.Is(err, ErrCorruptJournal) {
				t.Fatalf("ambiguous terminal admitted: %v", err)
			}
		})
	}
}

func TestReplayTerminalEnvelopeTypedBoundary(t *testing.T) {
	cases := []struct {
		name, payload string
		reject        bool
	}{
		{"valid", `{"inv_seq":1,"result":"NDI="}`, false},
		{"legacy_identity_absent", `{"result":"NDI="}`, false},
		{"opaque_result", `{"inv_seq":1,"result":"eyJ1c2VyIjoxLCJ1c2VyIjoyfQ=="}`, false},
		{"opaque_limit_request", `{"inv_seq":1,"error":"workflow_limit","limit_request":{"user":1,"user":2}}`, false},
		{"opaque_limit_payload", `{"inv_seq":1,"error":"workflow_limit","limit_entry":{"kind":"Suspended","payload":{"user":1,"user":2}}}`, false},
		{"duplicate_nested_kind", `{"limit_entry":{"kind":"Failed","kind":"Suspended","payload":{}}}`, true},
		{"alias_nested_kind", `{"limit_entry":{"Kind":"Failed","kind":"Suspended","payload":{}}}`, true},
		{"duplicate_nested_payload", `{"limit_entry":{"kind":"Suspended","payload":null,"payload":{}}}`, true},
		{"alias_nested_payload", `{"limit_entry":{"kind":"Suspended","Payload":null,"payload":{}}}`, true},
		{"unknown_nested_field", `{"limit_entry":{"kind":"Suspended","foreign":true,"payload":{}}}`, true},
		{"trailing_value", `{} {}`, true},
		{"null", `null`, true},
		{"empty", ``, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out Outcome
			err := decodeReplayTerminal([]byte(c.payload), &out)
			if (err != nil) != c.reject {
				t.Fatalf("decode=%v reject=%t", err, c.reject)
			}
		})
	}
}
