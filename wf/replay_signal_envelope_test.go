package wf

import (
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
)

func TestReplaySignalEnvelopeBeforeHandler(t *testing.T) {
	cases := []struct{ name, payload string }{
		{"duplicate_sequence", `{"sig_seq":9,"sig_seq":1,"name":"gate"}`},
		{"alias_sequence", `{"Sig_seq":9,"sig_seq":1,"name":"gate"}`},
		{"escaped_sequence", `{"\u0073ig_seq":9,"sig_seq":1,"name":"gate"}`},
		{"duplicate_name", `{"sig_seq":1,"name":"foreign","name":"gate"}`},
		{"alias_name", `{"sig_seq":1,"Name":"foreign","name":"gate"}`},
		{"duplicate_payload", `{"sig_seq":1,"name":"gate","payload":"Nw==","payload":null}`},
		{"alias_payload", `{"sig_seq":1,"name":"gate","Payload":"Nw==","payload":null}`},
		{"duplicate_reference", `{"sig_seq":1,"name":"gate","ref":"foreign","ref":""}`},
		{"alias_reference", `{"sig_seq":1,"name":"gate","Ref":"foreign","ref":""}`},
		{"duplicate_hash", `{"sig_seq":1,"name":"gate","hash":"foreign","hash":""}`},
		{"alias_hash", `{"sig_seq":1,"name":"gate","Hash":"foreign","hash":""}`},
		{"duplicate_child", `{"sig_seq":1,"name":"gate","graph_child":{},"graph_child":null}`},
		{"alias_child", `{"sig_seq":1,"name":"gate","Graph_child":{},"graph_child":null}`},
		{"duplicate_canonical", `{"sig_seq":1,"name":"gate","canonical_signal":{},"canonical_signal":null}`},
		{"alias_canonical", `{"sig_seq":1,"name":"gate","Canonical_signal":{},"canonical_signal":null}`},
		{"nested_child_invocation", `{"sig_seq":1,"name":"gate","graph_child":{"inv_seq":9,"inv_seq":1}}`},
		{"nested_child_alias", `{"sig_seq":1,"name":"gate","graph_child":{"Type":"foreign","type":"child"}}`},
		{"nested_canonical_index", `{"sig_seq":1,"name":"gate","canonical_signal":{"index":9,"index":0}}`},
		{"nested_canonical_alias", `{"sig_seq":1,"name":"gate","canonical_signal":{"Token":"foreign","token":"owned"}}`},
		{"unknown_field", `{"sig_seq":1,"name":"gate","foreign":true}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ordinary replayCheckpointSignal
			if err := json.Unmarshal([]byte(c.payload), &ordinary); err != nil || ordinary.Sequence != 1 || ordinary.Name != "gate" || len(ordinary.Payload) != 0 || ordinary.Ref != "" || ordinary.Hash != "" {
				t.Fatalf("ordinary decoder does not demonstrate final-value acceptance: %+v %v", ordinary, err)
			}
			records := replaySignalEnvelopeRecords(c.payload)
			raw, _ := json.Marshal(records)
			for _, opts := range []ReplayOptions{{}, {Type: "parent", ID: "id", InvSeq: 1}} {
				calls := 0
				_, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, opts)
				if !errors.Is(err, ErrCorruptJournal) || calls != 0 {
					t.Fatalf("signal envelope reached handler: calls=%d err=%v", calls, err)
				}
			}
			if err := ValidateReplayGraphHistory(records, nil, "parent", "id", 1, ""); !errors.Is(err, ErrCorruptJournal) {
				t.Fatalf("ambiguous signal admitted: %v", err)
			}
		})
	}
}

func replaySignalEnvelopeRecords(payload string) []journal.Record {
	return []journal.Record{
		{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1},
		{Entry: journal.Entry{Kind: journal.SignalConsumed, Index: 1, Payload: []byte(payload)}, Sequence: 2},
		{Entry: journal.Entry{Kind: journal.Completed, Index: 2, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 3},
	}
}

func TestReplaySignalEnvelopeOpaquePayload(t *testing.T) {
	user := []byte(`{"user":1,"user":2,"Name":"opaque"}`)
	encoded, _ := json.Marshal(user)
	payload := `{"sig_seq":1,"name":"gate","payload":` + string(encoded) + `}`
	raw, _ := json.Marshal(replaySignalEnvelopeRecords(payload))
	calls := 0
	result, err := Replay(raw, func(*Context) (int, error) { calls++; return 42, nil }, ReplayOptions{Type: "parent", ID: "id", InvSeq: 1})
	if err != nil || result != 42 || calls != 1 {
		t.Fatalf("opaque payload changed replay: result=%d calls=%d err=%v", result, calls, err)
	}
}

func TestReplaySignalEnvelopeMissing(t *testing.T) {
	for _, payload := range []string{"", "null", "  null  "} {
		t.Run("payload="+payload, func(t *testing.T) {
			records := replaySignalEnvelopeRecords(payload)
			if err := ValidateReplayGraphHistory(records, nil, "parent", "id", 1, ""); !errors.Is(err, ErrCorruptJournal) {
				t.Fatalf("missing signal envelope admitted: %v", err)
			}
		})
	}
}
