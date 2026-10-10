package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"js-wf/journal"
	"js-wf/wf"
)

func TestReplaySignalEnvelopeBeforePlugin(t *testing.T) {
	sum := sha256.Sum256([]byte(`7`))
	cases := []struct{ name, payload string }{
		{"sequence", `{"sig_seq":9,"sig_seq":1,"name":"gate"}`},
		{"name", `{"sig_seq":1,"Name":"foreign","name":"gate"}`},
		{"payload", `{"sig_seq":1,"name":"gate","payload":"Nw==","payload":null}`},
		{"reference", `{"sig_seq":1,"name":"gate","Ref":"foreign","ref":""}`},
		{"hash", `{"sig_seq":1,"name":"gate","hash":"foreign","hash":""}`},
		{"child", `{"sig_seq":1,"name":"gate","graph_child":{"inv_seq":9,"inv_seq":1}}`},
		{"canonical", `{"sig_seq":1,"name":"gate","canonical_signal":{"Token":"foreign","token":"owned"}}`},
		{"unknown", `{"sig_seq":1,"name":"gate","foreign":true}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := replayBundle{Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`7`), InputHash: hex.EncodeToString(sum[:]), Journal: []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Kind: journal.SignalConsumed, Index: 1, Payload: json.RawMessage(c.payload)}, Sequence: 2},
				{Entry: journal.Entry{Kind: journal.Completed, Index: 2, Payload: json.RawMessage(`{"inv_seq":1,"result":"NDI="}`)}, Sequence: 3},
			}}
			_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
			if !errors.Is(err, wf.ErrCorruptJournal) {
				t.Fatalf("legacy signal envelope reached plugin: %v", err)
			}
		})
	}
}
