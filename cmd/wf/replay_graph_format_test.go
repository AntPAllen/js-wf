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

func TestReplayDeclaredGraphBeforePlugin(t *testing.T) {
	sum := sha256.Sum256([]byte(`null`))
	makeBundle := func() replayBundle {
		return replayBundle{Format: wf.ReplayFormatGraphV1, Type: "parent", ID: "id", InvSeq: 1, Input: []byte(`null`), InputHash: hex.EncodeToString(sum[:]), Journal: []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: json.RawMessage(`{"sig_seq":1,"name":"gate","payload":"dHJ1ZQ=="}`)}}, {Entry: journal.Entry{Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"signal:gate"}`)}}}}
	}
	tests := []struct {
		name   string
		change func(*replayBundle)
		want   error
	}{
		{"removed_annotation", func(*replayBundle) {}, wf.ErrCorruptJournal},
		{"unknown_format", func(b *replayBundle) { b.Format = "graph-v99" }, wf.ErrReplayFormatUnsupported},
		{"removed_checkpoint_metadata", func(b *replayBundle) {
			b.Journal = b.Journal[:1]
			b.Journal = append(b.Journal, journal.Record{Entry: journal.Entry{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"checkpoint"}`)}}, journal.Record{Entry: journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"frame"}`)}}, journal.Record{Entry: journal.Entry{Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"continuation:finish"}`)}})
		}, wf.ErrCorruptJournal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := makeBundle()
			test.change(&b)
			_, err := runReplayBundle(b, "/does-not-exist.so", "Workflow")
			if !errors.Is(err, test.want) {
				t.Fatalf("before plugin=%v want=%v", err, test.want)
			}
		})
	}
}
