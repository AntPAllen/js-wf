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

func TestReplayGraphChildProvenance(t *testing.T) {
	hash := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
	input, result := []byte(`7`), []byte(`42`)
	resultHash := hash(result)
	outcome, _ := json.Marshal(wf.Outcome{InvSeq: 2, ResultRef: "child-result", ResultHash: resultHash})
	event := map[string]any{
		"name": "child_2", "sig_seq": 1, "ref": "child-outcome", "hash": hash(outcome),
		"graph_child":      map[string]any{"type": "child", "id": "child-id", "inv_seq": 2, "result_ref": "child-result", "result_hash": resultHash},
		"canonical_signal": map[string]any{"index": 0, "token": "owned"},
	}
	makeBundle := func() replayBundle {
		body, _ := json.Marshal(event)
		return replayBundle{Type: "parent", ID: "id", InvSeq: 1, Input: input, InputHash: hash(input), Journal: []journal.Record{
			{Entry: journal.Entry{Kind: journal.StepRequested, Payload: []byte(`{"kind":"call_async","name":"child_2","child_type":"child","child_id":"child-id"}`)}},
			{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: body}},
			{Entry: journal.Entry{Kind: journal.Completed, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)}},
		}, Objects: map[string][]byte{"child-outcome": outcome, "child-result": result}}
	}
	mutateEvent := func(b *replayBundle, change func(map[string]any)) {
		var value map[string]any
		if err := json.Unmarshal(b.Journal[1].Payload, &value); err != nil {
			t.Fatal(err)
		}
		change(value)
		b.Journal[1].Payload, _ = json.Marshal(value)
	}
	tests := []struct {
		name   string
		change func(*replayBundle)
		want   error
	}{
		{"valid", func(*replayBundle) {}, nil},
		{"wrong_type", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { e["graph_child"].(map[string]any)["type"] = "foreign" })
		}, wf.ErrCorruptJournal},
		{"wrong_id", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { e["graph_child"].(map[string]any)["id"] = "foreign" })
		}, wf.ErrCorruptJournal},
		{"wrong_invocation", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { e["graph_child"].(map[string]any)["inv_seq"] = 99 })
		}, wf.ErrCorruptJournal},
		{"zero_invocation", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { e["graph_child"].(map[string]any)["inv_seq"] = 0 })
		}, wf.ErrCorruptJournal},
		{"wrong_reference", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { e["graph_child"].(map[string]any)["result_ref"] = "foreign" })
		}, wf.ErrCorruptJournal},
		{"wrong_hash", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { e["graph_child"].(map[string]any)["result_hash"] = hash([]byte(`43`)) })
		}, wf.ErrCorruptJournal},
		{"missing_binding", func(b *replayBundle) { mutateEvent(b, func(e map[string]any) { delete(e, "graph_child") }) }, wf.ErrCorruptJournal},
		{"missing_outcome", func(b *replayBundle) { delete(b.Objects, "child-outcome") }, wf.ErrReplayObjectMissing},
		{"missing_result", func(b *replayBundle) { delete(b.Objects, "child-result") }, wf.ErrReplayObjectMissing},
		{"changed_outcome_bytes", func(b *replayBundle) { b.Objects["child-outcome"] = []byte(`{"inv_seq":99}`) }, wf.ErrCorruptJournal},
		{"changed_result_bytes", func(b *replayBundle) { b.Objects["child-result"] = []byte(`43`) }, wf.ErrCorruptJournal},
		{"annotation_before_request", func(b *replayBundle) { b.Journal[0], b.Journal[1] = b.Journal[1], b.Journal[0] }, wf.ErrCorruptJournal},
		{"undeclared_name", func(b *replayBundle) { mutateEvent(b, func(e map[string]any) { e["name"] = "other" }) }, wf.ErrCorruptJournal},
		{"legacy_signal", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { delete(e, "graph_child"); delete(e, "canonical_signal") })
		}, nil},
		{"ordinary_before_child_request", func(b *replayBundle) {
			mutateEvent(b, func(e map[string]any) { delete(e, "graph_child") })
			b.Journal[0], b.Journal[1] = b.Journal[1], b.Journal[0]
		}, nil},
		{"failed_child", func(b *replayBundle) {
			data, _ := json.Marshal(wf.Outcome{InvSeq: 2, Error: "planned failure"})
			b.Objects["child-outcome"] = data
			mutateEvent(b, func(e map[string]any) {
				e["hash"] = hash(data)
				c := e["graph_child"].(map[string]any)
				delete(c, "result_ref")
				delete(c, "result_hash")
			})
		}, nil},
		{"failed_with_result", func(b *replayBundle) {
			data, _ := json.Marshal(wf.Outcome{InvSeq: 2, Error: "planned failure", ResultRef: "child-result", ResultHash: resultHash})
			b.Objects["child-outcome"] = data
			mutateEvent(b, func(e map[string]any) { e["hash"] = hash(data) })
		}, wf.ErrCorruptJournal},
		{"inline_result", func(b *replayBundle) {
			data, _ := json.Marshal(wf.Outcome{InvSeq: 2, Result: result})
			b.Objects["child-outcome"] = data
			mutateEvent(b, func(e map[string]any) {
				e["hash"] = hash(data)
				c := e["graph_child"].(map[string]any)
				delete(c, "result_ref")
				delete(c, "result_hash")
			})
		}, nil},
		{"limit_nested_signal", func(b *replayBundle) {
			data, _ := json.Marshal(wf.Outcome{InvSeq: 1, Error: journal.ErrTooLong.Error(), LimitEntry: &wf.LimitEntry{Kind: string(journal.SignalConsumed), Payload: b.Journal[1].Payload}})
			b.Journal = b.Journal[:2]
			b.Journal[1].Kind = journal.Failed
			b.Journal[1].Payload = data
		}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := makeBundle()
			test.change(&b)
			err := validateReplayGraphChildren(b)
			if !errors.Is(err, test.want) {
				t.Fatalf("annotation validation=%v want=%v", err, test.want)
			}
			if test.want != nil {
				// The actual CLI bundle path must reject before opening a handler plugin.
				_, err = runReplayBundle(b, "/does-not-exist.so", "Workflow")
				if !errors.Is(err, test.want) {
					t.Fatalf("CLI validation=%v want=%v", err, test.want)
				}
			}
		})
	}
}
