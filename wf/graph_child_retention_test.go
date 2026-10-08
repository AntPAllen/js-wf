package wf_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
)

func TestGraphChildRetentionRequiresExactParentOwnership(t *testing.T) {
	for _, externalEnvelope := range []bool{false, true} {
		for _, mode := range []string{"valid", "ordinary", "before_request", "duplicate_request", "duplicate_signal", "foreign_type", "foreign_id", "foreign_generation", "foreign_ref", "foreign_hash", "forged_envelope", "missing_result_edge", "missing_envelope_edge", "bad_inline_hash", "failed", "cancelled"} {
			t.Run(mode+"/external-envelope="+map[bool]string{false: "false", true: "true"}[externalEnvelope], func(t *testing.T) {
				ctx := context.Background()
				m := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
				graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol()})
				if err != nil {
					t.Fatal(err)
				}
				hash := func(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
				result := []byte(`"retained external child bytes"`)
				ref := "child-result-" + hash(result)
				outcome := wf.Outcome{InvSeq: 7, ResultRef: ref, ResultHash: hash(result)}
				kind := journal.Completed
				if mode == "failed" || mode == "cancelled" {
					kind = journal.Failed
					outcome = wf.Outcome{InvSeq: 7, Error: mode}
				}
				body, _ := json.Marshal(outcome)
				tail, err := graph.Begin(ctx, "child", "one", 7)
				if err != nil {
					t.Fatal(err)
				}
				tail, err = graph.Append(ctx, "child", "one", 7, journal.Entry{Kind: journal.Started}, tail, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				var childPayloads [][]byte
				if kind == journal.Completed {
					childPayloads = [][]byte{result}
				}
				_, err = graph.Append(ctx, "child", "one", 7, journal.Entry{Kind: kind, Index: 1, Payload: body}, tail, childPayloads, nil)
				if err != nil {
					t.Fatal(err)
				}
				child, err := graph.OpenTerminal(ctx, "child", "one", 7)
				if err != nil {
					t.Fatal(err)
				}
				defer child.Close(ctx)
				terminal, err := wf.ReadGraphTerminal(ctx, child, 7, 1000)
				if err != nil {
					t.Fatal(err)
				}
				tail, err = graph.Begin(ctx, "parent", "one", 8)
				if err != nil {
					t.Fatal(err)
				}
				index := uint64(0)
				appendEntry := func(kind journal.Kind, body []byte, payloads [][]byte) {
					var e error
					tail, e = graph.Append(ctx, "parent", "one", 8, journal.Entry{Kind: kind, Index: index, Payload: body}, tail, payloads, nil)
					if e != nil {
						t.Fatal(e)
					}
					index++
				}
				appendEntry(journal.Started, nil, nil)
				request := []byte(`{"kind":"call_async","name":"child_0","child_type":"child","child_id":"one"}`)
				if mode != "before_request" {
					appendEntry(journal.StepRequested, request, nil)
				}
				if mode == "duplicate_request" {
					appendEntry(journal.StepRequested, request, nil)
				}
				declaration := map[string]any{"type": "child", "id": "one", "inv_seq": uint64(7), "result_ref": outcome.ResultRef, "result_hash": outcome.ResultHash}
				switch mode {
				case "foreign_type":
					declaration["type"] = "foreign"
				case "foreign_id":
					declaration["id"] = "foreign"
				case "foreign_generation":
					declaration["inv_seq"] = uint64(9)
				case "foreign_ref":
					declaration["result_ref"] = "foreign"
				case "foreign_hash":
					declaration["result_hash"] = strings.Repeat("f", 64)
				}
				payload := body
				if mode == "forged_envelope" {
					payload = []byte(`{"inv_seq":7,"result":"NDM="}`)
				}
				event := map[string]any{"sig_seq": uint64(1), "name": "child_0", "payload": payload, "hash": hash(payload), "graph_child": declaration}
				if mode == "ordinary" {
					delete(event, "graph_child")
				}
				var payloads [][]byte
				if kind == journal.Completed && mode != "missing_result_edge" {
					payloads = append(payloads, result)
				}
				if externalEnvelope {
					delete(event, "payload")
					event["ref"] = "signal-" + hash(payload)
					if mode != "missing_envelope_edge" {
						payloads = append(payloads, payload)
					}
				}
				if mode == "bad_inline_hash" {
					event["hash"] = strings.Repeat("f", 64)
				}
				eventBytes, _ := json.Marshal(event)
				appendEntry(journal.SignalConsumed, eventBytes, payloads)
				if mode == "duplicate_signal" {
					appendEntry(journal.SignalConsumed, eventBytes, payloads)
				}
				if mode == "before_request" {
					appendEntry(journal.StepRequested, request, nil)
				}
				parent, err := graph.Open(ctx, "parent", "one", 8)
				if err != nil {
					t.Fatal(err)
				}
				defer parent.Close(ctx)
				owned, err := wf.GraphOwnsChildResult(ctx, parent, "child", "one", 7, "child_0", terminal, 1000)
				valid := mode == "valid" || mode == "failed" || mode == "cancelled" || mode == "missing_envelope_edge" && !externalEnvelope
				if valid {
					if err != nil || !owned {
						t.Fatal("valid owned transfer rejected", owned, err)
					}
				} else if mode == "ordinary" {
					if owned || err != nil {
						t.Fatal("opaque ordinary signal became retirement permission", owned, err)
					}
				} else if owned || !errors.Is(err, wf.ErrCorruptJournal) {
					t.Fatal("invalid transfer authorized retirement", owned, err)
				}
			})
		}
	}
}
