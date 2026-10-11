package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"js-wf/internal/blobpublication"
	"js-wf/internal/checkpoint"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

func promiseHistoryFixture(t *testing.T, explicit, external bool) (GraphReferenceSnapshot, *auditedGraphJournal, checkpoint.Frame, map[string][]byte) {
	t.Helper()
	child := auditedCheckpointChild{Type: "child", ID: "id", Invocation: 3}
	body := []byte(`{"inv_seq":3,"result":"NDI="}`)
	if external {
		child.Ref, child.Hash = "child-result", digest([]byte(`42`))
		body = []byte(`{"inv_seq":3,"result_ref":"child-result","result_hash":"` + child.Hash + `"}`)
	}
	objects := map[string][]byte{"signal-body": body}
	graph := GraphReferenceSnapshot{PayloadLimit: 1024, LoadObject: func(_ context.Context, name string, _ int) ([]byte, error) {
		data, ok := objects[name]
		if !ok {
			return nil, fmt.Errorf("missing object")
		}
		return data, nil
	}}
	state := &auditedGraphJournal{children: map[string]auditedCheckpointChild{"child_0": {Type: "child", ID: "id"}}}
	event := auditedCheckpointSignal{Sequence: 7, Name: "child_0", Ref: "graph-signal-" + digest(body), Hash: digest(body), Child: &child}
	edges := []retainedgraph.Link{{Hash: event.Hash, Reference: blobpublication.Reference{Object: "signal-body"}}}
	if external {
		edges = append(edges, retainedgraph.Link{Hash: child.Hash, Reference: blobpublication.Reference{Object: "child-result-body"}})
	}
	observeSDKPromiseSignal(state, event, edges)
	request := `{"kind":"signal","name":"child_0"}`
	completion := `{"signal_seq":7}`
	if explicit {
		request = `{"kind":"select_many","cases":[{"kind":"promise","name":"child_0"}]}`
		completion = `{"case_index":0,"signal_seq":7}`
	}
	state.journal.request = json.RawMessage(request)
	if err := observeSDKPromiseSelection(state, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(completion)}); err != nil {
		t.Fatal(err)
	}
	return graph, state, checkpoint.Frame{PromiseOutcomes: map[string]json.RawMessage{"child_0": body}}, objects
}

func TestRawGraphCheckpointPromiseHistory(t *testing.T) {
	for _, mode := range []string{"signal-inline", "signal-external", "explicit-inline", "explicit-external", "ambiguous-cache-omitted", "cached-select-reuse", "compacted-outcome", "multiple-ambiguous-candidates", "multiple-ambiguous-later"} {
		t.Run(mode, func(t *testing.T) {
			graph, state, frame, objects := promiseHistoryFixture(t, mode == "explicit-inline" || mode == "explicit-external", mode == "signal-external" || mode == "explicit-external")
			switch mode {
			case "ambiguous-cache-omitted":
				frame.PromiseOutcomes = nil
			case "cached-select-reuse":
				state.journal.request = json.RawMessage(`{"kind":"select_many","cases":[{"kind":"promise","name":"child_0"}]}`)
				if err := observeSDKPromiseSelection(state, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"case_index":0}`)}); err != nil {
					t.Fatal(err)
				}
			case "compacted-outcome":
				body := []byte("{ \"inv_seq\": 3, \"result\": \"NDI=\" }")
				objects["signal-body"] = body
				candidate := state.promiseCandidates["child_0"][0]
				candidate.event.Hash = digest(body)
				state.promiseCandidates["child_0"][0] = candidate
			case "multiple-ambiguous-candidates", "multiple-ambiguous-later":
				other := state.promiseCandidates["child_0"][0]
				other.event.Sequence = 8
				other.object = "other-body"
				child := *other.event.Child
				child.Invocation = 4
				other.event.Child = &child
				body := []byte(`{"inv_seq":4,"result":"NDM="}`)
				other.event.Hash = digest(body)
				objects[other.object] = body
				state.promiseCandidates["child_0"] = append(state.promiseCandidates["child_0"], other)
				if mode == "multiple-ambiguous-later" {
					frame.PromiseOutcomes["child_0"] = body
				}
			}
			if err := auditSDKCheckpointPromises(context.Background(), graph, state, frame); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, mode := range []string{"outcome-changed", "name-changed", "no-selection", "child-declaration", "child-generation", "source-unowned", "source-wrong-bytes", "source-missing", "external-unowned", "explicit-cache-omitted"} {
		t.Run(mode, func(t *testing.T) {
			graph, state, frame, objects := promiseHistoryFixture(t, mode == "explicit-cache-omitted", mode == "external-unowned")
			candidate := state.promiseCandidates["child_0"][0]
			switch mode {
			case "outcome-changed":
				frame.PromiseOutcomes["child_0"] = json.RawMessage(`{"inv_seq":3,"result":"NDM="}`)
			case "name-changed":
				frame.PromiseOutcomes = map[string]json.RawMessage{"foreign": frame.PromiseOutcomes["child_0"]}
			case "no-selection":
				state.promiseCandidates = nil
			case "child-declaration":
				state.children["child_0"] = auditedCheckpointChild{Type: "other", ID: "id"}
			case "child-generation":
				child := *candidate.event.Child
				child.Invocation++
				candidate.event.Child = &child
			case "source-unowned":
				candidate.object = ""
			case "source-wrong-bytes":
				objects["signal-body"] = []byte(`{}`)
			case "source-missing":
				delete(objects, "signal-body")
			case "external-unowned":
				candidate.childResultOwned = false
			case "explicit-cache-omitted":
				frame.PromiseOutcomes = nil
			}
			if state.promiseCandidates != nil {
				state.promiseCandidates["child_0"][0] = candidate
			}
			if err := auditSDKCheckpointPromises(context.Background(), graph, state, frame); err == nil {
				t.Fatal("corrupt promise provenance accepted")
			}
		})
	}
	for _, mode := range []string{"case-index-missing", "cached-without-history", "explicit-cache-reconsumed", "arrival-missing"} {
		t.Run(mode, func(t *testing.T) {
			_, state, _, _ := promiseHistoryFixture(t, true, false)
			payload := json.RawMessage(`{"case_index":0,"signal_seq":7}`)
			switch mode {
			case "case-index-missing":
				payload = json.RawMessage(`{"signal_seq":7}`)
			case "cached-without-history":
				state.promiseCandidates = nil
				state.requiredPromises = nil
				payload = json.RawMessage(`{"case_index":0}`)
			case "arrival-missing":
				state.sdkSignals = nil
				state.requiredPromises = nil
			}
			if err := observeSDKPromiseSelection(state, journal.Entry{Kind: journal.StepCompleted, Payload: payload}); err == nil {
				t.Fatal("invalid promise selection accepted")
			}
		})
	}
}
