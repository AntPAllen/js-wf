package integrity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

type auditedPromiseSignal struct {
	event            auditedCheckpointSignal
	object           string
	childResultOwned bool
}

func observeSDKPromiseSignal(state *auditedGraphJournal, signal auditedCheckpointSignal, edges []retainedgraph.Link) {
	if state.sdkSignals == nil {
		state.sdkSignals = map[uint64]auditedPromiseSignal{}
	}
	s := auditedPromiseSignal{event: signal}
	for _, edge := range edges {
		if edge.Hash == signal.Hash {
			s.object = edge.Reference.Object
		}
		if signal.Child != nil && signal.Child.Hash != "" && edge.Hash == signal.Child.Hash {
			s.childResultOwned = true
		}
	}
	if state.sdkArrivalQueue == nil {
		state.sdkArrivalQueue = map[string][]uint64{}
	}
	state.sdkArrivalQueue[signal.Name] = append(state.sdkArrivalQueue[signal.Name], signal.Sequence)
	state.sdkSignals[signal.Sequence] = s
}

func observeSDKPromiseSelection(state *auditedGraphJournal, entry journal.Entry) error {
	if entry.Kind != journal.StepCompleted {
		return nil
	}
	var request struct {
		Kind  string `json:"kind"`
		Name  string `json:"name"`
		Cases []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"cases"`
	}
	if json.Unmarshal(state.journal.request, &request) != nil {
		return fmt.Errorf("invalid SDK promise request")
	}
	if request.Kind != "signal" && request.Kind != "select_many" {
		return nil
	}
	var done struct {
		Sequence  uint64 `json:"signal_seq"`
		CaseIndex *int   `json:"case_index"`
	}
	if json.Unmarshal(entry.Payload, &done) != nil {
		return fmt.Errorf("invalid SDK promise completion")
	}
	name, required := request.Name, false
	if request.Kind == "select_many" {
		if done.CaseIndex == nil || *done.CaseIndex < 0 || *done.CaseIndex >= len(request.Cases) {
			return fmt.Errorf("invalid SDK promise selected case")
		}
		selected := request.Cases[*done.CaseIndex]
		if selected.Kind != "promise" {
			return nil
		}
		name, required = selected.Name, true
		if done.Sequence == 0 {
			// A zero sequence reuses an already cached immutable outcome.
			if len(state.promiseCandidates[name]) == 0 {
				return fmt.Errorf("cached promise selection without prior outcome")
			}
			if state.requiredPromises == nil {
				state.requiredPromises = map[string][]auditedPromiseSignal{}
			}
			if len(state.requiredPromises[name]) == 0 {
				state.requiredPromises[name] = append([]auditedPromiseSignal(nil), state.promiseCandidates[name]...)
			}
			return nil
		}
	}
	signal, found := state.sdkSignals[done.Sequence]
	if !found || signal.event.Name != name {
		return fmt.Errorf("promise/signal selection lacks preceding arrival")
	}
	if signal.event.Child == nil {
		if required {
			return fmt.Errorf("promise selection lacks child provenance")
		}
		return nil // Ordinary AwaitSignal does not materialize a promise.
	}
	if state.promiseCandidates == nil {
		state.promiseCandidates = map[string][]auditedPromiseSignal{}
	}
	if required && len(state.requiredPromises[name]) > 0 {
		return fmt.Errorf("explicit cached promise consumed another signal")
	}
	state.promiseCandidates[name] = append(state.promiseCandidates[name], signal)
	if required {
		if state.requiredPromises == nil {
			state.requiredPromises = map[string][]auditedPromiseSignal{}
		}
		state.requiredPromises[name] = []auditedPromiseSignal{signal}
	}
	return nil
}

func auditSDKCheckpointPromises(ctx context.Context, graph GraphReferenceSnapshot, state *auditedGraphJournal, frame checkpoint.Frame) error {
	for name, raw := range frame.PromiseOutcomes {
		candidates := state.promiseCandidates[name]
		if required := state.requiredPromises[name]; len(required) > 0 {
			candidates = required
		}
		if len(candidates) == 0 {
			return fmt.Errorf("checkpoint promise %s lacks selected child outcome", name)
		}
		matched := false
		for _, signal := range candidates {
			event := signal.event
			body := event.Payload
			if event.Ref != "" {
				if len(body) != 0 || signal.object == "" || !graphAuditHash(event.Hash) {
					return fmt.Errorf("checkpoint promise lacks original signal-owned body")
				}
				var err error
				body, err = graph.LoadObject(ctx, signal.object, graph.PayloadLimit)
				if err != nil {
					return err
				}
				if len(body) > graph.PayloadLimit || digest(body) != event.Hash {
					return fmt.Errorf("checkpoint promise original signal bytes differ")
				}
			}
			var original, saved bytes.Buffer
			// SDK frame encoding compacts RawMessage values; whitespace alone is
			// not a changed promise outcome. Key order and value spelling remain bound.
			if json.Compact(&original, body) != nil || json.Compact(&saved, raw) != nil {
				return fmt.Errorf("invalid selected promise JSON")
			}
			if !bytes.Equal(original.Bytes(), saved.Bytes()) {
				continue
			}
			child, declared := state.children[name]
			if !declared || event.Child == nil || identity.Validate(event.Child.Type, event.Child.ID) != nil || event.Child.Invocation == 0 || event.Child.Type != child.Type || event.Child.ID != child.ID {
				return fmt.Errorf("checkpoint promise child declaration differs")
			}
			var outcome struct {
				Invocation   uint64          `json:"inv_seq"`
				Result       []byte          `json:"result"`
				Ref          string          `json:"result_ref"`
				Hash         string          `json:"result_hash"`
				Error        string          `json:"error"`
				LimitRequest json.RawMessage `json:"limit_request"`
				LimitEntry   json.RawMessage `json:"limit_entry"`
			}
			if checkpoint.DecodeUnambiguous(body, &outcome) != nil || outcome.Invocation != event.Child.Invocation || outcome.Ref != event.Child.Ref || outcome.Hash != event.Child.Hash || (outcome.Ref == "") != (outcome.Hash == "") || outcome.Ref != "" && (len(outcome.Result) != 0 || !graphAuditHash(outcome.Hash) || !signal.childResultOwned) || outcome.Error != "" && (len(outcome.Result) != 0 || outcome.Ref != "") {
				return fmt.Errorf("checkpoint promise original child outcome differs")
			}
			matched = true
			break
		}
		if !matched {
			return fmt.Errorf("checkpoint promise %s differs from selected outcome", name)
		}
	}
	for name := range state.requiredPromises {
		if _, ok := frame.PromiseOutcomes[name]; !ok {
			return fmt.Errorf("checkpoint omitted explicit promise selection %s", name)
		}
	}
	return nil
}
