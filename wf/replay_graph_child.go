package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/journal"
)

// ValidateReplayGraphChildren verifies graph child annotations without executing
// a handler. Replay additionally validates each selected child signal, including
// ordinary signals consumed before the child's request. Unannotated legacy
// histories keep their existing signal-backed child behavior.
func ValidateReplayGraphChildren(records []journal.Record, objects map[string][]byte) error {
	_, err := replayGraphChildValidator(records, objects)
	return err
}

func replayGraphChildValidator(records []journal.Record, objects map[string][]byte, canonical ...bool) (func(context.Context, Signal) error, error) {
	if !hasReplayGraphAnnotations(records) && (len(canonical) == 0 || !canonical[0]) {
		return nil, nil
	}
	bindings := map[uint64]Signal{}
	type child struct {
		Type       string `json:"type"`
		ID         string `json:"id"`
		Invocation uint64 `json:"inv_seq"`
		Ref        string `json:"result_ref,omitempty"`
		Hash       string `json:"result_hash,omitempty"`
	}
	children := map[string]child{}
	load := func(ref, hash string) ([]byte, error) {
		expected, err := hex.DecodeString(hash)
		if ref == "" || err != nil || len(expected) != sha256.Size {
			return nil, ErrCorruptJournal
		}
		data, ok := objects[ref]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrReplayObjectMissing, ref)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != hash {
			return nil, ErrCorruptJournal
		}
		return data, nil
	}
	validateSignal := func(payload json.RawMessage) error {
		var event struct {
			Sequence  uint64          `json:"sig_seq"`
			Name      string          `json:"name"`
			Payload   []byte          `json:"payload"`
			Ref       string          `json:"ref"`
			Hash      string          `json:"hash"`
			Child     *child          `json:"graph_child"`
			Canonical json.RawMessage `json:"canonical_signal"`
		}
		if json.Unmarshal(payload, &event) != nil {
			return ErrCorruptJournal
		}
		request, requested := children[event.Name]
		if event.Child == nil {
			// Canonical consumption of a previously requested child cannot omit its
			// binding. A child-named ordinary signal before the request remains ordinary.
			if requested {
				return ErrCorruptJournal
			}
			return nil
		}
		if event.Sequence == 0 {
			return ErrCorruptJournal
		}
		if _, exists := bindings[event.Sequence]; exists {
			return ErrCorruptJournal
		}
		c := event.Child
		if !requested || c.Type != request.Type || c.ID != request.ID || identity.Validate(c.Type, c.ID) != nil || c.Invocation == 0 || (c.Ref == "") != (c.Hash == "") {
			return ErrCorruptJournal
		}
		data := event.Payload
		if event.Ref != "" {
			if len(event.Payload) != 0 {
				return ErrCorruptJournal
			}
			var err error
			data, err = load(event.Ref, event.Hash)
			if err != nil {
				return err
			}
		} else if event.Hash != "" {
			return ErrCorruptJournal
		}
		var outcome Outcome
		if json.Unmarshal(data, &outcome) != nil || outcome.InvSeq != c.Invocation || outcome.ResultRef != c.Ref || outcome.ResultHash != c.Hash {
			return ErrCorruptJournal
		}
		bindings[event.Sequence] = Signal{Sequence: event.Sequence, Name: event.Name, Payload: bytes.Clone(data)}
		if outcome.Error != "" {
			if len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "" {
				return ErrCorruptJournal
			}
			return nil
		}
		if c.Ref != "" {
			if len(outcome.Result) != 0 {
				return ErrCorruptJournal
			}
			_, err := load(c.Ref, c.Hash)
			return err
		}
		return nil
	}
	for _, record := range records {
		switch record.Kind {
		case journal.StepRequested:
			var request struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
				Type string `json:"child_type"`
				ID   string `json:"child_id"`
			}
			if json.Unmarshal(record.Payload, &request) != nil {
				return nil, ErrCorruptJournal
			}
			if request.Kind == "call" || request.Kind == "call_async" {
				if identity.ValidateToken(request.Name) != nil || identity.Validate(request.Type, request.ID) != nil {
					return nil, ErrCorruptJournal
				}
				if _, exists := children[request.Name]; exists {
					return nil, ErrCorruptJournal
				}
				children[request.Name] = child{Type: request.Type, ID: request.ID}
			}
		case journal.SignalConsumed:
			if err := validateSignal(record.Payload); err != nil {
				return nil, err
			}
		case journal.Failed:
			var outcome Outcome
			if json.Unmarshal(record.Payload, &outcome) != nil {
				return nil, ErrCorruptJournal
			}
			if outcome.LimitEntry != nil && outcome.LimitEntry.Kind == string(journal.SignalConsumed) {
				if err := validateSignal(outcome.LimitEntry.Payload); err != nil {
					return nil, err
				}
			}
		}
	}
	return func(ctx context.Context, selected Signal) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		binding, ok := bindings[selected.Sequence]
		if !ok || binding.Name != selected.Name || !bytes.Equal(binding.Payload, selected.Payload) {
			return ErrCorruptJournal
		}
		return nil
	}, nil
}

// Graph checkpoint metadata marks a history even when it has not consumed a
// signal yet. Signal annotations also mark pre-checkpoint canonical histories.
func hasReplayGraphAnnotations(records []journal.Record) bool {
	signal := func(payload json.RawMessage) bool {
		var event struct {
			Child     json.RawMessage `json:"graph_child"`
			Canonical json.RawMessage `json:"canonical_signal"`
		}
		if json.Unmarshal(payload, &event) != nil {
			return false
		}
		present := func(raw json.RawMessage) bool { return len(raw) != 0 && string(raw) != "null" }
		return present(event.Child) || present(event.Canonical)
	}
	for _, record := range records {
		switch record.Kind {
		case journal.SignalConsumed:
			if signal(record.Payload) {
				return true
			}
		case journal.StepCompleted:
			var completion struct {
				Metadata json.RawMessage `json:"checkpoint_metadata_ref"`
			}
			if json.Unmarshal(record.Payload, &completion) == nil && len(completion.Metadata) != 0 && string(completion.Metadata) != "null" {
				return true
			}
		case journal.Failed:
			var outcome Outcome
			if json.Unmarshal(record.Payload, &outcome) == nil && outcome.LimitEntry != nil && outcome.LimitEntry.Kind == string(journal.SignalConsumed) && signal(outcome.LimitEntry.Payload) {
				return true
			}
		}
	}
	return false
}
