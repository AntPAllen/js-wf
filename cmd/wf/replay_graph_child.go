package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"
)

// Offline replay must verify the same graph child provenance that native
// export checks. These annotations bind a consumed result to the earlier
// runtime child request and parent-owned outcome, even after child retirement.
// Legacy signals have neither graph annotation and retain their existing path.
func validateReplayGraphChildren(bundle replayBundle) error {
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
			return nil, wf.ErrCorruptJournal
		}
		data, ok := bundle.Objects[ref]
		if !ok {
			return nil, fmt.Errorf("%w: %s", wf.ErrReplayObjectMissing, ref)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != hash {
			return nil, wf.ErrCorruptJournal
		}
		return data, nil
	}
	validateSignal := func(payload json.RawMessage) error {
		var event struct {
			Name      string          `json:"name"`
			Payload   []byte          `json:"payload"`
			Ref       string          `json:"ref"`
			Hash      string          `json:"hash"`
			Child     *child          `json:"graph_child"`
			Canonical json.RawMessage `json:"canonical_signal"`
		}
		if json.Unmarshal(payload, &event) != nil {
			return wf.ErrCorruptJournal
		}
		request, requested := children[event.Name]
		if event.Child == nil {
			// Canonical consumption of a previously requested child cannot omit its
			// binding. A child-named ordinary signal before the request remains ordinary.
			if requested && len(event.Canonical) != 0 && string(event.Canonical) != "null" {
				return wf.ErrCorruptJournal
			}
			return nil
		}
		c := event.Child
		if !requested || c.Type != request.Type || c.ID != request.ID || identity.Validate(c.Type, c.ID) != nil || c.Invocation == 0 || (c.Ref == "") != (c.Hash == "") {
			return wf.ErrCorruptJournal
		}
		data := event.Payload
		if event.Ref != "" {
			var err error
			data, err = load(event.Ref, event.Hash)
			if err != nil {
				return err
			}
		} else if event.Hash != "" {
			return wf.ErrCorruptJournal
		}
		var outcome wf.Outcome
		if json.Unmarshal(data, &outcome) != nil || outcome.InvSeq != c.Invocation || outcome.ResultRef != c.Ref || outcome.ResultHash != c.Hash {
			return wf.ErrCorruptJournal
		}
		if outcome.Error != "" {
			if len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "" {
				return wf.ErrCorruptJournal
			}
			return nil
		}
		if c.Ref != "" {
			if len(outcome.Result) != 0 {
				return wf.ErrCorruptJournal
			}
			_, err := load(c.Ref, c.Hash)
			return err
		}
		return nil
	}
	for _, record := range bundle.Journal {
		switch record.Kind {
		case journal.StepRequested:
			var request struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
				Type string `json:"child_type"`
				ID   string `json:"child_id"`
			}
			if json.Unmarshal(record.Payload, &request) != nil {
				return wf.ErrCorruptJournal
			}
			if request.Kind == "call" || request.Kind == "call_async" {
				if identity.ValidateToken(request.Name) != nil || identity.Validate(request.Type, request.ID) != nil {
					return wf.ErrCorruptJournal
				}
				if _, exists := children[request.Name]; exists {
					return wf.ErrCorruptJournal
				}
				children[request.Name] = child{Type: request.Type, ID: request.ID}
			}
		case journal.SignalConsumed:
			if err := validateSignal(record.Payload); err != nil {
				return err
			}
		case journal.Failed:
			var outcome wf.Outcome
			if json.Unmarshal(record.Payload, &outcome) != nil {
				return wf.ErrCorruptJournal
			}
			if outcome.LimitEntry != nil && outcome.LimitEntry.Kind == string(journal.SignalConsumed) {
				if err := validateSignal(outcome.LimitEntry.Payload); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
