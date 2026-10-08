package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"
)

// This declaration is added only after a runtime child request, current child
// invocation headers, and canonical terminal bytes agree. Its external result
// is copied into the parent's SignalConsumed publication, never inherited from
// a different owner's graph without a new grant.
type graphChildResult struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Invocation uint64 `json:"inv_seq"`
	Ref        string `json:"result_ref,omitempty"`
	Hash       string `json:"result_hash,omitempty"`
}

func (c graphChildResult) validate() error {
	if identity.Validate(c.Type, c.ID) != nil || c.Invocation == 0 || (c.Ref == "") != (c.Hash == "") || c.Hash != "" && !validGraphHash(c.Hash) {
		return journal.ErrGap
	}
	return nil
}

func (g *graphDelivery) prepareChildSignal(ctx context.Context, entry *journal.Entry) (func(), error) {
	if entry.Kind == journal.Failed {
		var outcome wf.Outcome
		if json.Unmarshal(entry.Payload, &outcome) != nil {
			return nil, journal.ErrGap
		}
		if outcome.LimitEntry == nil || outcome.LimitEntry.Kind != string(journal.SignalConsumed) {
			return nil, nil
		}
		nested := journal.Entry{Kind: journal.SignalConsumed, Payload: outcome.LimitEntry.Payload}
		cleanup, err := g.prepareChildSignal(ctx, &nested)
		if err != nil {
			return nil, err
		}
		outcome.LimitEntry.Payload = nested.Payload
		entry.Payload, err = json.Marshal(outcome)
		if err != nil && cleanup != nil {
			cleanup()
			cleanup = nil
		}
		return cleanup, err
	}
	if entry.Kind != journal.SignalConsumed {
		return nil, nil
	}
	var event signalRecord
	if json.Unmarshal(entry.Payload, &event) != nil {
		return nil, journal.ErrGap
	}
	child, err := g.childRequest(event.Name)
	if err != nil {
		return nil, err
	}
	if child == nil {
		if event.Child != nil {
			return nil, journal.ErrGap
		}
		return nil, nil
	} // Ordinary user signals never become child pointers.
	if event.Child != nil || g.invocationPort == nil || identity.Validate(child.Type, child.ID) != nil {
		return nil, journal.ErrGap
	}
	payload := event.Payload
	if event.Ref != "" {
		var ok bool
		payload, ok = g.pending[event.Ref]
		if !ok {
			return nil, fmt.Errorf("%w: unstaged child signal", ErrResultBlobUnavailable)
		}
	}
	var outcome wf.Outcome
	if json.Unmarshal(payload, &outcome) != nil || outcome.InvSeq == 0 {
		return nil, wf.ErrCorruptJournal
	}
	input, err := g.invocationPort.LastInvocation(ctx, identity.InvocationSubject(child.Type, child.ID))
	if err != nil {
		return nil, fmt.Errorf("%w: child invocation: %w", ErrResultBlobUnavailable, err)
	}
	if input == nil || input.Sequence != outcome.InvSeq || input.Header.Get(client.ParentTypeHeader) != g.typ || input.Header.Get(client.ParentIDHeader) != g.id || input.Header.Get(client.ParentInvSeqHeader) != fmt.Sprint(g.invocation) || input.Header.Get(client.ParentSignalHeader) != event.Name {
		return nil, wf.ErrCorruptJournal
	}
	view, err := g.store.OpenTerminal(ctx, child.Type, child.ID, outcome.InvSeq)
	if err != nil {
		return nil, fmt.Errorf("%w: child terminal: %w", ErrResultBlobUnavailable, err)
	}
	if view == nil {
		return nil, fmt.Errorf("%w: child not terminal", ErrResultBlobUnavailable)
	}
	close := func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		// An uncertain release retains this bounded reader until expiry. The
		// parent owns a separate copy; release uncertainty cannot revoke it.
		_ = view.Close(cleanup)
	}
	verified, err := wf.ReadGraphTerminal(ctx, view, outcome.InvSeq, g.store.PayloadReadLimit())
	if err != nil {
		close()
		return nil, err
	}
	if !bytes.Equal(verified.Payload, payload) {
		close()
		return nil, wf.ErrCorruptJournal
	}
	child.Invocation = outcome.InvSeq
	child.Ref = outcome.ResultRef
	child.Hash = outcome.ResultHash
	if err = child.validate(); err != nil {
		close()
		return nil, err
	}
	if child.Ref != "" {
		g.pending[child.Ref] = bytes.Clone(verified.Result)
	}
	event.Child = child
	entry.Payload, err = json.Marshal(event)
	if err != nil {
		close()
		return nil, err
	}
	// Keep the source snapshot pinned through the parent CAS and readback.
	return close, nil
}

// Child identity comes only from the runtime request already in this journal.
func (g *graphDelivery) childRequest(name string) (*graphChildResult, error) {
	var child *graphChildResult
	for _, record := range g.records {
		if record.Kind != journal.StepRequested {
			continue
		}
		var request struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
			Type string `json:"child_type"`
			ID   string `json:"child_id"`
		}
		if json.Unmarshal(record.Payload, &request) != nil {
			return nil, journal.ErrGap
		}
		if (request.Kind == "call" || request.Kind == "call_async") && request.Name == name {
			if child != nil {
				return nil, journal.ErrGap
			}
			child = &graphChildResult{Type: request.Type, ID: request.ID}
		}
	}
	return child, nil
}

// Replay verifies the recorded declaration against parent-owned bytes. It never
// opens the source child, whose generation may already have been retired.
func (g *graphDelivery) validateChildSignal(ctx context.Context, entry journal.Entry) error {
	if entry.Kind == journal.Failed {
		var outcome wf.Outcome
		if json.Unmarshal(entry.Payload, &outcome) != nil {
			return journal.ErrGap
		}
		if outcome.LimitEntry == nil || outcome.LimitEntry.Kind != string(journal.SignalConsumed) {
			return nil
		}
		return g.validateChildSignal(ctx, journal.Entry{Kind: journal.SignalConsumed, Payload: outcome.LimitEntry.Payload})
	}
	if entry.Kind != journal.SignalConsumed {
		return nil
	}
	var event signalRecord
	if json.Unmarshal(entry.Payload, &event) != nil {
		return journal.ErrGap
	}
	request, err := g.childRequest(event.Name)
	if err != nil {
		return err
	}
	if request == nil {
		if event.Child != nil {
			return journal.ErrGap
		}
		return nil
	}
	if event.Child == nil || event.Child.Type != request.Type || event.Child.ID != request.ID || event.Child.validate() != nil {
		return journal.ErrGap
	}
	payload := event.Payload
	if event.Ref != "" {
		payload, err = g.GetBytes(ctx, event.Ref)
		if err != nil {
			return err
		}
	}
	var outcome wf.Outcome
	if json.Unmarshal(payload, &outcome) != nil || outcome.InvSeq != event.Child.Invocation || outcome.ResultRef != event.Child.Ref || outcome.ResultHash != event.Child.Hash {
		return wf.ErrCorruptJournal
	}
	return nil
}
