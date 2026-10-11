package integrity

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"js-wf/identity"
	"js-wf/journal"
)

func canonicalSignalCursor(c auditedGraphCursor) bool {
	return c.Schema == "js-wf-graph-runtime-cursor-v4" || c.Schema == "js-wf-graph-runtime-cursor-v5" || c.Schema == "js-wf-graph-runtime-cursor-v6"
}

// This audit uses wire descriptors only, never GraphView or its validators.
// Queue bodies survive deletion of the original WF_SIG transport messages.
func auditGraphSignalRecord(ctx context.Context, graph GraphReferenceSnapshot, state *auditedGraphJournal, record GraphAuditRecord) error {
	c := state.cursor
	if !canonicalSignalCursor(c) || c.Start == nil {
		return fmt.Errorf("signal forest without canonical signal generation")
	}
	var input journal.GraphSignalInput
	var binding journal.GraphSignalBinding
	var packet []byte
	if record.Stream == "signal-input" {
		var wire struct {
			Input       journal.GraphSignalInput `json:"input"`
			IndexPacket []byte                   `json:"index_packet"`
		}
		if json.Unmarshal(record.Record.Data, &wire) != nil || record.Index != uint64(len(state.signalInputs)) || wire.Input.Index != record.Index {
			return fmt.Errorf("invalid signal reservation record/order")
		}
		input, packet = wire.Input, wire.IndexPacket
	} else {
		var wire struct {
			Binding     journal.GraphSignalBinding `json:"binding"`
			IndexPacket []byte                     `json:"index_packet"`
		}
		if json.Unmarshal(record.Record.Data, &wire) != nil || wire.Binding.Schema != "js-wf-canonical-signal-binding-v1" || record.Index != uint64(len(state.signalBindings)) || wire.Binding.Index != record.Index {
			return fmt.Errorf("invalid signal queue record/order")
		}
		binding, input, packet = wire.Binding, wire.Binding.Input, wire.IndexPacket
		if binding.Sequence == 0 || binding.Sequence > c.SignalSource || len(state.signalBindings) > 0 && binding.Sequence <= state.signalBindings[len(state.signalBindings)-1].Sequence {
			return fmt.Errorf("signal queue source order differs")
		}
		if input.Index >= uint64(len(state.signalInputs)) || state.signalInputs[input.Index] != input {
			return fmt.Errorf("signal queue reservation differs")
		}
		if state.boundInputs == nil {
			state.boundInputs = map[uint64]bool{}
		}
		if state.boundInputs[input.Index] {
			return fmt.Errorf("duplicate bound reservation")
		}
		state.boundInputs[input.Index] = true
	}
	r := input.Request
	if input.Schema != "js-wf-canonical-signal-input-v1" || identity.Validate(r.Type, r.ID) != nil || identity.ValidateToken(r.Name) != nil || len(r.Type) > 256 || len(r.ID) > 256 || len(r.Name) > 256 || r.Type != c.Start.Request.Type || r.ID != c.Start.Request.ID || r.Invocation != c.Invocation || r.Invocation == 0 || len(r.Key) == 0 || len(r.Key) > 128 || !utf8.ValidString(r.Key) || !graphAuditID(input.Token) || !graphAuditHash(input.InputSHA256) || input.InputSize < 0 || input.InputSize > graph.PayloadLimit || input.Index >= c.SignalInputs {
		return fmt.Errorf("invalid canonical signal input identity")
	}
	if len(record.Record.Blobs) != 1 || record.Record.Blobs[0].Hash != input.InputSHA256 {
		return fmt.Errorf("signal input lacks exact owned body")
	}
	body, err := graph.LoadObject(ctx, record.Record.Blobs[0].Reference.Object, graph.PayloadLimit)
	if err != nil {
		return err
	}
	if len(body) != input.InputSize || digest(body) != input.InputSHA256 {
		return fmt.Errorf("signal input body size/hash differs")
	}
	requestBytes, _ := json.Marshal(r)
	index := &state.inputIndex
	if record.Stream == "signal-queue" {
		index = &state.queueIndex
	}
	if err := index.append(ctx, packet, sha256.Sum256(requestBytes), record.Index); err != nil {
		return err
	}
	if record.Stream == "signal-input" {
		if state.signalKeys == nil {
			state.signalKeys = map[journal.GraphSignalRequest]bool{}
			state.signalTokens = map[string]bool{}
		}
		if state.signalKeys[r] || state.signalTokens[input.Token] {
			return fmt.Errorf("duplicate signal reservation identity")
		}
		state.signalKeys[r], state.signalTokens[input.Token] = true, true
		state.signalInputs = append(state.signalInputs, input)
	} else {
		state.signalBindings = append(state.signalBindings, binding)
	}
	return nil
}

func auditGraphSignalEvent(ctx context.Context, graph GraphReferenceSnapshot, record GraphAuditRecord, signal auditedCheckpointSignal) error {
	if signal.Canonical == nil || !graphAuditID(signal.Canonical.Token) || signal.Ref != "graph-signal-"+signal.Hash || !graphAuditHash(signal.Hash) || len(signal.Payload) != 0 {
		return fmt.Errorf("invalid canonical signal consumption marker")
	}
	for _, edge := range record.Record.Blobs {
		if edge.Hash != signal.Hash {
			continue
		}
		body, err := graph.LoadObject(ctx, edge.Reference.Object, graph.PayloadLimit)
		if err != nil {
			return err
		}
		if len(body) > graph.PayloadLimit || digest(body) != signal.Hash {
			return fmt.Errorf("invalid owned consumed signal bytes")
		}
		return nil
	}
	return fmt.Errorf("consumed signal lacks journal-owned body")
}

func finishGraphSignalAudit(state *auditedGraphJournal) error {
	c := state.cursor
	if !canonicalSignalCursor(c) {
		return nil
	}
	if uint64(len(state.signalInputs)) != c.SignalInputs || uint64(len(state.signalBindings)) != c.SignalBindings || uint64(len(state.signalEvents)) != c.SignalConsumed {
		return fmt.Errorf("canonical signal census differs")
	}
	for index, signal := range state.signalEvents {
		if signal.Canonical == nil || signal.Canonical.Index != uint64(index) || index >= len(state.signalBindings) {
			return fmt.Errorf("signal consumption queue order differs")
		}
		binding := state.signalBindings[index]
		if signal.Canonical.Token != binding.Input.Token || signal.Sequence != binding.Sequence || signal.Name != binding.Input.Request.Name || signal.Hash != binding.Input.InputSHA256 {
			return fmt.Errorf("signal consumption binding differs")
		}
	}
	return nil
}
