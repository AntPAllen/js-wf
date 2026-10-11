package integrity

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/internal/retainedgraph"
)

type auditedCheckpointChild struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Invocation uint64 `json:"inv_seq"`
	Ref        string `json:"result_ref,omitempty"`
	Hash       string `json:"result_hash,omitempty"`
}

type auditedCheckpointSignal struct {
	Sequence  uint64                  `json:"sig_seq"`
	Name      string                  `json:"name"`
	Payload   []byte                  `json:"payload,omitempty"`
	Ref       string                  `json:"ref,omitempty"`
	Hash      string                  `json:"hash,omitempty"`
	Child     *auditedCheckpointChild `json:"graph_child,omitempty"`
	Canonical *struct {
		Index uint64 `json:"index"`
		Token string `json:"token"`
	} `json:"canonical_signal,omitempty"`
}

type auditedCheckpointMetadata struct {
	Version    int                                `json:"version"`
	Identity   checkpoint.Identity                `json:"identity"`
	Anchor     checkpoint.Anchor                  `json:"anchor"`
	FrameHash  string                             `json:"frame_sha256"`
	Children   map[string]auditedCheckpointChild  `json:"children"`
	Signals    map[uint64]auditedCheckpointSignal `json:"signals"`
	SignalNext uint64                             `json:"signal_next"`
	SignalLast uint64                             `json:"signal_last"`
}

func auditCheckpointMetadata(ctx context.Context, graph GraphReferenceSnapshot, state *auditedGraphJournal, frame checkpoint.Frame, edges []retainedgraph.Link, ref, hash string) error {
	// Journal-only checkpoint writers have no worker annotation. A partial
	// annotation cannot be mistaken for that valid unannotated wire format.
	if ref == "" && hash == "" {
		return nil
	}
	if ref != "step-result-"+hash || !graphAuditHash(hash) {
		return fmt.Errorf("invalid checkpoint metadata reference")
	}
	load := func(hash string) ([]byte, error) {
		for _, edge := range edges {
			if edge.Hash != hash {
				continue
			}
			raw, err := graph.LoadObject(ctx, edge.Reference.Object, graph.PayloadLimit)
			if err != nil {
				return nil, err
			}
			if len(raw) > graph.PayloadLimit || digest(raw) != hash {
				return nil, fmt.Errorf("checkpoint metadata payload differs")
			}
			return raw, nil
		}
		return nil, fmt.Errorf("checkpoint metadata lacks owned payload edge")
	}
	raw, err := load(hash)
	if err != nil {
		return err
	}
	var meta auditedCheckpointMetadata
	if checkpoint.DecodeUnambiguous(raw, &meta) != nil || meta.Version != 1 || meta.Identity != frame.Identity || meta.Anchor != frame.Anchor || meta.FrameHash != state.checkpoint.Runtime.SHA256 {
		return fmt.Errorf("checkpoint metadata identity/anchor/frame differs")
	}
	if meta.SignalNext != state.signalCount || meta.SignalLast != state.journal.lastSignal || meta.SignalNext > state.cursor.SignalBindings || meta.SignalLast > frame.SignalCursor || (meta.SignalNext == 0) != (meta.SignalLast == 0) {
		return fmt.Errorf("checkpoint metadata signal prefix differs")
	}
	if len(meta.Children) != len(state.children) {
		return fmt.Errorf("checkpoint metadata child census differs")
	}
	for name, child := range meta.Children {
		declared, ok := state.children[name]
		if !ok || identity.ValidateToken(name) != nil || identity.Validate(child.Type, child.ID) != nil || child != declared {
			return fmt.Errorf("checkpoint metadata child declaration differs")
		}
	}
	refs := map[string]string{ref: hash, state.checkpoint.Runtime.Object: state.checkpoint.Runtime.SHA256}
	add := func(ref, hash string) error {
		if ref == "" && hash == "" {
			return nil
		}
		if ref == "" || !graphAuditHash(hash) {
			return fmt.Errorf("invalid checkpoint metadata result reference")
		}
		if prior, ok := refs[ref]; ok && prior != hash {
			return fmt.Errorf("checkpoint metadata reference alias differs")
		}
		refs[ref] = hash
		_, err := load(hash)
		return err
	}
	pending := map[uint64]checkpoint.Signal{}
	for _, signal := range frame.PendingSignals {
		pending[signal.Sequence] = signal
	}
	for seq, event := range meta.Signals {
		buffered, ok := pending[seq]
		original, recorded := state.childSignals[seq]
		child, declared := meta.Children[event.Name]
		if !ok || !recorded || !declared || !reflect.DeepEqual(event, original) || event.Sequence != seq || event.Name != buffered.Name || event.Child == nil || identity.Validate(event.Child.Type, event.Child.ID) != nil || event.Child.Invocation == 0 || event.Child.Type != child.Type || event.Child.ID != child.ID || event.Canonical == nil || event.Canonical.Index >= meta.SignalNext || event.Sequence > meta.SignalLast || !graphAuditID(event.Canonical.Token) {
			return fmt.Errorf("checkpoint metadata buffered child provenance differs")
		}
		if event.Ref == "" && !bytes.Equal(event.Payload, buffered.Payload) || event.Ref != "" && digest(buffered.Payload) != event.Hash {
			return fmt.Errorf("checkpoint metadata buffered child payload differs")
		}
		if err := add(event.Ref, event.Hash); err != nil {
			return err
		}
		if err := add(event.Child.Ref, event.Child.Hash); err != nil {
			return err
		}
	}
	// The worker retains metadata for every buffered child, not only a selected
	// subset. Ordinary pending user signals need no child provenance entry.
	for seq := range pending {
		if _, child := state.childSignals[seq]; child {
			if _, ok := meta.Signals[seq]; !ok {
				return fmt.Errorf("checkpoint metadata buffered child omitted")
			}
		}
	}
	return nil
}
