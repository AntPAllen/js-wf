package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"js-wf/identity"

	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/wf"
)

// Transfer each materialized promise payload into the same canonical append
// as its frame. A frame declaration cannot create authority for unowned bytes.
// The pinned source edge is checked again by the graph publication protocol.
func (g *graphDelivery) materializeCheckpointReferences(ctx context.Context, entry *journal.Entry, refs map[string]string) error {
	if entry.Kind != journal.StepCompleted || len(g.records) == 0 {
		return nil
	}
	request := g.records[len(g.records)-1]
	if request.Kind != journal.StepRequested {
		return nil
	}
	var declaration struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if json.Unmarshal(request.Payload, &declaration) != nil {
		return journal.ErrGap
	}
	if declaration.Kind != "checkpoint" {
		return nil
	}
	var completion struct {
		Ref  string `json:"result_ref"`
		Hash string `json:"result_hash"`
	}
	if json.Unmarshal(entry.Payload, &completion) != nil || completion.Ref == "" || refs[completion.Ref] != completion.Hash {
		return journal.ErrGap
	}
	raw, err := g.GetBytes(ctx, completion.Ref)
	if err != nil {
		return err
	}
	frame, err := checkpoint.Decode(raw, completion.Hash, checkpoint.Identity{Type: g.typ, ID: g.id, InvSeq: g.invocation}, checkpoint.Anchor{Index: entry.Index, Epoch: entry.Epoch})
	if err != nil || frame.Stage != declaration.Name || graphHash(frame.Data) != declaration.InputHash {
		return journal.ErrGap
	}
	for _, raw := range frame.PromiseOutcomes {
		var outcome wf.Outcome
		if json.Unmarshal(raw, &outcome) != nil {
			return journal.ErrGap
		}
		if outcome.ResultRef == "" {
			continue
		}
		source, ok := g.refs[outcome.ResultRef]
		if !ok || source.link.Hash != outcome.ResultHash {
			return journal.ErrGap
		}
		if hash, exists := refs[outcome.ResultRef]; exists && hash != outcome.ResultHash {
			return journal.ErrGap
		}
		refs[outcome.ResultRef] = outcome.ResultHash
	}
	meta := graphCheckpointMetadata{Version: 1, Identity: frame.Identity, Anchor: frame.Anchor, FrameHash: completion.Hash, Children: g.children, Signals: map[uint64]signalRecord{}, SignalNext: g.signalBase, SignalLast: g.signalLast}
	for _, record := range g.records {
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var event signalRecord
		if json.Unmarshal(record.Payload, &event) != nil || event.Canonical == nil || event.Canonical.Index != meta.SignalNext || event.Sequence <= meta.SignalLast {
			return journal.ErrGap
		}
		meta.SignalNext++
		meta.SignalLast = event.Sequence
	}
	if meta.SignalLast > frame.SignalCursor {
		return journal.ErrGap
	}
	for _, signal := range frame.PendingSignals {
		event, ok := g.childSignals[signal.Sequence]
		if !ok {
			continue
		}
		if event.Name != signal.Name || event.Child == nil {
			return journal.ErrGap
		}
		declared, err := graphReferences(journal.Entry{Kind: journal.SignalConsumed, Payload: mustCheckpointJSON(event)})
		if err != nil {
			return err
		}
		for name, hash := range declared {
			source, ok := g.refs[name]
			if !ok || source.link.Hash != hash {
				return journal.ErrGap
			}
			if old, ok := refs[name]; ok && old != hash {
				return journal.ErrGap
			}
			refs[name] = hash
		}
		meta.Signals[signal.Sequence] = event
	}
	for name, child := range meta.Children {
		if identity.ValidateToken(name) != nil || identity.Validate(child.Type, child.ID) != nil || child.Invocation != 0 || child.Ref != "" || child.Hash != "" {
			return journal.ErrGap
		}
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if len(encoded) > checkpoint.MaxBytes {
		return journal.ErrTooLong
	}
	metadataHash := graphHash(encoded)
	metadataRef := "step-result-" + metadataHash
	g.pending[metadataRef] = encoded
	refs[metadataRef] = metadataHash
	var fields map[string]json.RawMessage
	if json.Unmarshal(entry.Payload, &fields) != nil {
		return journal.ErrGap
	}
	fields["checkpoint_metadata_ref"], _ = json.Marshal(metadataRef)
	fields["checkpoint_metadata_hash"], _ = json.Marshal(metadataHash)
	entry.Payload, err = json.Marshal(fields)
	if err != nil {
		return err
	}

	return nil
}

// This object is owned by the checkpoint completion alongside its frame.
// It preserves worker provenance that the SDK frame cannot reconstruct.
type graphCheckpointMetadata struct {
	Version    int                         `json:"version"`
	Identity   checkpoint.Identity         `json:"identity"`
	Anchor     checkpoint.Anchor           `json:"anchor"`
	FrameHash  string                      `json:"frame_sha256"`
	Children   map[string]graphChildResult `json:"children"`
	Signals    map[uint64]signalRecord     `json:"signals"`
	SignalNext uint64                      `json:"signal_next"`
	SignalLast uint64                      `json:"signal_last"`
}

func mustCheckpointJSON(event signalRecord) []byte { raw, _ := json.Marshal(event); return raw }

func (g *graphDelivery) restoreCheckpointMetadata(ctx context.Context) error {
	found := g.checkpoint
	anchor, err := g.view.Read(ctx, found.Anchor.Index)
	if err != nil {
		return err
	}
	if err := g.register(anchor); err != nil {
		return err
	}
	var completion struct {
		Ref  string `json:"checkpoint_metadata_ref"`
		Hash string `json:"checkpoint_metadata_hash"`
	}
	if json.Unmarshal(anchor.Payload, &completion) != nil || completion.Ref == "" || !validGraphHash(completion.Hash) {
		return journal.ErrGap
	}
	raw, err := g.GetBytes(ctx, completion.Ref)
	if err != nil {
		return err
	}
	if len(raw) > checkpoint.MaxBytes || graphHash(raw) != completion.Hash {
		return journal.ErrGap
	}
	var meta graphCheckpointMetadata
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&meta) != nil || decoder.Decode(new(any)) != io.EOF || meta.Version != 1 || meta.Identity != (checkpoint.Identity{Type: g.typ, ID: g.id, InvSeq: g.invocation}) || meta.Anchor != (checkpoint.Anchor{Index: found.Runtime.Index, Epoch: found.Runtime.Epoch}) || meta.FrameHash != found.Runtime.SHA256 {
		return journal.ErrGap
	}
	frame, err := checkpoint.Decode(found.Frame, found.Runtime.SHA256, meta.Identity, meta.Anchor)
	if err != nil || meta.SignalLast > frame.SignalCursor || meta.SignalNext > g.view.SignalQueueCount() || (meta.SignalNext == 0) != (meta.SignalLast == 0) {
		return journal.ErrGap
	}
	consumed := g.view.SignalConsumedCount()
	for _, record := range found.Records {
		if record.Kind == journal.SignalConsumed {
			if consumed == 0 {
				return journal.ErrGap
			}
			consumed--
		}
	}
	if meta.SignalNext != consumed {
		return journal.ErrGap
	}
	if meta.SignalNext > 0 {
		binding, err := g.view.SignalBindingAt(ctx, meta.SignalNext-1)
		if err != nil {
			return err
		}
		if binding.Sequence != meta.SignalLast {
			return journal.ErrGap
		}
	}
	refs := map[string]string{}
	add := func(name, hash string) error {
		if name == "" {
			if hash != "" {
				return journal.ErrGap
			}
			return nil
		}
		if old, ok := refs[name]; ok && old != hash {
			return journal.ErrGap
		}
		refs[name] = hash
		return nil
	}
	for _, raw := range frame.PromiseOutcomes {
		var out wf.Outcome
		if json.Unmarshal(raw, &out) != nil {
			return journal.ErrGap
		}
		if err := add(out.ResultRef, out.ResultHash); err != nil {
			return err
		}
	}
	for name, child := range meta.Children {
		if identity.ValidateToken(name) != nil || identity.Validate(child.Type, child.ID) != nil || child.Invocation != 0 || child.Ref != "" || child.Hash != "" {
			return journal.ErrGap
		}
	}
	for seq, event := range meta.Signals {
		var buffered *checkpoint.Signal
		for i := range frame.PendingSignals {
			if frame.PendingSignals[i].Sequence == seq {
				buffered = &frame.PendingSignals[i]
				break
			}
		}
		if buffered == nil || event.Sequence != seq || event.Name != buffered.Name || event.Child == nil || event.Child.validate() != nil {
			return journal.ErrGap
		}
		if event.Canonical == nil || event.Canonical.Index >= meta.SignalNext || event.Sequence > meta.SignalLast {
			return journal.ErrGap
		}
		binding, err := g.view.SignalBindingAt(ctx, event.Canonical.Index)
		if err != nil {
			return err
		}
		if binding.Sequence != event.Sequence || binding.Input.Token != event.Canonical.Token || binding.Input.Request.Name != event.Name || binding.Input.InputSHA256 != event.Hash {
			return journal.ErrGap
		}
		if event.Ref != "" && graphHash(buffered.Payload) != event.Hash || event.Ref == "" && !bytes.Equal(event.Payload, buffered.Payload) {
			return journal.ErrGap
		}
		child, ok := meta.Children[event.Name]
		if !ok || child.Type != event.Child.Type || child.ID != event.Child.ID {
			return journal.ErrGap
		}
		declared, err := graphReferences(journal.Entry{Kind: journal.SignalConsumed, Payload: mustCheckpointJSON(event)})
		if err != nil {
			return err
		}
		for name, hash := range declared {
			if err := add(name, hash); err != nil {
				return err
			}
		}
	}
	if err := g.registerReferences(anchor, refs); err != nil {
		return err
	}
	g.children = meta.Children
	if g.children == nil {
		g.children = map[string]graphChildResult{}
	}
	g.childSignals = meta.Signals
	if g.childSignals == nil {
		g.childSignals = map[uint64]signalRecord{}
	}
	g.signalBase, g.signalLast = meta.SignalNext, meta.SignalLast
	for _, event := range g.childSignals {
		if err := g.validateChildSignal(ctx, journal.Entry{Kind: journal.SignalConsumed, Payload: mustCheckpointJSON(event)}); err != nil {
			return err
		}
	}
	return nil
}
