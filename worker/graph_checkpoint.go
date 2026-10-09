package worker

import (
	"context"
	"encoding/json"

	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/wf"
)

// Transfer each materialized promise payload into the same canonical append
// as its frame. A frame declaration cannot create authority for unowned bytes.
// The pinned source edge is checked again by the graph publication protocol.
func (g *graphDelivery) materializeCheckpointReferences(ctx context.Context, entry journal.Entry, refs map[string]string) error {
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
	return nil
}
