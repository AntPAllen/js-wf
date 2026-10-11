package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

// These metadata shapes are decoded independently of the production cursor and
// checkpoint reader. The shared frame type and unambiguous JSON decoder are wire
// definitions only; checkpoint.Decode and production admission are not called.
type auditedCheckpointPointer struct {
	Runtime struct {
		InvSeq       uint64 `json:"inv_seq"`
		Stage        string `json:"stage"`
		Sequence     uint64 `json:"sequence"`
		Index        uint64 `json:"index"`
		Epoch        uint64 `json:"epoch"`
		StepPosition uint64 `json:"step_position"`
		Object       string `json:"object"`
		SHA256       string `json:"sha256"`
	} `json:"runtime"`
	RequestIndex uint64 `json:"request_index"`
}

func auditCheckpointPointer(c auditedGraphCursor) (*auditedCheckpointPointer, error) {
	var p auditedCheckpointPointer
	if checkpoint.DecodeUnambiguous(c.Checkpoint, &p) != nil {
		return nil, fmt.Errorf("invalid checkpoint pointer JSON")
	}
	r := p.Runtime
	if c.Schema != "js-wf-graph-runtime-cursor-v5" && c.Schema != "js-wf-graph-runtime-cursor-v6" ||
		r.InvSeq != c.Invocation || r.InvSeq == 0 || identity.ValidateToken(r.Stage) != nil ||
		r.Index >= c.Count || r.Sequence != c.Base+r.Index+1 || r.Epoch == 0 || r.Epoch > c.Epoch ||
		r.StepPosition < 2 || r.StepPosition%2 != 0 || r.StepPosition > r.Index ||
		!graphAuditHash(r.SHA256) || r.Object != "step-result-"+r.SHA256 ||
		p.RequestIndex == 0 || p.RequestIndex >= r.Index || p.RequestIndex < c.RetainedFrom {
		return nil, fmt.Errorf("invalid checkpoint pointer bounds/generation")
	}
	return &p, nil
}

func auditCheckpointFrame(ctx context.Context, graph GraphReferenceSnapshot, state *auditedGraphJournal, anchor journal.Record, edges []retainedgraph.Link) error {
	p := state.checkpoint
	r := p.Runtime
	request := state.checkpointRequest
	var declared struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if state.lastStepRequest != p.RequestIndex || request.Kind != journal.StepRequested || request.Index != p.RequestIndex || request.Sequence >= anchor.Sequence || request.Epoch > anchor.Epoch ||
		json.Unmarshal(request.Payload, &declared) != nil || declared.Kind != "checkpoint" || declared.Name != r.Stage || !graphAuditHash(declared.InputHash) ||
		anchor.Kind != journal.StepCompleted || anchor.Index != r.Index || anchor.Sequence != r.Sequence || anchor.Epoch != r.Epoch || state.sdkPosition != r.StepPosition {
		return fmt.Errorf("checkpoint request/completion anchor differs")
	}
	var done struct {
		Result     json.RawMessage `json:"result"`
		ResultRef  string          `json:"result_ref"`
		ResultHash string          `json:"result_hash"`
		Error      string          `json:"error"`
		ErrorKind  string          `json:"error_kind"`
		SignalSeq  uint64          `json:"signal_seq"`
		Selected   string          `json:"selected"`
	}
	if json.Unmarshal(anchor.Payload, &done) != nil || len(done.Result) != 0 || done.Error != "" || done.ErrorKind != "" || done.SignalSeq != 0 || done.Selected != "" || done.ResultRef != r.Object || done.ResultHash != r.SHA256 {
		return fmt.Errorf("checkpoint completion result differs")
	}
	var frameLink *retainedgraph.Link
	for i := range edges {
		if edges[i].Hash == r.SHA256 {
			if frameLink != nil {
				return fmt.Errorf("duplicate checkpoint frame edge")
			}
			frameLink = &edges[i]
		}
	}
	if frameLink == nil {
		return fmt.Errorf("checkpoint frame lacks owned edge")
	}
	raw, err := graph.LoadObject(ctx, frameLink.Reference.Object, checkpoint.MaxBytes)
	if err != nil {
		return err
	}
	if len(raw) > checkpoint.MaxBytes || digest(raw) != r.SHA256 {
		return fmt.Errorf("checkpoint frame bytes differ")
	}
	var frame checkpoint.Frame
	if checkpoint.DecodeUnambiguous(raw, &frame) != nil {
		return fmt.Errorf("invalid checkpoint frame JSON")
	}
	parts := strings.Split(state.key, ".")
	if len(parts) != 2 || frame.Version != checkpoint.Version || frame.Identity.Type != parts[0] || frame.Identity.ID != parts[1] || frame.Identity.InvSeq != r.InvSeq ||
		frame.Anchor.Index != r.Index || frame.Anchor.Epoch != r.Epoch || frame.Stage != r.Stage || frame.StepPosition != r.StepPosition ||
		!json.Valid(frame.Data) || digest(frame.Data) != declared.InputHash {
		return fmt.Errorf("checkpoint frame identity/anchor/stage/locals differs")
	}
	return nil
}
