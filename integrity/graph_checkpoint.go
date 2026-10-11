package integrity

import (
	"bytes"
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
		Result       json.RawMessage `json:"result"`
		ResultRef    string          `json:"result_ref"`
		ResultHash   string          `json:"result_hash"`
		Error        string          `json:"error"`
		ErrorKind    string          `json:"error_kind"`
		SignalSeq    uint64          `json:"signal_seq"`
		Selected     string          `json:"selected"`
		MetadataRef  string          `json:"checkpoint_metadata_ref"`
		MetadataHash string          `json:"checkpoint_metadata_hash"`
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
	if frame.PanicAttempts != uint64(state.journal.lastAttempt) || frame.SignalCursor < state.journal.lastSignal {
		return fmt.Errorf("checkpoint attempt/signal cursor differs from journal prefix")
	}
	if err := auditMaterializedCheckpoint(frame); err != nil {
		return err
	}
	// Worker annotation identifies an SDK materialization. Journal-only frame
	// writers may supply state without recording SDK operations.
	if done.MetadataRef != "" || done.MetadataHash != "" {
		if err := auditSDKCheckpointState(state.sdkState, frame.State); err != nil {
			return err
		}
	}
	// The checkpoint completion must own the transitive promise payloads, not
	// merely mention hashes previously reachable somewhere in the journal.
	refs := map[string]string{r.Object: r.SHA256}
	for name, raw := range frame.PromiseOutcomes {
		var outcome struct {
			ResultRef  string `json:"result_ref"`
			ResultHash string `json:"result_hash"`
		}
		if json.Unmarshal(raw, &outcome) != nil {
			return fmt.Errorf("invalid checkpoint promise %s", name)
		}
		if outcome.ResultRef == "" {
			continue
		}
		if prior, ok := refs[outcome.ResultRef]; ok && prior != outcome.ResultHash {
			return fmt.Errorf("checkpoint promise aliases conflicting result hashes")
		}
		refs[outcome.ResultRef] = outcome.ResultHash
		found := false
		for _, link := range edges {
			if link.Hash != outcome.ResultHash {
				continue
			}
			data, err := graph.LoadObject(ctx, link.Reference.Object, graph.PayloadLimit)
			if err != nil {
				return err
			}
			if len(data) > graph.PayloadLimit || digest(data) != outcome.ResultHash {
				return fmt.Errorf("checkpoint promise result bytes differ")
			}
			found = true
			break
		}
		if !found {
			return fmt.Errorf("checkpoint promise %s lacks owned result edge", name)
		}
	}
	return auditCheckpointMetadata(ctx, graph, state, frame, edges, done.MetadataRef, done.MetadataHash)
}

// This validates materialized values and identity sets without calling the
// production frame validator. Comparing these values to SDK history remains a
// separate audit.
func auditMaterializedCheckpoint(frame checkpoint.Frame) error {
	for key, value := range frame.State {
		if identity.ValidateToken(key) != nil || !json.Valid(value) {
			return fmt.Errorf("invalid checkpoint state entry")
		}
	}
	for name, raw := range frame.PromiseOutcomes {
		if identity.ValidateToken(name) != nil || len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
			return fmt.Errorf("invalid checkpoint promise outcome")
		}
		var out struct {
			InvSeq       uint64          `json:"inv_seq,omitempty"`
			Result       []byte          `json:"result,omitempty"`
			ResultRef    string          `json:"result_ref,omitempty"`
			ResultHash   string          `json:"result_hash,omitempty"`
			Error        string          `json:"error,omitempty"`
			LimitRequest json.RawMessage `json:"limit_request,omitempty"`
			LimitEntry   json.RawMessage `json:"limit_entry,omitempty"`
		}
		if checkpoint.DecodeUnambiguous(raw, &out) != nil ||
			out.ResultRef == "" && out.ResultHash != "" ||
			out.ResultRef != "" && (len(out.Result) != 0 || !graphAuditHash(out.ResultHash)) {
			return fmt.Errorf("invalid checkpoint promise result")
		}
	}
	consumed := make(map[uint64]bool, len(frame.ConsumedSignals))
	for i, sequence := range frame.ConsumedSignals {
		if sequence == 0 || i > 0 && sequence <= frame.ConsumedSignals[i-1] {
			return fmt.Errorf("invalid checkpoint consumed signal identities")
		}
		consumed[sequence] = true
	}
	for i, signal := range frame.PendingSignals {
		if signal.Sequence == 0 || signal.Sequence > frame.SignalCursor || consumed[signal.Sequence] ||
			i > 0 && signal.Sequence <= frame.PendingSignals[i-1].Sequence || identity.ValidateToken(signal.Name) != nil {
			return fmt.Errorf("invalid checkpoint pending signal identities")
		}
	}
	for i, step := range frame.CancelledTimers {
		if step%2 != 0 || step >= frame.StepPosition || i > 0 && step <= frame.CancelledTimers[i-1] {
			return fmt.Errorf("invalid checkpoint cancelled timer identities")
		}
	}
	if frame.PanicAttempts > frame.Anchor.Index {
		return fmt.Errorf("checkpoint attempts exceed anchor")
	}
	return nil
}
