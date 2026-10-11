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

// Reconstruct the SDK state map from completed state operations, independently
// of wf.Context/replay and worker checkpoint restoration. The journal already
// establishes the unique outstanding request paired with this completion.
func auditSDKStateOperation(ctx context.Context, graph GraphReferenceSnapshot, state *auditedGraphJournal, entry journal.Entry, edges []retainedgraph.Link) error {
	if entry.Kind != journal.StepCompleted {
		return nil
	}
	var request struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if json.Unmarshal(state.journal.request, &request) != nil {
		return fmt.Errorf("invalid SDK state request")
	}
	if request.Kind != "state_set" && request.Kind != "state_get" {
		return nil
	}
	if identity.ValidateToken(request.Name) != nil || !graphAuditHash(request.InputHash) {
		return fmt.Errorf("invalid SDK state operation identity")
	}
	var completion struct {
		Result json.RawMessage `json:"result"`
		Ref    string          `json:"result_ref"`
		Hash   string          `json:"result_hash"`
		Error  string          `json:"error"`
	}
	if json.Unmarshal(entry.Payload, &completion) != nil {
		return fmt.Errorf("invalid SDK state completion")
	}
	if completion.Error != "" {
		return nil
	} // Failed operations do not mutate SDK state.
	raw := completion.Result
	if completion.Ref != "" {
		if len(raw) != 0 || !graphAuditHash(completion.Hash) || completion.Ref != "step-result-"+completion.Hash {
			return fmt.Errorf("invalid SDK state result reference")
		}
		found := false
		for _, edge := range edges {
			if edge.Hash != completion.Hash {
				continue
			}
			var err error
			raw, err = graph.LoadObject(ctx, edge.Reference.Object, graph.PayloadLimit)
			if err != nil {
				return err
			}
			if len(raw) > graph.PayloadLimit || digest(raw) != completion.Hash {
				return fmt.Errorf("SDK state result bytes differ")
			}
			found = true
			break
		}
		if !found {
			return fmt.Errorf("SDK state result lacks completion-owned bytes")
		}
	} else if completion.Hash != "" {
		return fmt.Errorf("SDK state result hash without reference")
	}
	var value struct {
		Found bool            `json:"found"`
		Value json.RawMessage `json:"value"`
	}
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' || checkpoint.DecodeUnambiguous(raw, &value) != nil || value.Found && !json.Valid(value.Value) || !value.Found && len(value.Value) != 0 {
		return fmt.Errorf("invalid SDK state result value")
	}
	if request.Kind == "state_set" {
		if !value.Found || digest(value.Value) != request.InputHash {
			return fmt.Errorf("SDK state write input/result differs")
		}
		if state.sdkState == nil {
			state.sdkState = map[string]json.RawMessage{}
		}
		state.sdkState[request.Name] = bytes.Clone(value.Value)
	} else {
		prior, found := state.sdkState[request.Name]
		if request.InputHash != digest([]byte("null")) || found != value.Found || !bytes.Equal(prior, value.Value) {
			return fmt.Errorf("SDK state read differs from preceding writes")
		}
	}
	return nil
}

func auditSDKCheckpointState(expected, actual map[string]json.RawMessage) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("SDK checkpoint state census differs from history")
	}
	for key, value := range expected {
		got, ok := actual[key]
		if !ok || !bytes.Equal(value, got) {
			return fmt.Errorf("SDK checkpoint state %s differs from history", key)
		}
	}
	return nil
}
