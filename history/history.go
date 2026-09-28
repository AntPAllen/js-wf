// Package history records SDK operations and checks the write-once start
// contract against concurrent client observations.
package history

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"js-wf/client"

	"github.com/anishathalye/porcupine"
)

type Recorder struct {
	mu         sync.Mutex
	operations []client.Operation
}

func (r *Recorder) Record(operation client.Operation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	operation.Args = append(json.RawMessage(nil), operation.Args...)
	operation.Result = append(json.RawMessage(nil), operation.Result...)
	r.operations = append(r.operations, operation)
}

func (r *Recorder) Snapshot() []client.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]client.Operation, len(r.operations))
	for i, operation := range r.operations {
		operation.Args = append(json.RawMessage(nil), operation.Args...)
		operation.Result = append(json.RawMessage(nil), operation.Result...)
		out[i] = operation
	}
	return out
}

func (r *Recorder) WriteJSONL(w io.Writer) error {
	enc := json.NewEncoder(w)
	for _, operation := range r.Snapshot() {
		if err := enc.Encode(operation); err != nil {
			return err
		}
	}
	return nil
}

type startInput struct {
	Type              string `json:"type"`
	ID                string `json:"id"`
	InputHash         string `json:"input_hash"`
	ParentType        string `json:"parent_type"`
	ParentID          string `json:"parent_id"`
	SignalName        string `json:"signal_name"`
	HasConfirmedStart bool   `json:"-"`
}

type startOutput struct {
	Status string `json:"status"`
	InvSeq uint64 `json:"inv_seq"`
}

type startState struct {
	Stored     bool
	InputHash  string
	ParentType string
	ParentID   string
	SignalName string
	InvSeq     uint64
}

// CheckStarts checks completed Start and StartChild operations. An unknown
// publish may either leave the register empty or commit the supplied input;
// later observations can resolve that uncertainty. Unclassified errors fail
// closed rather than being silently omitted.
func CheckStarts(operations []client.Operation, timeout time.Duration) (porcupine.CheckResult, error) {
	var converted []porcupine.Operation
	for index, operation := range operations {
		if operation.Op != "start" && operation.Op != "start_child" {
			continue
		}
		var input startInput
		var output startOutput
		if err := json.Unmarshal(operation.Args, &input); err != nil {
			return porcupine.Illegal, fmt.Errorf("operation %d args: %w", index, err)
		}
		if err := json.Unmarshal(operation.Result, &output); err != nil {
			return porcupine.Illegal, fmt.Errorf("operation %d result: %w", index, err)
		}
		if input.Type == "" || input.ID == "" || input.InputHash == "" || operation.InvokeTS.IsZero() || operation.ReturnTS.Before(operation.InvokeTS) {
			return porcupine.Illegal, fmt.Errorf("operation %d has invalid identity or timestamps", index)
		}
		switch output.Status {
		case "started", "already_started", "input_mismatch", "enqueue_unknown", "unknown":
		default:
			return porcupine.Unknown, fmt.Errorf("operation %d has unsupported start outcome %q", index, output.Status)
		}
		converted = append(converted, porcupine.Operation{Input: input, Output: output, Call: operation.InvokeTS.UnixNano(), Return: operation.ReturnTS.UnixNano()})
	}
	if len(converted) == 0 {
		return porcupine.Unknown, fmt.Errorf("no start operations")
	}
	confirmed := map[string]bool{}
	for _, operation := range converted {
		input := operation.Input.(startInput)
		output := operation.Output.(startOutput)
		if output.Status == "started" || output.Status == "enqueue_unknown" {
			confirmed[input.Type+"."+input.ID] = true
		}
	}
	for i := range converted {
		input := converted[i].Input.(startInput)
		input.HasConfirmedStart = confirmed[input.Type+"."+input.ID]
		converted[i].Input = input
	}
	model := porcupine.NondeterministicModel{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			for _, operation := range history {
				input := operation.Input.(startInput)
				key := input.Type + "." + input.ID
				byKey[key] = append(byKey[key], operation)
			}
			keys := make([]string, 0, len(byKey))
			for key := range byKey {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			parts := make([][]porcupine.Operation, 0, len(keys))
			for _, key := range keys {
				parts = append(parts, byKey[key])
			}
			return parts
		},
		Init: func() []interface{} { return []interface{}{startState{}} },
		Step: func(rawState, rawInput, rawOutput interface{}) []interface{} {
			state := rawState.(startState)
			input := rawInput.(startInput)
			output := rawOutput.(startOutput)
			matches := state.InputHash == input.InputHash && state.ParentType == input.ParentType && state.ParentID == input.ParentID && state.SignalName == input.SignalName
			switch output.Status {
			case "started", "enqueue_unknown":
				if state.Stored || output.InvSeq == 0 {
					return nil
				}
				return []interface{}{startState{Stored: true, InputHash: input.InputHash, ParentType: input.ParentType, ParentID: input.ParentID, SignalName: input.SignalName, InvSeq: output.InvSeq}}
			case "already_started":
				if output.InvSeq == 0 {
					return nil
				}
				// A successful WF_INV publish can lose its ack. The same
				// Start call then reads its own record and reports
				// already_started, even with no earlier observed call.
				if !state.Stored {
					if input.HasConfirmedStart {
						return nil
					}
					return []interface{}{startState{Stored: true, InputHash: input.InputHash, ParentType: input.ParentType, ParentID: input.ParentID, SignalName: input.SignalName, InvSeq: output.InvSeq}}
				}
				if !matches || state.InvSeq != 0 && output.InvSeq != state.InvSeq {
					return nil
				}
				state.InvSeq = output.InvSeq
				return []interface{}{state}
			case "input_mismatch":
				if !state.Stored || matches || output.InvSeq == 0 || state.InvSeq != 0 && output.InvSeq != state.InvSeq {
					return nil
				}
				state.InvSeq = output.InvSeq
				return []interface{}{state}
			case "unknown":
				if output.InvSeq != 0 {
					return nil
				}
				if state.Stored {
					return []interface{}{state}
				}
				committed := startState{Stored: true, InputHash: input.InputHash, ParentType: input.ParentType, ParentID: input.ParentID, SignalName: input.SignalName}
				return []interface{}{state, committed}
			default:
				return nil
			}
		},
	}
	return porcupine.CheckOperationsTimeout(model.ToModel(), converted, timeout), nil
}
