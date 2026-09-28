package history

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"js-wf/client"

	"github.com/anishathalye/porcupine"
)

type resultInput struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type resultOutput struct {
	Status     string `json:"status"`
	InvSeq     uint64 `json:"inv_seq"`
	ResultHash string `json:"result_hash"`
}

type resultState struct {
	InvSeq     uint64
	Status     string
	ResultHash string
}

// CheckResults checks that completed and failed results are immutable within
// one invocation generation. A purge retires that generation; a later result
// may belong to a newer generation after the ID is reused. The model allows
// unobserved completion and purge events between calls.
func CheckResults(operations []client.Operation, timeout time.Duration) (porcupine.CheckResult, error) {
	var converted []porcupine.Operation
	for index, operation := range operations {
		if operation.Op != "getResult" {
			continue
		}
		var input resultInput
		var output resultOutput
		if err := json.Unmarshal(operation.Args, &input); err != nil {
			return porcupine.Illegal, fmt.Errorf("operation %d args: %w", index, err)
		}
		if err := json.Unmarshal(operation.Result, &output); err != nil {
			return porcupine.Illegal, fmt.Errorf("operation %d result: %w", index, err)
		}
		if input.Type == "" || input.ID == "" || operation.InvokeTS.IsZero() || operation.ReturnTS.Before(operation.InvokeTS) {
			return porcupine.Illegal, fmt.Errorf("operation %d has invalid result arguments or timestamps", index)
		}
		switch output.Status {
		case "completed", "failed":
			if output.InvSeq == 0 || output.ResultHash == "" {
				return porcupine.Illegal, fmt.Errorf("operation %d has incomplete terminal result", index)
			}
		case "purged":
			if output.InvSeq == 0 || output.ResultHash != "" {
				return porcupine.Illegal, fmt.Errorf("operation %d has invalid purge result", index)
			}
		case "not_found":
			if output.InvSeq != 0 || output.ResultHash != "" {
				return porcupine.Illegal, fmt.Errorf("operation %d has invalid not-found result", index)
			}
		default:
			return porcupine.Unknown, fmt.Errorf("operation %d has unsupported result outcome %q", index, output.Status)
		}
		converted = append(converted, porcupine.Operation{Input: input, Output: output, Call: operation.InvokeTS.UnixNano(), Return: operation.ReturnTS.UnixNano()})
	}
	if len(converted) == 0 {
		return porcupine.Unknown, fmt.Errorf("no getResult operations")
	}
	model := porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			for _, operation := range history {
				input := operation.Input.(resultInput)
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
		Init: func() interface{} { return resultState{} },
		Step: func(rawState, _, rawOutput interface{}) (bool, interface{}) {
			state := rawState.(resultState)
			output := rawOutput.(resultOutput)
			if output.Status == "not_found" {
				return true, state
			}
			if output.InvSeq < state.InvSeq {
				return false, nil
			}
			if output.InvSeq > state.InvSeq {
				state = resultState{InvSeq: output.InvSeq}
			}
			if output.Status == "purged" {
				state.Status = "purged"
				state.ResultHash = ""
				return true, state
			}
			if state.Status == "purged" || state.Status != "" && (state.Status != output.Status || state.ResultHash != output.ResultHash) {
				return false, nil
			}
			state.Status = output.Status
			state.ResultHash = output.ResultHash
			return true, state
		},
	}
	return porcupine.CheckOperationsTimeout(model, converted, timeout), nil
}
