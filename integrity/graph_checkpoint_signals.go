package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

// SignalConsumed records worker delivery, not SDK use. Reconstruct SDK use
// from the matching completion, without calling SDK replay or selection code.
func observeSDKSignalSelection(state *auditedGraphJournal, entry journal.Entry) error {
	if entry.Kind != journal.StepCompleted {
		return nil
	}
	var request struct {
		Kind  string `json:"kind"`
		Name  string `json:"name"`
		Cases []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"cases"`
	}
	if json.Unmarshal(state.journal.request, &request) != nil {
		return fmt.Errorf("invalid SDK signal request")
	}
	if request.Kind != "signal" && request.Kind != "call" && request.Kind != "timer_signal_select" && request.Kind != "select_many" {
		return nil
	}
	var done struct {
		Sequence  uint64 `json:"signal_seq"`
		Selected  string `json:"selected"`
		CaseIndex *int   `json:"case_index"`
	}
	if json.Unmarshal(entry.Payload, &done) != nil {
		return fmt.Errorf("invalid SDK signal completion")
	}
	name := request.Name
	switch request.Kind {
	case "timer_signal_select":
		if done.Selected == "timer" && done.Sequence == 0 {
			return nil
		}
		if done.Selected != "signal" {
			return fmt.Errorf("invalid SDK timer/signal branch")
		}
	case "select_many":
		if done.CaseIndex == nil || *done.CaseIndex < 0 || *done.CaseIndex >= len(request.Cases) {
			return fmt.Errorf("invalid SDK signal selected case")
		}
		selected := request.Cases[*done.CaseIndex]
		if selected.Kind == "timer" && done.Sequence == 0 {
			return nil
		}
		if selected.Kind != "signal" && selected.Kind != "promise" {
			return fmt.Errorf("invalid SDK selected signal kind")
		}
		if selected.Kind == "promise" && done.Sequence == 0 {
			return nil
		} // Promise history verifies cache reuse.
		name = selected.Name
	}
	if identity.ValidateToken(name) != nil || done.Sequence == 0 {
		return fmt.Errorf("invalid SDK selected signal identity")
	}
	signal, found := state.sdkSignals[done.Sequence]
	if !found || signal.event.Name != name || state.sdkUsedSignals[done.Sequence] {
		return fmt.Errorf("SDK selected signal missing, mismatched or reused")
	}
	// Each signal wait selects the oldest unconsumed arrival of its name.
	// Select case readiness/clock ordering is a separate history audit.
	position := state.sdkArrivalPositions[name]
	arrivals := state.sdkArrivalQueue[name]
	if position >= len(arrivals) || arrivals[position] != done.Sequence {
		return fmt.Errorf("SDK selected signal skipped an earlier arrival")
	}
	if state.sdkArrivalPositions == nil {
		state.sdkArrivalPositions = map[string]int{}
	}
	state.sdkArrivalPositions[name] = position + 1
	if state.sdkUsedSignals == nil {
		state.sdkUsedSignals = map[uint64]bool{}
	}
	state.sdkUsedSignals[done.Sequence] = true
	return nil
}

func auditSDKCheckpointSignals(state *auditedGraphJournal, frame checkpoint.Frame) error {
	if frame.SignalCursor != state.journal.lastSignal {
		return fmt.Errorf("SDK checkpoint signal cursor differs from prefix")
	}
	if len(frame.ConsumedSignals) != len(state.sdkUsedSignals) {
		return fmt.Errorf("SDK checkpoint consumed signal census differs")
	}
	consumed := map[uint64]bool{}
	for _, seq := range frame.ConsumedSignals {
		if !state.sdkUsedSignals[seq] || consumed[seq] {
			return fmt.Errorf("SDK checkpoint fabricated consumed signal")
		}
		consumed[seq] = true
	}
	if len(frame.PendingSignals) != len(state.sdkSignals)-len(state.sdkUsedSignals) {
		return fmt.Errorf("SDK checkpoint pending signal census differs")
	}
	seen := map[uint64]bool{}
	for _, pending := range frame.PendingSignals {
		original, found := state.sdkSignals[pending.Sequence]
		if !found || seen[pending.Sequence] || state.sdkUsedSignals[pending.Sequence] || original.event.Name != pending.Name {
			return fmt.Errorf("SDK checkpoint pending signal identity differs")
		}
		seen[pending.Sequence] = true
		event := original.event
		if event.Ref != "" {
			if digest(pending.Payload) != event.Hash {
				return fmt.Errorf("SDK checkpoint pending signal bytes differ")
			}
		} else if !bytes.Equal(pending.Payload, event.Payload) {
			return fmt.Errorf("SDK checkpoint pending inline signal differs")
		}
	}
	return nil
}
