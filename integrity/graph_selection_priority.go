package integrity

import (
	"encoding/json"
	"fmt"

	"js-wf/identity"
	"js-wf/journal"
)

// Run before selection observers consume signals or materialize promises.
// Positive-deadline timers need delivery clock evidence which is not retained
// here. Only readiness proved by this prefix can rule out a later choice.
func auditSDKSelectionPriority(s *auditedGraphJournal, entry journal.Entry) error {
	if entry.Kind != journal.StepCompleted {
		return nil
	}
	var req auditedStepRequest
	if json.Unmarshal(s.journal.request, &req) != nil {
		return fmt.Errorf("invalid selection request")
	}
	if req.Kind != "select_many" && req.Kind != "timer_signal_select" {
		return nil
	}
	var done auditedStepCompletion
	if json.Unmarshal(entry.Payload, &done) != nil {
		return fmt.Errorf("invalid selection completion")
	}
	signalReady := func(name string) bool { return s.sdkArrivalPositions[name] < len(s.sdkArrivalQueue[name]) }
	if req.Kind == "timer_signal_select" {
		if done.Selected == "timer" && signalReady(req.Name) {
			return fmt.Errorf("timer branch skipped buffered signal")
		}
		return nil
	}
	if done.CaseIndex == nil || *done.CaseIndex < 0 || *done.CaseIndex >= len(req.Cases) {
		return fmt.Errorf("invalid selection case index")
	}
	for i, c := range req.Cases {
		if identity.ValidateToken(c.Name) != nil {
			return fmt.Errorf("invalid selection case name")
		}
		ready := false
		switch c.Kind {
		case "signal":
			ready = signalReady(c.Name)
		case "promise":
			if (c.ChildType != "" || c.ChildID != "") && identity.Validate(c.ChildType, c.ChildID) != nil {
				return fmt.Errorf("invalid selection child identity")
			}
			// Ambiguous AwaitSignal/AwaitPromise candidates do not prove a cache exists.
			ready = len(s.requiredPromises[c.Name]) > 0 || signalReady(c.Name)
		case "timer":
			timer, found := s.sdkTimers[c.TimerStep]
			ready = found && timer.name == c.Name && !timer.cancelled && !timer.fired && timer.deadline.IsZero() && c.FireAt.IsZero() && timer.domain == c.ClockDomain
		default:
			return fmt.Errorf("invalid selection case kind")
		}
		if i < *done.CaseIndex && ready {
			return fmt.Errorf("selection skipped earlier ready case")
		}
	}
	return nil
}
