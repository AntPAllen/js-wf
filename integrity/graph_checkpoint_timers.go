package integrity

import (
	"encoding/json"
	"fmt"
	"time"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

type auditedSDKTimer struct {
	name, domain     string
	deadline         time.Time
	cancelled, fired bool
}

// These declarations are decoded independently of SDK replay and timer helpers.
type auditedTimerCase struct {
	Kind     string    `json:"kind"`
	Name     string    `json:"name"`
	Step     uint64    `json:"timer_step"`
	Deadline time.Time `json:"fire_at"`
	Domain   string    `json:"clock_domain"`
}

func observeSDKTimerOperation(s *auditedGraphJournal, entry journal.Entry) error {
	if entry.Kind != journal.StepCompleted {
		return nil
	}
	var req struct {
		auditedTimerCase
		TimerName string             `json:"timer_name"`
		Duration  int64              `json:"duration_nanos"`
		Cases     []auditedTimerCase `json:"cases"`
	}
	if json.Unmarshal(s.journal.request, &req) != nil {
		return fmt.Errorf("invalid SDK timer request")
	}
	switch req.Kind {
	case "timer_start", "timer_cancel", "timer_await", "timer_signal_select", "select_many":
	default:
		return nil
	}
	var done struct {
		Cancelled bool   `json:"cancelled"`
		Selected  string `json:"selected"`
		Sequence  uint64 `json:"signal_seq"`
		CaseIndex *int   `json:"case_index"`
	}
	if json.Unmarshal(entry.Payload, &done) != nil {
		return fmt.Errorf("invalid SDK timer completion")
	}
	if req.Kind == "timer_start" {
		if s.sdkPosition == 0 || (s.sdkPosition-1)%2 != 0 || identity.ValidateToken(req.Name) != nil || req.Duration > 0 && req.Deadline.IsZero() {
			return fmt.Errorf("invalid SDK timer creation")
		}
		step := s.sdkPosition - 1 // Request was already counted; completion is not yet counted.
		if _, exists := s.sdkTimers[step]; exists {
			return fmt.Errorf("duplicate SDK timer creation")
		}
		if s.sdkTimers == nil {
			s.sdkTimers = map[uint64]auditedSDKTimer{}
		}
		s.sdkTimers[step] = auditedSDKTimer{name: req.Name, domain: req.Domain, deadline: req.Deadline}
		return nil
	}
	validate := func(c auditedTimerCase, deadline bool) (auditedSDKTimer, error) {
		t, exists := s.sdkTimers[c.Step]
		if !exists || t.name != c.Name || t.cancelled || t.fired || deadline && (!t.deadline.Equal(c.Deadline) || t.domain != c.Domain) {
			return t, fmt.Errorf("SDK timer action lacks matching live creation")
		}
		return t, nil
	}
	if req.Kind == "select_many" {
		for _, c := range req.Cases {
			if c.Kind == "timer" {
				if _, err := validate(c, true); err != nil {
					return err
				}
			}
		}
		if done.CaseIndex == nil || *done.CaseIndex < 0 || *done.CaseIndex >= len(req.Cases) {
			return fmt.Errorf("invalid SDK timer selected case")
		}
		c := req.Cases[*done.CaseIndex]
		if c.Kind != "timer" {
			return nil
		}
		if done.Sequence != 0 {
			return fmt.Errorf("SDK timer selection carries signal")
		}
		t := s.sdkTimers[c.Step]
		t.fired = true
		s.sdkTimers[c.Step] = t
		return nil
	}
	c := req.auditedTimerCase
	if req.Kind == "timer_signal_select" {
		c.Name = req.TimerName
	}
	t, err := validate(c, req.Kind != "timer_cancel")
	if err != nil {
		return err
	}
	switch req.Kind {
	case "timer_cancel":
		if !done.Cancelled {
			return fmt.Errorf("SDK timer cancellation not confirmed")
		}
		t.cancelled = true
	case "timer_await":
		t.fired = true
	case "timer_signal_select":
		if done.Selected == "signal" {
			return nil
		}
		if done.Selected != "timer" || done.Sequence != 0 {
			return fmt.Errorf("invalid SDK timer branch")
		}
		t.fired = true
	}
	s.sdkTimers[c.Step] = t
	return nil
}

func auditSDKCheckpointTimers(s *auditedGraphJournal, frame checkpoint.Frame) error {
	expected := map[uint64]bool{}
	for step, t := range s.sdkTimers {
		if !t.cancelled && !t.fired {
			return fmt.Errorf("SDK checkpoint retains live timer")
		}
		if t.cancelled {
			expected[step] = true
		}
	}
	if len(expected) != len(frame.CancelledTimers) {
		return fmt.Errorf("SDK checkpoint cancelled timer census differs")
	}
	for _, step := range frame.CancelledTimers {
		if !expected[step] {
			return fmt.Errorf("SDK checkpoint fabricated or duplicate cancelled timer")
		}
		delete(expected, step)
	}
	return nil
}
