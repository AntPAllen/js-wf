//go:build linux

package integration_test

import (
	"fmt"
	"time"

	"js-wf/journal"
	"js-wf/runtimeclock"
	"js-wf/worker"
)

// Match one actual timer-creation observation, preserving legacy interpretation.
// Domain observations report bounds at return time, advanced monotonically;
// their interval must overlap the unshifted controller's operation bracket.
func matrixTimerOrigin(operations []worker.OperationEvent, typ, id, owner string, index uint64, duration time.Duration, fireAt time.Time, domain string) (*worker.OperationEvent, error) {
	var found *worker.OperationEvent
	for _, op := range operations {
		if op.Type != typ || op.ID != id || op.Worker != owner || op.JournalIndex != index || op.Error != "" || op.At.IsZero() || op.Duration < 0 {
			continue
		}
		if domain == "" {
			if op.Operation != "timer_clock" || op.ServerTime == nil || !op.ServerTime.Add(duration).Equal(fireAt) {
				continue
			}
		} else {
			if domain != runtimeclock.DeadlineDomain || op.Operation != "timer_domain_clock" || op.ClockDomain != domain || op.ClockLower == nil || op.ClockUpper == nil || op.ClockLower.IsZero() || op.ClockUpper.IsZero() || op.ClockLower.After(*op.ClockUpper) || op.ClockLower.After(op.At) || op.ClockUpper.Before(op.At.Add(-op.Duration)) || !op.ClockUpper.Add(duration).Equal(fireAt) {
				continue
			}
		}
		if found != nil {
			return nil, fmt.Errorf("ambiguous timer origin for %s/%s index%d", typ, id, index)
		}
		copy := op
		found = &copy
	}
	return found, nil
}

// A fresh acknowledged publication corroborates the exact submitted hint.
// Duplicate acknowledgments and uncertain publications cannot establish which
// retained native source was stored. Domain and physical-clock proofs are distinct.
func matrixShiftedNativeHint(operations []worker.OperationEvent, id, owner string, index uint64, fireAt time.Time, domain string, offset time.Duration, observed time.Time) (*worker.OperationEvent, error) {
	var found *worker.OperationEvent
	for _, op := range operations {
		if op.Operation != "timer_native_hint" || op.Type != "matrixtimer" || op.ID != id || op.Worker != owner || op.JournalIndex != index || op.JournalKind != journal.StepRequested || op.ClockDomain != domain || op.Error != "" || op.TimerPublished == nil || !*op.TimerPublished || op.At.IsZero() || op.Duration < 0 || op.At.After(observed) || op.ServerTime == nil || op.ServerTime.IsZero() || op.ClockLower == nil || op.ClockUpper == nil || op.ClockLower.IsZero() || op.ClockUpper.IsZero() || op.ClockLower.After(*op.ClockUpper) || op.TimerDeadline == nil || !op.TimerDeadline.Equal(fireAt) || op.TimerScheduleAt == nil {
			continue
		}
		before := op.At.Add(-op.Duration)
		if op.ClockLower.After(op.At) || op.ClockUpper.Before(before) || op.ServerTime.Before(before.Add(offset).Add(-2*time.Second)) || op.ServerTime.After(op.At.Add(offset).Add(2*time.Second)) {
			continue
		}
		remaining := fireAt.Sub(*op.ClockLower)
		if remaining < 0 {
			remaining = 0
		}
		if !op.TimerScheduleAt.Equal(op.ServerTime.Add(remaining)) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("ambiguous acknowledged native hint for %s index%d", id, index)
		}
		copy := op
		found = &copy
	}
	return found, nil
}
