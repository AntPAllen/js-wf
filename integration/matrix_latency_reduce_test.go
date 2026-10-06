//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"js-wf/journal"
)

// matrixReduceInvocationLatencies evaluates the existing point rules from a
// complete logical journal and its raw server timestamps. Lookups return raw
// child/signal/terminal times; exactly one clock normalization happens here.
// Providers must report missing metadata as an error. No partial samples escape
// an error, and this function neither reads NATS nor modifies its inputs.
func matrixReduceInvocationLatencies(ctx context.Context, typ, id string, enabled, completionDeadline time.Time,
	offset time.Duration, records []journal.Record, rawTimes []time.Time,
	child func(string, string) (time.Time, error), signalTimeAt func(uint64) (time.Time, error),
	terminal func() (time.Time, error)) ([]matrixLatencySample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(records) != len(rawTimes) {
		return nil, fmt.Errorf("%s/%s journal timestamp census differs", typ, id)
	}
	enabled = enabled.Add(-offset)
	times := rawTimes
	if offset != 0 {
		times = make([]time.Time, len(rawTimes))
		for i, at := range rawTimes {
			times[i] = at.Add(-offset)
		}
	}
	var samples []matrixLatencySample
	progress := func(event string, at time.Time) error {
		for _, observed := range times {
			if !observed.Before(at) {
				samples = append(samples, matrixLatencySample{Type: typ, ID: id, Event: event, Enabled: at, Observed: observed, Delay: observed.Sub(at)})
				return nil
			}
		}
		return fmt.Errorf("%s/%s has no journal progress after %s at %s", typ, id, event, at)
	}
	if err := progress("start", enabled); err != nil {
		return nil, err
	}
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if record.Kind == journal.StepRequested {
			var request struct {
				Kind      string    `json:"kind"`
				FireAt    time.Time `json:"fire_at"`
				ChildType string    `json:"child_type"`
				ChildID   string    `json:"child_id"`
			}
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				return nil, err
			}
			if !request.FireAt.IsZero() {
				request.FireAt = request.FireAt.Add(-offset)
				if err := progress("timer_due", request.FireAt); err != nil {
					return nil, err
				}
				if request.Kind == "timer" {
					completed := false
					for j := index + 1; j < len(records); j++ {
						if records[j].Kind == journal.StepCompleted {
							if times[j].Before(request.FireAt) {
								return nil, fmt.Errorf("%s/%s timer completed before deadline", typ, id)
							}
							completed = true
							break
						}
					}
					if !completed {
						return nil, fmt.Errorf("%s/%s timer has no completion", typ, id)
					}
				}
				if request.FireAt.After(enabled) {
					enabled = request.FireAt
				}
			}
			if request.ChildID != "" {
				childTime, err := child(request.ChildType, request.ChildID)
				if err != nil {
					return nil, err
				}
				childTime = childTime.Add(-offset)
				if err := progress("child_completed", childTime); err != nil {
					return nil, err
				}
				if childTime.After(enabled) {
					enabled = childTime
				}
			}
		}
		if record.Kind == journal.SignalConsumed {
			var signal struct {
				Sequence uint64 `json:"sig_seq"`
			}
			if err := json.Unmarshal(record.Payload, &signal); err != nil {
				return nil, err
			}
			signalTime, err := signalTimeAt(signal.Sequence)
			if err != nil {
				return nil, err
			}
			signalTime = signalTime.Add(-offset)
			if times[index].Before(signalTime) {
				return nil, fmt.Errorf("%s/%s signal consumed before publish", typ, id)
			}
			if err := progress("signal_sent", signalTime); err != nil {
				return nil, err
			}
			if signalTime.After(enabled) {
				enabled = signalTime
			}
		}
	}
	terminalTime, err := terminal()
	if err != nil {
		return nil, err
	}
	terminalTime = terminalTime.Add(-offset)
	if terminalTime.After(completionDeadline) {
		return nil, fmt.Errorf("%s/%s completed at %s after post-heal deadline %s", typ, id, terminalTime, completionDeadline)
	}
	if terminalTime.Before(enabled) {
		return nil, fmt.Errorf("%s/%s completed before its enabling event: terminal=%s enabled=%s", typ, id, terminalTime, enabled)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	samples = append(samples, matrixLatencySample{Type: typ, ID: id, Event: "terminal", Enabled: enabled, Observed: terminalTime, Delay: terminalTime.Sub(enabled)})
	for i := range samples {
		samples[i].ServerClockOffset = offset
	}
	return samples, nil
}
