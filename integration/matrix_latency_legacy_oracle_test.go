//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"time"
)

// Frozen point oracle from 36f82cf, before latency reduction was extracted.
// Keep this implementation independent of the shared reducer.
func matrixInvocationLatenciesLegacyOracle(ctx context.Context, js jetstream.JetStream, typ, id string, enabled, completionDeadline time.Time, offset time.Duration) ([]matrixLatencySample, error) {
	enabled = enabled.Add(-offset)

	records, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return nil, err
	}
	times := make([]time.Time, len(records))
	for i, record := range records {
		msg, err := stream.GetMsg(ctx, record.Sequence)
		if err != nil {
			return nil, err
		}
		times[i] = msg.Time.Add(-offset)
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
				child, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(request.ChildType, request.ChildID))
				if err != nil {
					return nil, err
				}
				child.Time = child.Time.Add(-offset)
				if err := progress("child_completed", child.Time); err != nil {
					return nil, err
				}
				if child.Time.After(enabled) {
					enabled = child.Time
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
			signals, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				return nil, err
			}
			msg, err := signals.GetMsg(ctx, signal.Sequence)
			if err != nil {
				return nil, err
			}
			msg.Time = msg.Time.Add(-offset)
			if times[index].Before(msg.Time) {
				return nil, fmt.Errorf("%s/%s signal consumed before publish", typ, id)
			}
			if err := progress("signal_sent", msg.Time); err != nil {
				return nil, err
			}
			if msg.Time.After(enabled) {
				enabled = msg.Time
			}
		}
	}
	last, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
	if err != nil {
		return nil, err
	}
	last.Time = last.Time.Add(-offset)
	if last.Time.After(completionDeadline) {
		return nil, fmt.Errorf("%s/%s completed at %s after post-heal deadline %s", typ, id, last.Time, completionDeadline)
	}
	if last.Time.Before(enabled) {
		return nil, fmt.Errorf("%s/%s completed before its enabling event: terminal=%s enabled=%s", typ, id, last.Time, enabled)
	}
	samples = append(samples, matrixLatencySample{Type: typ, ID: id, Event: "terminal", Enabled: enabled, Observed: last.Time, Delay: last.Time.Sub(enabled)})
	for i := range samples {
		samples[i].ServerClockOffset = offset
	}
	return samples, nil
}
