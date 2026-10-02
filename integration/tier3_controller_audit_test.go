//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/journal"
	"js-wf/worker"
)

type matrixControllerTimerCall struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	FirstCall   time.Time     `json:"first_call"`
	FirstReturn time.Time     `json:"first_return"`
	Duration    time.Duration `json:"duration_ns"`
}

type matrixControllerAudit struct {
	Bounds  []matrixControllerAppendBound `json:"bounds"`
	Samples []matrixLatencySample         `json:"samples"`
	Clock   string                        `json:"clock"`
}

func auditMatrixControllerLatencies(ctx context.Context, js jetstream.JetStream, operations []worker.OperationEvent, receipts []matrixControllerJournalReceipt, calls []client.Operation, timers []matrixControllerTimerCall, deadline time.Time) (matrixControllerAudit, error) {
	proof := matrixControllerAudit{Clock: "unshifted-controller-conservative-bounds"}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return proof, err
	}
	info, err := inv.Info(ctx)
	if err != nil {
		return proof, err
	}
	records := make(map[string][]journal.Record)
	bounds := make(map[string][]matrixControllerAppendBound)
	starts := make(map[string]time.Time)
	signals := make(map[uint64]time.Time)
	timerByName := make(map[string]matrixControllerTimerCall)
	for _, timer := range timers {
		timerByName[timer.ID+"/"+timer.Name] = timer
	}
	for _, call := range calls {
		var args struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		}
		var result struct {
			SignalSequence uint64 `json:"signal_seq"`
		}
		if err := json.Unmarshal(call.Args, &args); err != nil {
			return proof, err
		}
		if err := json.Unmarshal(call.Result, &result); err != nil {
			return proof, err
		}
		key := args.Type + "/" + args.ID
		if call.Op == "start" && (starts[key].IsZero() || call.InvokeTS.Before(starts[key])) {
			starts[key] = call.InvokeTS
		}
		if call.Op == "signal" && result.SignalSequence > 0 && (signals[result.SignalSequence].IsZero() || call.InvokeTS.Before(signals[result.SignalSequence])) {
			signals[result.SignalSequence] = call.InvokeTS
		}
	}
	operationByInvocation := make(map[string][]worker.OperationEvent)
	for _, event := range operations {
		key := event.Type + "/" + event.ID
		operationByInvocation[key] = append(operationByInvocation[key], event)
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		msg, err := inv.GetMsg(ctx, seq)
		if err != nil {
			return proof, err
		}
		parts := strings.Split(msg.Subject, ".")
		if len(parts) != 4 {
			return proof, fmt.Errorf("invalid invocation subject")
		}
		typ, id := parts[2], parts[3]
		key := typ + "/" + id
		retained, _, err := journal.New(js).Read(ctx, typ, id)
		if err != nil {
			return proof, err
		}
		received, err := matrixControllerReceiptTimes(typ, id, retained, receipts)
		if err != nil {
			return proof, err
		}
		windows, err := matrixControllerAppendBounds(typ, id, retained, operationByInvocation[key], received)
		if err != nil {
			return proof, err
		}
		if len(windows) == 0 {
			return proof, fmt.Errorf("empty controller journal %s", key)
		}
		records[key], bounds[key] = retained, windows
		proof.Bounds = append(proof.Bounds, windows...)
	}
	// A parent's durable child request precedes child Start/enqueue. Its call
	// start is therefore a conservative lower bound for child enqueue time.
	for key, entries := range records {
		for i, entry := range entries {
			if entry.Kind != journal.StepRequested {
				continue
			}
			var request struct {
				ChildType string `json:"child_type"`
				ChildID   string `json:"child_id"`
			}
			if err := json.Unmarshal(entry.Payload, &request); err != nil {
				return proof, err
			}
			if request.ChildID != "" {
				child := request.ChildType + "/" + request.ChildID
				at := bounds[key][i].Before
				if starts[child].IsZero() || at.Before(starts[child]) {
					starts[child] = at
				}
			}
		}
	}
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		entries := records[key]
		parts := strings.SplitN(key, "/", 2)
		typ, id := parts[0], parts[1]
		windows := bounds[key]
		enabled := starts[key]
		sample, err := matrixControllerProgress(typ, id, "start", enabled, 0, windows)
		if err != nil {
			return proof, err
		}
		proof.Samples = append(proof.Samples, sample)
		add := func(event string, at time.Time, after uint64) error {
			sample, err := matrixControllerProgress(typ, id, event, at, after, windows)
			if err != nil {
				return err
			}
			proof.Samples = append(proof.Samples, sample)
			if at.After(enabled) {
				enabled = at
			}
			return nil
		}
		for i, entry := range entries {
			if entry.Kind == journal.StepRequested {
				var request struct {
					Kind        string    `json:"kind"`
					Name        string    `json:"name"`
					Duration    int64     `json:"duration_nanos"`
					FireAt      time.Time `json:"fire_at"`
					ClockDomain string    `json:"clock_domain,omitempty"`
					ChildType   string    `json:"child_type"`
					ChildID     string    `json:"child_id"`
				}
				if err := json.Unmarshal(entry.Payload, &request); err != nil {
					return proof, err
				}
				if request.Kind == "timer" {
					timer, ok := timerByName[id+"/"+request.Name]
					if !ok || timer.FirstCall.IsZero() || timer.FirstReturn.IsZero() || int64(timer.Duration) != request.Duration {
						return proof, fmt.Errorf("missing controller timer call %s/%s", key, request.Name)
					}
					origin, originErr := matrixTimerOrigin(operationByInvocation[key], typ, id, entry.WorkerID, entry.Index, timer.Duration, request.FireAt, request.ClockDomain)
					if originErr != nil {
						return proof, originErr
					}
					if origin == nil || origin.Duration < 0 {
						return proof, fmt.Errorf("missing successful controller timer clock origin %s/%s", key, request.Name)
					}
					due := origin.At.Add(-origin.Duration).Add(timer.Duration)
					latestDue := origin.At.Add(timer.Duration)
					completed := false
					completionSequence := uint64(0)
					for j := i + 1; j < len(entries); j++ {
						if entries[j].Kind == journal.StepCompleted {
							if windows[j].Before.Before(latestDue) || timer.FirstReturn.Before(latestDue) {
								return proof, fmt.Errorf("controller timer completed early %s/%s", key, request.Name)
							}
							completed = true
							completionSequence = entries[j].Sequence
							break
						}
					}
					if !completed {
						return proof, fmt.Errorf("missing controller timer completion")
					}
					if err := add("timer_due", due, completionSequence-1); err != nil {
						return proof, err
					}
				}
				if request.ChildID != "" {
					child := bounds[request.ChildType+"/"+request.ChildID]
					if len(child) == 0 || child[len(child)-1].Kind != journal.Completed {
						return proof, fmt.Errorf("missing child terminal controller evidence")
					}
					terminal := child[len(child)-1]
					if err := add("child_completed", terminal.Before, terminal.Sequence); err != nil {
						return proof, err
					}
				}
			}
			if entry.Kind == journal.SignalConsumed {
				var signal struct {
					Sequence uint64 `json:"sig_seq"`
				}
				if err := json.Unmarshal(entry.Payload, &signal); err != nil {
					return proof, err
				}
				at := signals[signal.Sequence]
				if at.IsZero() {
					// Runtime child notifications lack an observed external SDK call.
					// Invocation creation precedes every notification to that invocation;
					// use that earlier bound rather than estimating a broker-clock offset.
					at = starts[key]
				}
				if err := add("signal_sent", at, entry.Sequence-1); err != nil {
					return proof, err
				}
			}
		}
		terminal := windows[len(windows)-1]
		if terminal.Kind != journal.Completed || terminal.After.After(deadline) || terminal.After.Before(enabled) {
			return proof, fmt.Errorf("invalid terminal controller window %s", key)
		}
		proof.Samples = append(proof.Samples, matrixLatencySample{Type: typ, ID: id, Event: "terminal", Enabled: enabled, Observed: terminal.After, ObservedLower: &terminal.Before, Delay: terminal.After.UTC().Sub(enabled.UTC())})
	}
	return proof, nil
}
