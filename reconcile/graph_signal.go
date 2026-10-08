package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/journal"
)

const CanonicalSignalRepairBatch = 8

type GraphSignalRecoveryPort interface {
	RecoverSignal(context.Context, journal.GraphSignalRequest, string) (uint64, error)
	RepairBoundSignalAttempt(context.Context, journal.GraphSignalBinding) (bool, error)
}

// CanonicalSignalScan uses its own authority-stream cursor/lease. A bounded
// per-generation repair position survives scanner/VM restarts in the root.
// Already-bound wakeup repair is metadata-only and never opens input readers.
type CanonicalSignalScan struct {
	graph    *journal.GraphStore
	recovery GraphSignalRecoveryPort
	Observe  func(RepairEvent)
	cycle    graphCatalogCycle
}

func NewCanonicalSignalScan(js jetstream.JetStream, graph *journal.GraphStore) (*CanonicalSignalScan, error) {
	c, err := client.NewWithGraphJournal(js, graph)
	if err != nil {
		return nil, err
	}
	return NewCanonicalSignalScanWithPort(graph, c)
}
func NewCanonicalSignalScanWithPort(graph *journal.GraphStore, recovery GraphSignalRecoveryPort) (*CanonicalSignalScan, error) {
	if graph == nil || !graph.CanonicalSignals() || recovery == nil {
		return nil, fmt.Errorf("canonical Signal scanner requires configured graph and recovery transport")
	}
	return &CanonicalSignalScan{graph: graph, recovery: recovery}, nil
}
func (s *CanonicalSignalScan) Scan(ctx context.Context, next uint64, budget int, dry bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return result, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	initial, confirmed := next, next
	result.NextSequence = next
	through, err := s.cycle.begin(ctx, s.graph, next)
	if err != nil {
		return result, err
	}
	defer func() {
		s.cycle.end(result, scanErr)
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
	}()
	for i := 0; i < budget; i++ {
		entry, status, err := s.graph.NextStartThrough(ctx, next, through)
		if err != nil {
			return result, err
		}
		if entry == nil {
			result.NextSequence = 1
			return result, nil
		}
		result.Inspected++
		if status != nil && !status.State.Pending && !status.Retired && !status.Purging && status.Kind != journal.Completed && status.Kind != journal.Failed {
			err = s.repair(ctx, status, entry.Sequence, dry, &result)
			if err != nil && (errors.Is(err, journal.ErrStale) || errors.Is(err, client.ErrStaleGeneration) || errors.Is(err, client.ErrPurged) || errors.Is(err, client.ErrNotFound)) {
				fresh, e := s.graph.InspectStartDestination(ctx, entry.Destination)
				if e != nil {
					return result, e
				}
				if fresh == nil || fresh.Retired || fresh.Purging || fresh.Kind == journal.Completed || fresh.Kind == journal.Failed || fresh.State.Start.Token != status.State.Start.Token {
					err = nil
				}
			}
			if err != nil {
				return result, fmt.Errorf("%w: repair canonical Signal: %w", journal.ErrUnknown, err)
			}
		}
		next = entry.Sequence + 1
		result.NextSequence = next
		confirmed = next
	}
	return result, nil
}
func (s *CanonicalSignalScan) repair(ctx context.Context, status *journal.GraphStartStatus, source uint64, dry bool, result *ScanResult) error {
	state := status.State
	r := state.Start.Request
	recovered := false
	event := RepairEvent{Kind: "graph-signal", Type: r.Type, ID: r.ID, SourceSequence: source, InvocationSequence: state.Invocation}
	if status.SignalInputs > status.SignalBindings {
		to := status.SignalRepair
		for count := 0; count < CanonicalSignalRepairBatch && to < status.SignalInputs; count++ {
			input, bound, err := s.graph.InspectSignalRepair(ctx, r.Type, r.ID, state.Invocation, to)
			if err != nil {
				return err
			}
			if bound == nil {
				event.Reason = "reserved_signal"
				if dry {
					result.Candidates = append(result.Candidates, Candidate{Type: r.Type, ID: r.ID, Reason: event.Reason})
					reportRepair(s.Observe, event, true, nil)
				} else {
					_, err = s.recovery.RecoverSignal(ctx, input.Request, input.Token)
					reportRepair(s.Observe, event, false, err)
					if err != nil {
						return err
					}
					result.Reenqueued++
					recovered = true
				}
			}
			to++
		}
		if !dry {
			if err := s.graph.AdvanceSignalRepair(ctx, r.Type, r.ID, state.Invocation, state.Start.Token, status.SignalRepair, to); err != nil {
				return err
			}
		}
	}
	// A successful recovery already enqueued a wakeup covering the entire queue.
	if recovered {
		return nil
	}
	// One wakeup covers every currently ready queue operation for the workflow.
	binding, err := s.graph.InspectReadySignal(ctx, r.Type, r.ID, state.Invocation)
	if err != nil {
		return err
	}
	if binding == nil {
		return nil
	}
	event.Reason = "bound_signal_without_consumption"
	if dry {
		result.Candidates = append(result.Candidates, Candidate{Type: r.Type, ID: r.ID, Reason: event.Reason})
		reportRepair(s.Observe, event, true, nil)
		return nil
	}
	repaired, err := s.recovery.RepairBoundSignalAttempt(ctx, *binding)
	reportRepair(s.Observe, event, false, err)
	if err != nil {
		return err
	}
	if repaired {
		result.Reenqueued++
	}
	return nil
}
