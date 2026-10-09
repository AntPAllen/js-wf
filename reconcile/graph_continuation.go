package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/journal"
)

type GraphContinuationRecoveryPort interface {
	RepairContinuationAttempt(context.Context, string, string, string, uint64, uint64) (bool, error)
}

type CanonicalContinuationScan struct {
	graph      *journal.GraphStore
	scheduling GraphRepairSchedulingPort
	recovery   GraphContinuationRecoveryPort
	cycle      graphCatalogCycle
	Observe    func(RepairEvent)
}

type nativeContinuationScheduling struct{ js jetstream.JetStream }

func (p nativeContinuationScheduling) GraphRepairBlocked(ctx context.Context, typ, id string) (bool, error) {
	return graphRepairBlocked(ctx, p.js, typ, id)
}

func NewCanonicalContinuationScan(js jetstream.JetStream, graph *journal.GraphStore) (*CanonicalContinuationScan, error) {
	c, err := client.NewWithGraphJournal(js, graph)
	if err != nil {
		return nil, err
	}
	return NewCanonicalContinuationScanWithPort(graph, nativeContinuationScheduling{js}, c)
}
func NewCanonicalContinuationScanWithPort(graph *journal.GraphStore, scheduling GraphRepairSchedulingPort, recovery GraphContinuationRecoveryPort) (*CanonicalContinuationScan, error) {
	if graph == nil || !graph.CheckpointIndex() || scheduling == nil || recovery == nil {
		return nil, fmt.Errorf("canonical continuation scan requires v5 checkpoint index, scheduling and recovery ports")
	}
	return &CanonicalContinuationScan{graph: graph, scheduling: scheduling, recovery: recovery}, nil
}

// Scan discovers resume hints through a captured catalog watermark. It never
// opens payload readers or trusts mutable projections. One caller uses Scan
// serially, with the same cursor/lease protocol as the other graph scanners.
func (s *CanonicalContinuationScan) Scan(ctx context.Context, next uint64, budget int, dry bool) (result ScanResult, scanErr error) {
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
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
		s.cycle.end(result, scanErr)
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
		if status.ContinuationReady() {
			recoverySequence := status.ContinuationRecoverySequence()
			request := status.State.Start.Request
			blocked, err := s.scheduling.GraphRepairBlocked(ctx, request.Type, request.ID)
			if err != nil {
				return result, fmt.Errorf("%w: continuation scheduling hint: %w", journal.ErrUnknown, err)
			}
			if !blocked {
				reason := "checkpoint_resume"
				if status.Checkpoint == nil {
					reason = "interrupted_completion"
				}
				event := RepairEvent{Kind: "graph-continuation", Type: request.Type, ID: request.ID, Reason: reason, SourceSequence: entry.Sequence, InvocationSequence: status.State.Invocation, JournalSequence: recoverySequence}
				if dry {
					result.Candidates = append(result.Candidates, Candidate{Type: request.Type, ID: request.ID, Reason: event.Reason})
					reportRepair(s.Observe, event, true, nil)
				} else {
					repaired, err := s.recovery.RepairContinuationAttempt(ctx, request.Type, request.ID, status.State.Start.Token, status.State.Invocation, recoverySequence)
					if err != nil && errors.Is(err, journal.ErrStale) {
						fresh, check := s.graph.InspectStartDestination(ctx, entry.Destination)
						if check != nil {
							return result, check
						}
						if !fresh.ContinuationReady() || fresh.State.Invocation != status.State.Invocation || fresh.State.Start.Token != status.State.Start.Token || fresh.ContinuationRecoverySequence() != recoverySequence {
							repaired, err = false, nil
							event.Reason = "resume_changed"
						}
					}
					reportRepair(s.Observe, event, false, err)
					if err != nil {
						return result, fmt.Errorf("%w: continuation wakeup: %w", journal.ErrUnknown, err)
					}
					if repaired {
						result.Reenqueued++
					}
				}
			}
		}
		next = entry.Sequence + 1
		result.NextSequence = next
		confirmed = next
	}
	return result, nil
}
