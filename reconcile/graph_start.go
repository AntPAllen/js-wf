package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/journal"
)

type GraphStartRecoveryPort interface {
	RecoverStartAttempt(context.Context, string, string, string) (client.Handle, error)
}

// CanonicalStartScan repairs pending input before WF_INV exists, and bound
// starts whose runtime journal is still empty. Cursor sequences belong to the
// authority stream, not WF_INV. Use a separate graph-start loop cursor/lease.
type CanonicalStartScan struct {
	graph    *journal.GraphStore
	recovery GraphStartRecoveryPort
	Observe  func(RepairEvent)
}

func NewCanonicalStartScan(js jetstream.JetStream, graph *journal.GraphStore) (*CanonicalStartScan, error) {
	c, err := client.NewWithGraphJournal(js, graph)
	if err != nil {
		return nil, err
	}
	return NewCanonicalStartScanWithPort(graph, c)
}

func NewCanonicalStartScanWithPort(graph *journal.GraphStore, recovery GraphStartRecoveryPort) (*CanonicalStartScan, error) {
	if graph == nil || !graph.CanonicalStarts() || recovery == nil {
		return nil, fmt.Errorf("canonical start scanner requires configured graph and recovery transport")
	}
	return &CanonicalStartScan{graph: graph, recovery: recovery}, nil
}

func (s *CanonicalStartScan) Scan(ctx context.Context, next uint64, budget int, dry bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return result, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	initial, confirmed := next, next
	result.NextSequence = next
	defer func() {
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
	}()
	for i := 0; i < budget; i++ {
		entry, status, err := s.graph.NextStart(ctx, next)
		if err != nil {
			return result, err
		}
		if entry == nil {
			result.NextSequence = 1
			return result, nil
		}
		result.Inspected++
		if status != nil && !status.Retired && !status.Purging && status.JournalCount == 0 {
			state := status.State
			r := state.Start.Request
			reason := "bound_without_history"
			if state.Pending {
				reason = "pending_start"
			}
			event := RepairEvent{Kind: "graph-start", Type: r.Type, ID: r.ID, Reason: reason, SourceSequence: entry.Sequence, InvocationSequence: state.Invocation}
			if dry {
				result.Candidates = append(result.Candidates, Candidate{Type: r.Type, ID: r.ID, Reason: reason})
				reportRepair(s.Observe, event, true, nil)
			} else {
				h, e := s.recovery.RecoverStartAttempt(ctx, r.Type, r.ID, state.Start.Token)
				event.InvocationSequence = h.InvSeq
				recovered := e == nil
				if e != nil && (errors.Is(e, client.ErrStaleGeneration) || errors.Is(e, journal.ErrStale) || errors.Is(e, client.ErrNotFound)) {
					// Retirement/replacement is only a safe skip after a fresh witness.
					current, check := s.graph.InspectStartDestination(ctx, entry.Destination)
					if check != nil {
						return result, check
					}
					if current == nil || current.Retired || current.Purging || current.State.Start.Token != state.Start.Token {
						event.Reason = "generation_changed"
						event.Outcome = "superseded"
						event.At = time.Now().UTC()
						if s.Observe != nil {
							s.Observe(event)
						}
						next = entry.Sequence + 1
						result.NextSequence = next
						confirmed = next
						continue
					}
				}
				reportRepair(s.Observe, event, false, e)
				if e != nil {
					return result, fmt.Errorf("%w: recover start: %w", journal.ErrUnknown, e)
				}
				if recovered {
					result.Reenqueued++
				}
			}
		}
		next = entry.Sequence + 1
		result.NextSequence = next
		confirmed = next
	}
	return result, nil
}
