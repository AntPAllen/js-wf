package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
)

type GraphTerminalRecoveryPort interface {
	RepairTerminalProjectionAttempt(context.Context, string, string, string, uint64) (bool, error)
}

// TerminalProjectionLookup reports key presence, including purge
// markers. Present bytes are neither terminal authority nor a payload grant.
type TerminalProjectionLookup interface {
	ProjectionPresent(context.Context, string, string) (bool, error)
}

type nativeTerminalProjectionLookup struct{ kv jetstream.KeyValue }

func (p nativeTerminalProjectionLookup) ProjectionPresent(ctx context.Context, typ, id string) (bool, error) {
	_, err := p.kv.Get(ctx, identity.Key(typ, id))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// CanonicalTerminalScan discovers missing projections without caller keys,
// opening payload readers, or publishing state itself. It uses an independent
// graph-terminal authority cursor/lease and delegates owned recovery to workers.
// One loop calls Scan serially; its watermark persists across budgeted calls and
// refreshes after a completed cycle, a cursor change, an error or process restart.
type CanonicalTerminalScan struct {
	graph        *journal.GraphStore
	projection   TerminalProjectionLookup
	recovery     GraphTerminalRecoveryPort
	Observe      func(RepairEvent)
	through      uint64
	haveThrough  bool
	expectedNext uint64
}

func NewCanonicalTerminalScan(ctx context.Context, js jetstream.JetStream, graph *journal.GraphStore) (*CanonicalTerminalScan, error) {
	c, err := client.NewWithGraphJournal(js, graph)
	if err != nil {
		return nil, err
	}
	lookup, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	kv, err := js.KeyValue(lookup, "WF_STATE")
	if err != nil {
		return nil, err
	}
	return NewCanonicalTerminalScanWithPort(graph, nativeTerminalProjectionLookup{kv}, c)
}

func NewCanonicalTerminalScanWithPort(graph *journal.GraphStore, projection TerminalProjectionLookup, recovery GraphTerminalRecoveryPort) (*CanonicalTerminalScan, error) {
	if graph == nil || !graph.CanonicalStarts() || projection == nil || recovery == nil {
		return nil, fmt.Errorf("canonical terminal scanner requires graph, projection lookup and recovery transport")
	}
	return &CanonicalTerminalScan{graph: graph, projection: projection, recovery: recovery}, nil
}

func (s *CanonicalTerminalScan) Scan(ctx context.Context, next uint64, budget int, dry bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return result, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	initial, confirmed := next, next
	result.NextSequence = next
	if !s.haveThrough || next == 1 || next != s.expectedNext {
		through, err := s.graph.StartCatalogHighWater(ctx)
		if err != nil {
			return result, err
		}
		s.through, s.haveThrough = through, true
	}

	defer func() {
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
		s.expectedNext = result.NextSequence
		if scanErr != nil || result.NextSequence == 1 {
			s.haveThrough = false
		}
	}()
	for i := 0; i < budget; i++ {
		entry, status, err := s.graph.NextStartThrough(ctx, next, s.through)
		if err != nil {
			return result, err
		}
		if entry == nil {
			result.NextSequence = 1
			return result, nil
		}
		result.Inspected++
		if status != nil && !status.Retired && !status.Purging && !status.State.Pending && (status.Kind == journal.Completed || status.Kind == journal.Failed) {
			r := status.State.Start.Request
			present, err := s.projection.ProjectionPresent(ctx, r.Type, r.ID)
			if err != nil {
				return result, fmt.Errorf("%w: terminal projection lookup: %w", journal.ErrUnknown, err)
			}
			if !present {
				event := RepairEvent{Kind: "graph-terminal", Type: r.Type, ID: r.ID, Reason: "terminal_without_projection", SourceSequence: entry.Sequence, InvocationSequence: status.State.Invocation}
				if dry {
					result.Candidates = append(result.Candidates, Candidate{Type: r.Type, ID: r.ID, Reason: event.Reason})
					reportRepair(s.Observe, event, true, nil)
				} else {
					repaired, e := s.recovery.RepairTerminalProjectionAttempt(ctx, r.Type, r.ID, status.State.Start.Token, status.State.Invocation)
					if e != nil && (errors.Is(e, journal.ErrStale) || errors.Is(e, client.ErrStaleGeneration) || errors.Is(e, client.ErrPurged) || errors.Is(e, client.ErrNotFound)) {
						fresh, check := s.graph.InspectStartDestination(ctx, entry.Destination)
						if check != nil {
							return result, check
						}
						if fresh == nil || fresh.Retired || fresh.Purging || fresh.State.Pending || fresh.State.Start.Token != status.State.Start.Token || fresh.State.Invocation != status.State.Invocation || (fresh.Kind != journal.Completed && fresh.Kind != journal.Failed) {
							repaired, e = false, nil
							event.Reason = "generation_changed"
						}
					}
					reportRepair(s.Observe, event, false, e)
					if e != nil {
						return result, fmt.Errorf("%w: terminal projection wakeup: %w", journal.ErrUnknown, e)
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
