package journal

import (
	"context"
	"fmt"
	"strings"

	"js-wf/internal/graphpublication"
)

// GraphStartStatus is a quorum-confirmed lifecycle observation, not an input
// ownership grant. Pending recovery and workers validate the retained input;
// bound enqueue repair can check the exact source token without opening input.
type GraphStartStatus struct {
	State          GraphStartState
	JournalCount   uint64
	Retired        bool
	Purging        bool
	Kind           Kind
	SignalInputs   uint64
	SignalBindings uint64
	SignalConsumed uint64
	SignalRepair   uint64
}

// InspectStart observes lifecycle metadata without acquiring an input reader
// or changing the logical head used by prepared runtime appends.
func (s *GraphStore) InspectStart(ctx context.Context, typ, id string) (*GraphStartStatus, error) {
	destination, err := graphDestination(typ, id)
	if err != nil {
		return nil, err
	}
	return s.InspectStartDestination(ctx, destination)
}

func (s *GraphStore) InspectStartDestination(ctx context.Context, destination string) (*GraphStartStatus, error) {
	if !s.cfg.CanonicalStarts || !strings.HasPrefix(destination, "journal/") || !startHex(strings.TrimPrefix(destination, "journal/"), 32) {
		return nil, ErrGap
	}
	root, err := s.cfg.Protocol.Port.ReadRoot(ctx, destination)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if len(root.Application) == 0 {
		_, err = s.validateRoot(root, "", "")
		return nil, err
	}
	var c graphCursor
	if graphDecode(root.Application, &c) != nil || c.Start == nil {
		return nil, ErrGap
	}
	r := c.Start.Request
	expected, err := graphDestination(r.Type, r.ID)
	if err != nil || expected != destination {
		return nil, ErrGap
	}
	validated, err := s.validateRoot(root, r.Type, r.ID)
	if err != nil || validated == nil {
		return nil, ErrGap
	}
	return &GraphStartStatus{State: startState(validated), JournalCount: validated.Count, Retired: validated.Retired, Purging: validated.Purging, Kind: validated.Kind, SignalInputs: validated.SignalInputs, SignalBindings: validated.SignalBindings, SignalConsumed: validated.SignalConsumed, SignalRepair: validated.SignalRepair}, nil
}

// NextStart discovers one retained authority root and confirms its current
// lifecycle. Work is bounded per call; no full catalog or input body is loaded.
func (s *GraphStore) NextStart(ctx context.Context, next uint64) (*graphpublication.RootCatalogEntry, *GraphStartStatus, error) {
	port, ok := s.cfg.Protocol.Port.(graphpublication.RootScanPort)
	if !s.cfg.CanonicalStarts || !ok {
		return nil, nil, ErrGap
	}
	entry, err := port.NextRoot(ctx, next)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if entry == nil {
		return nil, nil, nil
	}
	if entry.Sequence == 0 || entry.Sequence < next || entry.Sequence == ^uint64(0) {
		return nil, nil, ErrGap
	}
	status, err := s.InspectStartDestination(ctx, entry.Destination)
	return entry, status, err
}
