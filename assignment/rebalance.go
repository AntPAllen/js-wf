package assignment

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"js-wf/provision"
)

// Move describes one desired ownership change. Revision is filled by Rebalance
// from its retained assignment read; Plan only computes owners.
type Move struct {
	Partition uint32
	From      string
	To        string
	Revision  uint64
}

// Plan balances the 64 partitions across a supplied live membership snapshot.
// Members are sorted; the first remainder members receive one extra partition.
// Existing owners keep their lowest numbered partitions up to their quota. Only
// absent owners and excess partitions move, minimizing moves for these quotas.
// It does not mutate its inputs or infer whether a worker is alive.
func Plan(members []string, current map[uint32]string) ([]Move, error) {
	if len(members) == 0 || len(members) > int(provision.Partitions) {
		return nil, fmt.Errorf("worker count must be between 1 and %d", provision.Partitions)
	}
	members = append([]string(nil), members...)
	sort.Strings(members)
	quota := make(map[string]int, len(members))
	for i, owner := range members {
		if err := validOwner(owner); err != nil {
			return nil, err
		}
		if _, exists := quota[owner]; exists {
			return nil, fmt.Errorf("duplicate worker ID %q", owner)
		}
		quota[owner] = int(provision.Partitions) / len(members)
		if i < int(provision.Partitions)%len(members) {
			quota[owner]++
		}
	}
	for p, owner := range current {
		if _, err := key(p); err != nil {
			return nil, err
		}
		if owner != "" {
			if err := validOwner(owner); err != nil {
				return nil, err
			}
		}
	}
	kept := make(map[string]int, len(members))
	var available []uint32
	for p := uint32(0); p < provision.Partitions; p++ {
		owner := current[p]
		if kept[owner] < quota[owner] {
			kept[owner]++
		} else {
			available = append(available, p)
		}
	}
	var moves []Move
	next := 0
	for _, owner := range members {
		for kept[owner] < quota[owner] {
			p := available[next]
			next++
			moves = append(moves, Move{Partition: p, From: current[p], To: owner})
			kept[owner]++
		}
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Partition < moves[j].Partition })
	return moves, nil
}

// RebalancePort uses retained revisions to protect against concurrent moves.
// Store implements this port; a membership controller supplies the live set.
type RebalancePort interface {
	GetLatest(context.Context, uint32) (string, uint64, error)
	Assign(context.Context, uint32, string, uint64) (uint64, error)
}

type RebalanceResult struct {
	Planned   []Move
	Moved     int
	Conflicts int
}

// Rebalance applies one bounded pass. Conflicting assignments are left for the
// next pass; an uncertain write returns its error and can be resolved by reread
// on the next call. A dry run performs all reads without changing ownership.
func Rebalance(ctx context.Context, port RebalancePort, members []string, dryRun bool) (RebalanceResult, error) {
	var result RebalanceResult
	// Validate membership before issuing any transport calls.
	if _, err := Plan(members, nil); err != nil {
		return result, err
	}
	if port == nil {
		return result, fmt.Errorf("nil assignment port")
	}
	current := make(map[uint32]string, provision.Partitions)
	revisions := make(map[uint32]uint64, provision.Partitions)
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, revision, err := port.GetLatest(ctx, p)
		if err != nil {
			return result, err
		}
		current[p], revisions[p] = owner, revision
	}
	moves, err := Plan(members, current)
	if err != nil {
		return result, err
	}
	for i := range moves {
		moves[i].Revision = revisions[moves[i].Partition]
	}
	result.Planned = moves
	if dryRun {
		return result, nil
	}
	for _, move := range moves {
		if _, err := port.Assign(ctx, move.Partition, move.To, move.Revision); err != nil {
			if errors.Is(err, ErrConflict) {
				result.Conflicts++
				continue
			}
			return result, err
		}
		result.Moved++
	}
	return result, nil
}
