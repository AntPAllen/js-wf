package sim

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"js-wf/assignment"
	"js-wf/lease"
	"js-wf/provision"
)

func runPausedCoordinatorClaim(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("paused_coordinator_claim"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	cut, err := schedule.Choose([]string{"before_renew", "after_renew"})
	if err != nil {
		return trace, err
	}
	selected, err := schedule.Choose([]string{"0", "31", "63"})
	if err != nil {
		return trace, err
	}
	partition, err := strconv.Atoi(selected)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	registry := NewKVTransport(schedule, provision.LeaseTTL)
	retained := modeledAssignments{NewKVTransport(schedule, 0)}
	for p := uint32(0); p < provision.Partitions; p++ {
		if _, err := retained.Assign(ctx, p, "c", 0); err != nil {
			return trace, err
		}
	}
	paused := &pausedCoordinatorAssignments{RebalancePort: retained, cut: cut, claim: true, partition: uint32(partition)}
	members := assignment.NewMembershipWithPort(registry)
	a, err := members.Controller(ctx, "a", paused)
	if err != nil {
		return trace, err
	}
	c, err := members.Controller(ctx, "c", retained)
	if err != nil {
		return trace, err
	}
	paused.hook = func() error {
		schedule.RecordTransport(TransportEvent{Operation: "pause_coordinator_claim", Outcome: cut, Sequence: uint64(partition), AtMillis: schedule.NowMillis()})
		for range 4 {
			if err := schedule.AdvanceMillis(3000); err != nil {
				return err
			}
			if err := c.Step(ctx); err != nil {
				return err
			}
		}
		for p := uint32(0); p < provision.Partitions; p++ {
			owner, _, err := retained.GetLatest(ctx, p)
			if err != nil || owner != "c" {
				return fmt.Errorf("claim successor partition %d=%s: %v", p, owner, err)
			}
		}
		paused.afterTakeover = true
		return nil
	}
	want := lease.ErrLost
	attempts := 0
	if cut == "after_renew" {
		want = assignment.ErrConflict
		attempts = 1
	}
	if err := a.Step(ctx); !errors.Is(err, want) {
		return trace, fmt.Errorf("paused claim error=%v want=%v", err, want)
	}
	if paused.postAttempts != attempts || paused.postWrites != 0 {
		return trace, fmt.Errorf("expired claim continued: attempts=%d want=%d writes=%d", paused.postAttempts, attempts, paused.postWrites)
	}
	// Both error paths stop before balancing; cleanup must preserve the successor.
	before := retained.revision
	a.Close()
	if err := c.Step(ctx); err != nil {
		return trace, err
	}
	if retained.revision != before {
		return trace, fmt.Errorf("old claim cleanup changed successor revisions")
	}
	live, err := members.Live(ctx)
	if err != nil || !reflect.DeepEqual(live, []string{"c"}) {
		return trace, fmt.Errorf("claim membership=%v: %v", live, err)
	}
	c.Close()
	schedule.RecordTransport(TransportEvent{Operation: "check_paused_coordinator_claim", Outcome: cut, Sequence: uint64(attempts), AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}
