package sim

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"js-wf/assignment"
	"js-wf/provision"
)

type failingCoordinatorClaim struct {
	modeledAssignments
	mode  string
	calls int
}

func (p *failingCoordinatorClaim) GetLatest(ctx context.Context, partition uint32) (string, uint64, error) {
	if partition == 31 && p.mode == "read" {
		return "", 0, ErrTransportLost
	}
	return p.modeledAssignments.GetLatest(ctx, partition)
}
func (p *failingCoordinatorClaim) Assign(ctx context.Context, partition uint32, owner string, revision uint64) (uint64, error) {
	p.calls++
	if partition == 31 {
		switch p.mode {
		case "conflict":
			// A concurrent CAS changes the revision without changing the owner.
			if _, err := p.modeledAssignments.Assign(ctx, partition, "c", revision); err != nil {
				return 0, err
			}
		case "drop", "lost_ack":
			kind := KVDropBeforeCommit
			if p.mode == "lost_ack" {
				kind = KVLoseAckAfterCommit
			}
			if err := p.QueueFault(KVFault{Operation: "update", Kind: kind}); err != nil {
				return 0, err
			}
		}
	}
	return p.modeledAssignments.Assign(ctx, partition, owner, revision)
}

func TestCoordinatorClaimFailsClosed(t *testing.T) {
	for _, mode := range []string{"read", "drop", "lost_ack", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			schedule := NewScheduler(42)
			registry := NewKVTransport(schedule, provision.LeaseTTL)
			retained := modeledAssignments{NewKVTransport(schedule, 0)}
			for p := uint32(0); p < provision.Partitions; p++ {
				if _, err := retained.Assign(ctx, p, "c", 0); err != nil {
					t.Fatal(err)
				}
			}
			failed := &failingCoordinatorClaim{modeledAssignments: retained, mode: mode}
			members := assignment.NewMembershipWithPort(registry)
			a, err := members.Controller(ctx, "a", failed)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			c, err := members.Controller(ctx, "c", retained)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			want := ErrTransportLost
			if mode == "conflict" {
				want = assignment.ErrConflict
			}
			if err := a.Step(ctx); !errors.Is(err, want) {
				t.Fatalf("claim error=%v want=%v", err, want)
			}
			calls := 32
			if mode == "read" {
				calls = 31
			}
			if failed.calls != calls {
				t.Fatalf("claim continued after failure: calls=%d want=%d", failed.calls, calls)
			}
			// No balance move is allowed until the entire claim is acknowledged.
			for p := uint32(0); p < provision.Partitions; p++ {
				owner, rev, err := retained.GetLatest(ctx, p)
				if err != nil || owner != "c" {
					t.Fatalf("partial claim moved partition %d: %s %v", p, owner, err)
				}
				if p > 31 && rev != uint64(p+1) {
					t.Fatal(fmt.Sprintf("claim touched partition after failure: %d/%d", p, rev))
				}
			}
			a.Close()
			if err := c.Step(ctx); err != nil {
				t.Fatalf("successor claim: %v", err)
			}
			before := retained.revision
			if err := c.Step(ctx); err != nil {
				t.Fatal(err)
			}
			if retained.revision != before {
				t.Fatal("stable coordinator repeated claim")
			}
			if err := schedule.Finish(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
