package integration_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/lease"
	"js-wf/provision"
)

type heldCoordinatorAssignment struct {
	assignment.RebalancePort
	armed            atomic.Bool
	entered, resumed chan struct{}
	attempts         atomic.Int32
	conflicts        atomic.Int32
}

func (p *heldCoordinatorAssignment) Assign(ctx context.Context, partition uint32, owner string, revision uint64) (uint64, error) {
	if p.armed.CompareAndSwap(true, false) {
		close(p.entered)
		select {
		case <-p.resumed:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		p.attempts.Add(1)
		next, err := p.RebalancePort.Assign(ctx, partition, owner, revision)
		if errors.Is(err, assignment.ErrConflict) {
			p.conflicts.Add(1)
		}
		return next, err
	}
	return p.RebalancePort.Assign(ctx, partition, owner, revision)
}

func TestPausedMembershipCoordinatorCannotOverwriteSuccessor(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 40*time.Second)
	defer stop()
	owners, err := assignment.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	members, err := assignment.EnsureMembership(ctx, all[0], 3)
	if err != nil {
		t.Fatal(err)
	}
	held := &heldCoordinatorAssignment{RebalancePort: owners, entered: make(chan struct{}), resumed: make(chan struct{})}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(held.resumed) }) }
	a, err := members.Controller(ctx, "a", held)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Step(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := members.Controller(ctx, "b", owners)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	c, err := members.Controller(ctx, "c", owners)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := b.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	held.armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- a.Step(ctx) }()
	joined := false
	defer func() {
		resume()
		if !joined {
			<-done
		}
	}()
	select {
	case <-held.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The hold is after the coordinator renewal but before assignment revision CAS.
	// Step is called directly to exercise this boundary beyond Run's 8s pass limit.
	for ctx.Err() == nil {
		if err := c.Step(ctx); err != nil {
			t.Fatal(err)
		}
		owner, _, err := owners.GetLatest(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		if owner == "c" {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	before := make(map[uint32]uint64)
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, revision, err := owners.GetLatest(ctx, p)
		if err != nil || owner != "c" {
			t.Fatalf("takeover partition %d=%s err=%v", p, owner, err)
		}
		before[p] = revision
	}
	resume()
	result := <-done
	joined = true
	if !errors.Is(result, lease.ErrLost) {
		t.Fatalf("paused coordinator not fenced: %v", result)
	}
	if held.attempts.Load() != 1 || held.conflicts.Load() != 1 {
		t.Fatalf("stale assignment attempts=%d conflicts=%d", held.attempts.Load(), held.conflicts.Load())
	}
	a.Close()
	b.Close()
	if err := c.Step(ctx); err != nil {
		t.Fatal(err)
	}
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, revision, err := owners.GetLatest(ctx, p)
		if err != nil || owner != "c" || revision != before[p] {
			t.Fatalf("stale write changed successor partition %d=%s/%d want=%d err=%v", p, owner, revision, before[p], err)
		}
	}
	live, err := members.Live(ctx)
	if err != nil || !reflect.DeepEqual(live, []string{"c"}) {
		t.Fatalf("live membership=%v err=%v", live, err)
	}
}
