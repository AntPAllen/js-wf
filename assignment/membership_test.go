package assignment

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestMembershipExpiryFencesReusedIDAndVerifiesConfig(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	members, err := EnsureMembership(ctx, js, 1)
	if err != nil {
		t.Fatal(err)
	}
	old, err := members.Register(ctx, "worker.a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := members.Register(ctx, "worker.a"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("duplicate registration: %v", err)
	}
	live, err := members.Live(ctx)
	if err != nil || !reflect.DeepEqual(live, []string{"worker.a"}) {
		t.Fatalf("live %v %v", live, err)
	}
	// Advance through actual server expiry, without worker-time comparisons.
	for ctx.Err() == nil {
		live, err = members.Live(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(live) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("registration did not expire", ctx.Err())
	}
	successor, err := members.Register(ctx, "worker.a")
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Release(ctx)
	if successor.Epoch() <= old.Epoch() {
		t.Fatal("reused membership epoch did not increase")
	}
	if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("expired owner renewed successor: %v", err)
	}
	if err := old.Release(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("expired owner released successor: %v", err)
	}
	live, err = members.Live(ctx)
	if err != nil || !reflect.DeepEqual(live, []string{"worker.a"}) {
		t.Fatalf("successor removed: %v %v", live, err)
	}
	// An operator-created bucket with another expiry must fail closed.
	status, err := members.read.(membershipReadAdapter).kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := status.Config()
	cfg.TTL = time.Minute
	if _, err := js.UpdateKeyValue(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureMembership(ctx, js, 1); err == nil {
		t.Fatal("wrong membership TTL adopted")
	}
}

func TestMembershipControllerBalancesJoinAndGracefulLeave(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WF_ASSIGN", History: 1, Storage: jetstream.FileStorage, Replicas: 1}); err != nil {
		t.Fatal(err)
	}
	owners, err := New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	members, err := EnsureMembership(ctx, js, 1)
	if err != nil {
		t.Fatal(err)
	}
	a, err := members.Controller(ctx, "a", owners)
	if err != nil {
		t.Fatal(err)
	}
	aCtx, stopA := context.WithCancel(ctx)
	defer stopA()
	doneA := make(chan error, 1)
	go func() { doneA <- a.Run(aCtx) }()
	joinedA := false
	defer func() {
		stopA()
		if !joinedA {
			if err := <-doneA; err != nil {
				t.Error(err)
			}
		}
		a.Close()
	}()
	waitOwners := func(want map[string]int) {
		t.Helper()
		for ctx.Err() == nil {
			counts := map[string]int{}
			for p := uint32(0); p < provision.Partitions; p++ {
				owner, _, err := owners.GetLatest(ctx, p)
				if err != nil {
					t.Fatal(err)
				}
				counts[owner]++
			}
			if reflect.DeepEqual(counts, want) {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("membership did not converge", want, ctx.Err())
	}
	waitOwners(map[string]int{"a": 64})
	b, err := members.Controller(ctx, "b", owners)
	if err != nil {
		t.Fatal(err)
	}
	bCtx, stopB := context.WithCancel(ctx)
	doneB := make(chan error, 1)
	go func() { doneB <- b.Run(bCtx) }()
	waitOwners(map[string]int{"a": 32, "b": 32})
	stopB()
	if err := <-doneB; err != nil {
		t.Fatal(err)
	}
	b.Close()
	waitOwners(map[string]int{"a": 64})
	c, err := members.Controller(ctx, "c", owners)
	if err != nil {
		t.Fatal(err)
	}
	cCtx, stopC := context.WithCancel(ctx)
	doneC := make(chan error, 1)
	go func() { doneC <- c.Run(cCtx) }()
	defer func() {
		stopC()
		if err := <-doneC; err != nil {
			t.Error(err)
		}
		c.Close()
	}()
	waitOwners(map[string]int{"a": 32, "c": 32})
	// Stop the coordinator's heartbeat without releasing either registration.
	// A surviving controller must wait for server expiry, acquire a new epoch,
	// and recover all partitions. This is the retained-state crash boundary.
	stopA()
	err = <-doneA
	joinedA = true
	if err != nil {
		t.Fatal(err)
	}
	waitOwners(map[string]int{"c": 64})
	a.Close() // Old cleanup must not remove the successor coordinator lease.
	if _, err := members.leases.Acquire(ctx, "coordinator", "assign", "probe"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("old coordinator cleanup removed successor: %v", err)
	}
}
