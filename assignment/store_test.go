package assignment

import (
	"context"
	"errors"
	"testing"
	"time"

	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func TestInitializeStaticPreservesMoves(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	s, err := New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	owners := []string{"a", "b", "c"}
	if err := s.InitializeStatic(ctx, owners); err != nil {
		t.Fatal(err)
	}
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, rev, err := s.Get(ctx, p)
		if err != nil || rev == 0 || owner != owners[int(p)%len(owners)] {
			t.Fatalf("partition %d owner=%q revision=%d err=%v", p, owner, rev, err)
		}
	}
	oldOwner, oldRevision, err := s.Get(ctx, 0)
	if err != nil || oldOwner != "a" {
		t.Fatalf("partition 0 owner=%q err=%v", oldOwner, err)
	}
	newRevision, err := s.Assign(ctx, 0, "b", oldRevision)
	if err != nil || newRevision <= oldRevision {
		t.Fatalf("move revision=%d err=%v", newRevision, err)
	}
	if _, err := s.Assign(ctx, 0, "c", oldRevision); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update: %v", err)
	}
	if err := s.InitializeStatic(ctx, owners); err != nil {
		t.Fatal(err)
	}
	owner, revision, err := s.Get(ctx, 0)
	if err != nil || owner != "b" || revision != newRevision {
		t.Fatalf("restart owner=%q revision=%d err=%v", owner, revision, err)
	}
	if err := s.kv.Delete(ctx, "p00", jetstream.LastRevision(newRevision)); err != nil {
		t.Fatal(err)
	}
	owner, revision, err = s.GetLatest(ctx, 0)
	if err != nil || owner != "" || revision <= newRevision {
		t.Fatalf("deleted owner=%q revision=%d err=%v", owner, revision, err)
	}
}
