package assignment

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestPlanBalancedMinimalAndDeterministic(t *testing.T) {
	random := rand.New(rand.NewSource(42))
	for count := 1; count <= int(provision.Partitions); count++ {
		members := make([]string, count)
		for i := range members {
			members[i] = fmt.Sprintf("worker-%02d", i)
		}
		current := map[uint32]string{}
		before := map[string]int{}
		for p := uint32(0); p < provision.Partitions; p++ {
			owner := fmt.Sprintf("worker-%02d", random.Intn(count+3))
			current[p] = owner
			before[owner]++
		}
		original := map[uint32]string{}
		for p, owner := range current {
			original[p] = owner
		}
		moves, err := Plan(members, current)
		if err != nil {
			t.Fatal(err)
		}
		reversed := append([]string(nil), members...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		again, err := Plan(reversed, current)
		if err != nil || !reflect.DeepEqual(moves, again) || !reflect.DeepEqual(current, original) {
			t.Fatalf("unstable or mutated plan count=%d err=%v", count, err)
		}
		keptBound := 0
		for i, owner := range members {
			quota := int(provision.Partitions) / count
			if i < int(provision.Partitions)%count {
				quota++
			}
			keep := before[owner]
			if keep > quota {
				keep = quota
			}
			keptBound += keep
		}
		if len(moves) != int(provision.Partitions)-keptBound {
			t.Fatalf("count=%d moves=%d minimum=%d", count, len(moves), int(provision.Partitions)-keptBound)
		}
		for _, move := range moves {
			if move.From != current[move.Partition] || move.To == move.From {
				t.Fatal("invalid move", move)
			}
			current[move.Partition] = move.To
		}
		counts := map[string]int{}
		for _, owner := range current {
			counts[owner]++
		}
		for i, owner := range members {
			quota := int(provision.Partitions) / count
			if i < int(provision.Partitions)%count {
				quota++
			}
			if counts[owner] != quota {
				t.Fatalf("unbalanced %d %s=%d want=%d", count, owner, counts[owner], quota)
			}
		}
		if moves, err := Plan(members, current); err != nil || len(moves) != 0 {
			t.Fatalf("converged plan changed: %v %v", moves, err)
		}
	}
	for _, members := range [][]string{nil, {"a", "a"}, {""}, {" a"}} {
		if _, err := Plan(members, nil); err == nil {
			t.Fatalf("invalid members accepted %v", members)
		}
	}
	if _, err := Plan([]string{"a"}, map[uint32]string{64: "a"}); err == nil {
		t.Fatal("invalid partition accepted")
	}
}

type rebalanceFaultPort struct {
	*Store
	fired   bool
	loseAck bool
}

func (p *rebalanceFaultPort) Assign(ctx context.Context, partition uint32, owner string, revision uint64) (uint64, error) {
	if !p.fired {
		p.fired = true
		if p.loseAck {
			r, err := p.Store.Assign(ctx, partition, owner, revision)
			if err != nil {
				return r, err
			}
			return 0, context.DeadlineExceeded
		}
		if _, err := p.Store.Assign(ctx, partition, "concurrent-owner", revision); err != nil {
			return 0, err
		}
	}
	return p.Store.Assign(ctx, partition, owner, revision)
}

func TestRebalanceRealClusterRevisionConflictAndUnknownRecovery(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	for {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, readyErr := js.AccountInfo(attempt)
		done()
		if readyErr == nil {
			attempt, done = context.WithTimeout(ctx, 2*time.Second)
			_, readyErr = js.CreateOrUpdateKeyValue(attempt, jetstream.KeyValueConfig{Bucket: "WF_ASSIGN", History: 1, Storage: jetstream.FileStorage, Replicas: 3})
			done()
		}
		if readyErr == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("assignment bucket readiness: %v", readyErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	store, err := New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.InitializeStatic(ctx, []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	members := []string{"a", "c", "d", "e"}
	dry, err := Rebalance(ctx, store, members, true)
	if err != nil || len(dry.Planned) == 0 || dry.Moved != 0 {
		t.Fatalf("dry=%+v err=%v", dry, err)
	}
	for _, move := range dry.Planned {
		owner, revision, err := store.GetLatest(ctx, move.Partition)
		if err != nil || owner != move.From || revision != move.Revision {
			t.Fatalf("dry mutated ownership %v %s %d %v", move, owner, revision, err)
		}
	}
	result, err := Rebalance(ctx, &rebalanceFaultPort{Store: store}, members, false)
	if err != nil || result.Conflicts != 1 || result.Moved != len(result.Planned)-1 {
		t.Fatalf("conflict result=%+v err=%v", result, err)
	}
	owner, _, err := store.GetLatest(ctx, result.Planned[0].Partition)
	if err != nil || owner != "concurrent-owner" {
		t.Fatalf("concurrent owner overwritten %s %v", owner, err)
	}
	if _, err := Rebalance(ctx, store, members, false); err != nil {
		t.Fatal(err)
	}
	if result, err := Rebalance(ctx, store, members, false); err != nil || len(result.Planned) != 0 {
		t.Fatalf("not converged %+v %v", result, err)
	}
	// A committed move with a hidden acknowledgment must be discovered rather
	// than reversed or blindly repeated during the next pass.
	result, err = Rebalance(ctx, &rebalanceFaultPort{Store: store, loseAck: true}, []string{"a", "e"}, false)
	if !errors.Is(err, context.DeadlineExceeded) || len(result.Planned) == 0 {
		t.Fatalf("unknown=%+v err=%v", result, err)
	}
	first := result.Planned[0]
	owner, revision, err := store.GetLatest(ctx, first.Partition)
	if err != nil || owner != first.To || revision <= first.Revision {
		t.Fatalf("hidden move absent %s %d %v", owner, revision, err)
	}
	if _, err := Rebalance(ctx, store, []string{"a", "e"}, false); err != nil {
		t.Fatal(err)
	}
	_, after, err := store.GetLatest(ctx, first.Partition)
	if err != nil || after != revision {
		t.Fatalf("committed move repeated %d/%d %v", after, revision, err)
	}
	counts := map[string]int{}
	for p := uint32(0); p < provision.Partitions; p++ {
		owner, _, err := store.GetLatest(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		counts[owner]++
	}
	if !reflect.DeepEqual(counts, map[string]int{"a": 32, "e": 32}) {
		t.Fatalf("unbalanced owners %v", counts)
	}
	_, oldRevision, err := store.GetLatest(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.kv.Delete(ctx, "p00", jetstream.LastRevision(oldRevision)); err != nil {
		t.Fatal(err)
	}
	result, err = Rebalance(ctx, store, []string{"a", "e"}, false)
	if err != nil || result.Moved != 1 || result.Conflicts != 0 {
		t.Fatalf("deleted assignment recovery=%+v err=%v", result, err)
	}
}
