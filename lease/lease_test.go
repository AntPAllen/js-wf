package lease

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func TestAcquireReclaimsOnlyStaleUninitializedLease(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{kv: kv}
	key := identity.Key("work", "orphan")
	initial, _ := json.Marshal(Value{Worker: "crashed"})
	created, err := kv.Create(ctx, key, initial)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, "work", "orphan", "survivor"); !errors.Is(err, ErrHeld) {
		t.Fatalf("fresh initialization must remain held: %v", err)
	}
	entry, err := kv.Get(ctx, key)
	if err != nil || entry.Revision() != created {
		t.Fatalf("fresh lease changed: entry=%v err=%v", entry, err)
	}
	time.Sleep(1100 * time.Millisecond)
	l, err := store.Acquire(ctx, "work", "orphan", "survivor")
	if err != nil {
		t.Fatalf("reclaim abandoned initialization: %v", err)
	}
	if l.Epoch() <= created {
		t.Fatalf("fencing epoch did not advance: old=%d new=%d", created, l.Epoch())
	}
	entry, err = kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var value Value
	if err := json.Unmarshal(entry.Value(), &value); err != nil || value.Worker != "survivor" || value.Epoch != l.Epoch() {
		t.Fatalf("reclaimed lease: value=%+v err=%v", value, err)
	}
	if err := l.Release(ctx); err != nil {
		t.Fatal(err)
	}
	l, err = store.Acquire(ctx, "work", "orphan", "survivor")
	if err != nil {
		t.Fatal(err)
	}
	current, _ := json.Marshal(Value{Worker: "survivor", Epoch: l.Epoch()})
	if _, err := kv.Update(ctx, key, current, l.revision); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("stale release revision: %v", err)
	}
	if err := l.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup after uncertain renewal: %v", err)
	}
	if _, err := kv.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("cleanup left lease behind: %v", err)
	}
	l, err = store.Acquire(ctx, "work", "orphan", "survivor")
	if err != nil {
		t.Fatal(err)
	}
	successor, _ := json.Marshal(Value{Worker: "replacement", Epoch: l.Epoch() + 1})
	if _, err := kv.Update(ctx, key, successor, l.revision); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("superseded lease release: %v", err)
	}
	if err := l.Cleanup(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("cleanup should preserve successor: %v", err)
	}
	entry, err = kv.Get(ctx, key)
	if err != nil || string(entry.Value()) != string(successor) {
		t.Fatalf("successor changed: entry=%v err=%v", entry, err)
	}
}
