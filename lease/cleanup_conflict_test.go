package lease

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

type cleanupRealConflictPort struct {
	KVPort
	conflicts  int
	successor  bool
	deletes    int
	staleReads int
	initial    KVEntry
}

func (p *cleanupRealConflictPort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	revision, err := p.KVPort.Create(ctx, key, value)
	if err == nil {
		p.initial = KVEntry{Value: append([]byte(nil), value...), Revision: revision}
	}
	return revision, err
}
func (p *cleanupRealConflictPort) Get(ctx context.Context, key string) (KVEntry, error) {
	if p.staleReads > 0 {
		p.staleReads--
		return p.initial, nil
	}
	return p.KVPort.Get(ctx, key)
}

func (p *cleanupRealConflictPort) Delete(ctx context.Context, key string, revision uint64) error {
	p.deletes++
	if p.conflicts > 0 {
		p.conflicts--
		entry, err := p.KVPort.Get(ctx, key)
		if err != nil {
			return err
		}
		value := entry.Value
		if p.successor {
			var current Value
			if err := json.Unmarshal(value, &current); err != nil {
				return err
			}
			current.Epoch++
			value, err = json.Marshal(current)
			if err != nil {
				return err
			}
		}
		if _, err := p.KVPort.Update(ctx, key, value, entry.Revision); err != nil {
			return err
		}
	}
	return p.KVPort.Delete(ctx, key, revision)
}

func TestCleanupRereadsIdentityAfterRealRevisionConflict(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	var kv jetstream.KeyValue
	for {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, readyErr := js.AccountInfo(attempt)
		done()
		if readyErr == nil {
			attempt, done = context.WithTimeout(ctx, 2*time.Second)
			kv, readyErr = js.CreateOrUpdateKeyValue(attempt, jetstream.KeyValueConfig{Bucket: "WF_LEASE", History: 1, TTL: provision.LeaseTTL, Storage: jetstream.FileStorage, Replicas: 3})
			done()
		}
		if readyErr == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("replicated lease bucket readiness: %v", readyErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, mode := range []string{"same-owner", "same-worker-new-epoch", "persistent-same-owner", "stale-initialization", "persistent-stale-initialization", "stale-predecessor"} {
		t.Run(mode, func(t *testing.T) {
			port := &cleanupRealConflictPort{KVPort: jetStreamKVPort{kv: kv}}
			var predecessor KVEntry
			if mode == "stale-predecessor" {
				prior, err := NewWithKVPort(port).Acquire(ctx, "test", mode, "predecessor")
				if err != nil {
					t.Fatal(err)
				}
				predecessor, err = port.KVPort.Get(ctx, prior.key)
				if err != nil {
					t.Fatal(err)
				}
				if err := prior.Release(ctx); err != nil {
					t.Fatal(err)
				}
				port.deletes = 0
			}
			owner, err := NewWithKVPort(port).Acquire(ctx, "test", mode, "worker")
			if err != nil {
				t.Fatal(err)
			}
			port.conflicts = 1
			port.successor = mode == "same-worker-new-epoch"
			if mode == "persistent-same-owner" {
				port.conflicts = 3
			}
			if mode == "stale-initialization" || mode == "persistent-stale-initialization" || mode == "stale-predecessor" {
				port.conflicts = 0
				port.staleReads = 1
				if mode == "stale-predecessor" {
					port.initial = predecessor
				}
				if mode == "persistent-stale-initialization" {
					port.staleReads = 3
				}
			}
			err = owner.Cleanup(ctx)
			switch mode {
			case "stale-initialization", "stale-predecessor":
				if err != nil || port.deletes != 1 {
					t.Fatalf("stale initialization cleanup: deletes=%d err=%v", port.deletes, err)
				}
			case "persistent-stale-initialization":
				if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, ErrLost) || port.deletes != 0 {
					t.Fatalf("persistent stale initialization: deletes=%d err=%v", port.deletes, err)
				}
				if err := owner.Cleanup(ctx); err != nil {
					t.Fatal(err)
				}
			case "same-owner":
				if err != nil || port.deletes != 2 {
					t.Fatalf("cleanup abandoned same owner: deletes=%d err=%v", port.deletes, err)
				}
			case "same-worker-new-epoch":
				if !errors.Is(err, ErrLost) || port.deletes != 1 {
					t.Fatalf("successor cleanup: deletes=%d err=%v", port.deletes, err)
				}
				entry, err := kv.Get(ctx, owner.key)
				if err != nil {
					t.Fatal(err)
				}
				var value Value
				if err := json.Unmarshal(entry.Value(), &value); err != nil || value.Epoch != owner.Epoch()+1 {
					t.Fatalf("successor damaged: %+v %v", value, err)
				}
				return
			default:
				if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, ErrLost) || port.deletes != 3 {
					t.Fatalf("persistent conflict misclassified: deletes=%d err=%v", port.deletes, err)
				}
				if err := owner.Cleanup(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := kv.Get(ctx, owner.key); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("lease remains: %v", err)
			}
		})
	}
}
