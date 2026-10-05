package lease

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

type countedHeldNativePort struct {
	KVPort
	creates, gets, updates, deletes int
	last                            KVEntry
}

func (p *countedHeldNativePort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	p.creates++
	return p.KVPort.Create(ctx, key, value)
}
func (p *countedHeldNativePort) Get(ctx context.Context, key string) (KVEntry, error) {
	p.gets++
	value, err := p.KVPort.Get(ctx, key)
	p.last = value
	return value, err
}
func (p *countedHeldNativePort) Update(ctx context.Context, key string, value []byte, rev uint64) (uint64, error) {
	p.updates++
	return p.KVPort.Update(ctx, key, value, rev)
}
func (p *countedHeldNativePort) Delete(ctx context.Context, key string, rev uint64) error {
	p.deletes++
	return p.KVPort.Delete(ctx, key, rev)
}

func TestHeldObservationNativeRenewalMetadata(t *testing.T) {
	root := os.Getenv("WF_HELD_LEASE_NATIVE_ROOT")
	if root == "" {
		root = t.TempDir()
	} else {
		if !filepath.IsAbs(root) {
			t.Fatal("native root must be absolute")
		}
		if err := os.Mkdir(root, 0755); err != nil {
			t.Fatal(err)
		}
	}
	cluster, err := testcluster.Start(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	// This diagnostic needs only the production lease bucket, not every
	// workflow stream. Bound readiness calls inside the original deadline.
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
			t.Fatalf("lease bucket readiness: %v", readyErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	store := NewWithKeyValue(kv)
	owner, err := store.Acquire(ctx, "test", "held", "prior")
	if err != nil {
		t.Fatal(err)
	}
	port := &countedHeldNativePort{KVPort: jetStreamKVPort{kv: kv}}
	contender := NewWithKVPort(port)
	var observations []HeldObservation
	for i := 0; i < 2; i++ {
		_, err := contender.AcquireWithHeldObserver(ctx, "test", "held", "next", func(o HeldObservation) { observations = append(observations, o) })
		if err != ErrHeld || len(observations) != i+1 {
			t.Fatalf("held result: %v observations=%v", err, observations)
		}
		o := observations[i]
		if !o.EntryObserved || !o.ValueValid || o.Reason != "held_entry" || o.Worker != "prior" || o.Epoch != owner.Epoch() || o.Revision != port.last.Revision || !o.Created.Equal(port.last.Created) || o.ObservedAt.IsZero() {
			t.Fatalf("native metadata disagrees with actual read: %+v entry=%+v", o, port.last)
		}
		if i == 0 {
			if err := owner.Renew(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if observations[1].Revision <= observations[0].Revision || !observations[1].Created.After(observations[0].Created) {
		t.Fatalf("renewal did not change observed version/time: %+v", observations)
	}
	if port.creates != 2 || port.gets != 2 || port.updates != 0 || port.deletes != 0 {
		t.Fatalf("observer broker requests=%+v", port)
	}
	stream, err := js.Stream(ctx, "KV_WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	info := stream.CachedInfo()
	if info.Config.Replicas != 3 || info.Config.Storage != jetstream.FileStorage || info.Config.MaxAge != provision.LeaseTTL {
		t.Fatalf("wrong native lease configuration: %+v", info.Config)
	}
	ids := []string{}
	for _, server := range cluster.Servers {
		ids = append(ids, server.ID())
	}
	proof := struct {
		Observations                    []HeldObservation `json:"observations"`
		Creates, Gets, Updates, Deletes int
		ServerIDs                       []string              `json:"server_ids"`
		Stream                          *jetstream.StreamInfo `json:"stream"`
	}{observations, port.creates, port.gets, port.updates, port.deletes, ids, info}
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "held-observations.json"), append(data, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("native held observations=%+v requests=create:%d get:%d update:%d delete:%d", observations, port.creates, port.gets, port.updates, port.deletes)
}
