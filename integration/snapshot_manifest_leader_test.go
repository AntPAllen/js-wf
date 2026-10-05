package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type staleSnapshotManifestJS struct {
	jetstream.JetStream
	key  string
	mode string
	old  jetstream.KeyValueEntry
	gets atomic.Int64
}

func (j *staleSnapshotManifestJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	state, err := j.JetStream.KeyValue(ctx, bucket)
	if err != nil || bucket != "WF_STATE" {
		return state, err
	}
	return &staleSnapshotManifestKV{KeyValue: state, owner: j}, nil
}

type staleSnapshotManifestKV struct {
	jetstream.KeyValue
	owner *staleSnapshotManifestJS
}

func (kv *staleSnapshotManifestKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if key == kv.owner.key {
		kv.owner.gets.Add(1)
		switch kv.owner.mode {
		case "absent":
			return nil, jetstream.ErrKeyNotFound
		case "old":
			return kv.owner.old, nil
		}
	}
	return kv.KeyValue.Get(ctx, key)
}

// The journal and both successive compactions are real. Only the weak KV Get
// response is injected; leader reads are native administrative requests.
func TestSnapshotManifestReadsUseLeader(t *testing.T) {
	parent := os.Getenv("WF_SNAPSHOT_LEADER_ROOT")
	if parent == "" {
		t.Skip("set WF_SNAPSHOT_LEADER_ROOT to retain native regression stores")
	}
	root := filepath.Join(parent, t.Name())
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.Start(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := setupCluster(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var mu sync.Mutex
	var requests []string
	traced, err := jetstream.New(cluster.Clients[1], jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) { mu.Lock(); requests = append(requests, subject); mu.Unlock() }}))
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "manifest-leader"
	key := "snap." + identity.Key(typ, id)
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	stateStream, err := all[0].Stream(ctx, "KV_WF_STATE")
	if err != nil || !stateStream.CachedInfo().Config.AllowDirect {
		t.Fatalf("direct-enabled state: %v", err)
	}
	store := journal.New(all[0])
	var tail uint64
	appendTo := func(start, end uint64) {
		t.Helper()
		for index := start; index < end; index++ {
			kind := journal.StepCompleted
			if index == 0 {
				kind = journal.Started
			}
			tail, err = store.Append(ctx, typ, id, journal.Entry{Epoch: 1, Index: index, Kind: kind}, tail)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	appendTo(0, 120)
	if _, err := store.SnapshotPrefix(ctx, typ, id, 16); err != nil {
		t.Fatal(err)
	}
	old, err := state.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	appendTo(120, 200)
	latest, err := store.SnapshotPrefix(ctx, typ, id, 16)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := state.Get(ctx, key)
	if err != nil || committed.Revision() <= old.Revision() {
		t.Fatalf("successive manifests: %v", err)
	}
	for _, mode := range []string{"absent", "old"} {
		t.Run(mode, func(t *testing.T) {
			wrapped := &staleSnapshotManifestJS{JetStream: traced, key: key, mode: mode, old: old}
			mu.Lock()
			requests = nil
			mu.Unlock()
			records, gotTail, err := journal.New(wrapped).Read(ctx, typ, id)
			if err != nil || len(records) != 200 || gotTail != tail {
				t.Fatalf("compacted read under stale %s manifest: records=%d tail=%d want=%d err=%v", mode, len(records), gotTail, tail, err)
			}
			for index, record := range records {
				if record.Index != uint64(index) {
					t.Fatalf("index %d: %+v", index, record)
				}
			}
			value, err := journal.NewSnapshotPort(wrapped).GetManifestRevision(ctx, key)
			if err != nil || value.Revision != committed.Revision() || !bytes.Equal(value.Value, committed.Value()) {
				t.Fatalf("exact committed manifest revision: revision=%d want=%d err=%v", value.Revision, committed.Revision(), err)
			}
			if wrapped.gets.Load() != 0 {
				t.Fatalf("weak KV Get invoked %d times", wrapped.gets.Load())
			}
			mu.Lock()
			leaders, direct := 0, 0
			for _, subject := range requests {
				if subject == "$JS.API.STREAM.MSG.GET.KV_WF_STATE" {
					leaders++
				}
				if len(subject) >= len("$JS.API.DIRECT.GET.KV_WF_STATE") && subject[:len("$JS.API.DIRECT.GET.KV_WF_STATE")] == "$JS.API.DIRECT.GET.KV_WF_STATE" {
					direct++
				}
			}
			mu.Unlock()
			if leaders < 2 || direct != 0 {
				t.Fatalf("manifest wire route: leader=%d direct=%d", leaders, direct)
			}
			t.Logf("200 exact records; committed manifest revision=%d leader requests=%d direct requests=%d", value.Revision, leaders, direct)
		})
	}
	t.Run("object_absence", func(t *testing.T) {
		wrapped := &absentResultJS{JetStream: traced}
		wrapped.remaining.Store(1)
		mu.Lock()
		requests = nil
		mu.Unlock()
		got, err := journal.NewSnapshotPort(wrapped).GetObject(ctx, latest.Object)
		if err != nil || len(got) == 0 || wrapped.calls.Load() != 2 {
			t.Fatalf("snapshot object after weak absence: bytes=%d calls=%d err=%v", len(got), wrapped.calls.Load(), err)
		}
		mu.Lock()
		defer mu.Unlock()
		leaders := 0
		for _, subject := range requests {
			if subject == "$JS.API.STREAM.MSG.GET.OBJ_WF_BLOB" {
				leaders++
			}
		}
		if leaders != 1 {
			t.Fatalf("snapshot object oracle requests=%d", leaders)
		}
	})
	t.Run("deleted_and_purged", func(t *testing.T) {
		port := journal.NewSnapshotPort(traced)
		if err := state.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		if _, err := port.GetManifestRevision(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("DEL: %v", err)
		}
		if _, err := state.Put(ctx, key, committed.Value()); err != nil {
			t.Fatal(err)
		}
		if err := state.Purge(ctx, key); err != nil {
			t.Fatal(err)
		}
		if _, err := port.GetManifestRevision(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("PURGE: %v", err)
		}
	})
	t.Run("malformed_manifest", func(t *testing.T) {
		if _, err := state.Put(ctx, key, []byte(`{bad`)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := journal.New(traced).Read(ctx, typ, id); !errors.Is(err, journal.ErrGap) {
			t.Fatalf("malformed manifest: %v", err)
		}
	})
	t.Run("blocked_leader_deadline_cancel", func(t *testing.T) {
		const prefix = "$JS.SNAPSHOTCHECK.API"
		options := traced.Options()
		options.APIPrefix = prefix
		options.Domain = ""
		routed := resultOracleRoute{JetStream: traced, options: options}
		requests := make(chan struct{}, 4)
		sub, err := cluster.Clients[1].Subscribe(prefix+".STREAM.MSG.GET.KV_WF_STATE", func(*nats.Msg) { requests <- struct{}{} })
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if err := cluster.Clients[1].FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		port := journal.NewSnapshotPort(routed)
		attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
		started := time.Now()
		_, err = port.GetManifestRevision(attempt, key)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
			t.Fatalf("leader deadline: %v", err)
		}
		select {
		case <-requests:
		default:
			t.Fatal("missing administrative oracle request")
		}
		attempt, stop = context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { _, err := port.GetManifestRevision(attempt, key); done <- err }()
		select {
		case <-requests:
		case <-time.After(time.Second):
			stop()
			t.Fatal("cancellation did not reach leader request")
		}
		stop()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("leader cancel: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("leader ignored cancellation")
		}
	})
}
