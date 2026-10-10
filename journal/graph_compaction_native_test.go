package journal_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/testcluster"
)

// Rebinding after a server restart retains no process-local ownership
// map. Existing view handles still have to pass canonical reader validation.
type archiveReopenPort struct{ *graphpublication.NativePort }

// Storage is native; the client dispatch/source fixture remains modeled. This
// qualifies the journal relocation lifecycle, not an autonomous native worker.
func TestNativeGraphCheckpointArchiveReopenAndCollection(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d-domain", replicas), func(t *testing.T) {
			cluster, err := testcluster.StartWithDomain(t.TempDir(), replicas, "ARCHIVE")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if replicas > 1 {
				for {
					ready := false
					for _, server := range cluster.Servers {
						ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
					}
					if ready {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			nativeCheckpointArchiveFixture(t, ctx, replicas, func() *nats.Conn { return cluster.Clients[0] }, func() {
				for i := range cluster.Servers {
					cluster.KillNode(i)
				}
				for _, server := range cluster.Servers {
					if server.Running() {
						t.Fatal("server survived store restart cut")
					}
				}
				for i := range cluster.Servers {
					if err := cluster.RestartNode(i); err != nil {
						t.Fatal(err)
					}
				}
				if replicas > 1 {
					for {
						ready := false
						for _, server := range cluster.Servers {
							ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
						}
						if ready {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(20 * time.Millisecond):
						}
					}
				}
			})
			t.Log("NATIVE_CHECKPOINT_ARCHIVE successive_compactions=2 all_servers_gracefully_restarted=true old_reader_preserved=true original_receipt_collected=true full_logical_audit=true raw_chunks_after_retirement=0")
		})
	}
}

// Both restart mechanisms exercise the same native storage and ownership assertions.
func nativeCheckpointArchiveFixture(t *testing.T, ctx context.Context, replicas int, connection func() *nats.Conn, restart func()) {
	t.Helper()
	var trace atomic.Bool
	apiTrace := jetstream.WithClientTrace(&jetstream.ClientTrace{
		RequestSent: func(subject string, body []byte) {
			if trace.Load() {
				t.Logf("ARCHIVE_API_SEND subject=%s request=%s", subject, body)
			}
		},
		ResponseReceived: func(subject string, body []byte, _ nats.Header) {
			if trace.Load() {
				t.Logf("ARCHIVE_API_RESPONSE subject=%s bytes=%d", subject, len(body))
			}
		},
	})
	js, err := jetstream.NewWithDomain(connection(), "ARCHIVE", apiTrace)
	if err != nil {
		t.Fatal(err)
	}
	progressKV, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "ARCHIVE_PROGRESS", Replicas: replicas, Storage: jetstream.FileStorage, MaxValueSize: journal.MaxCompactionCheckpointBytes})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	cfg := journal.NativeGraphConfig{AuthorityStream: "ARCHIVE_AUTH", AuthorityPrefix: "wf.graph.archive", ObjectBucket: "ARCHIVE_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, Now: func() time.Time { return now }, PinTTL: 3 * time.Hour, IntentTTL: time.Second}
	configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range configs {
		if _, err = js.CreateStream(ctx, stream); err != nil {
			t.Fatal(err)
		}
	}
	authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
	if err != nil {
		t.Fatal(err)
	}
	nativePort, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
	if err != nil {
		t.Fatal(err)
	}
	port := &archiveReopenPort{nativePort}
	config := journal.GraphConfig{Protocol: graphpublication.Protocol{Port: port}, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, Now: cfg.Now, PinTTL: cfg.PinTTL, IntentTTL: cfg.IntentTTL, CompactionCheckpoints: journal.NewCompactionCheckpointPort(progressKV)}
	reopen := func() *journal.GraphStore {
		keys, err := port.RootKeys(ctx)
		if err != nil || len(keys) != 1 {
			t.Fatal("capture pre-cut authority", keys, err)
		}
		before, err := port.ReadRoot(ctx, keys[0])
		if err != nil || len(before.Readers) != 1 {
			t.Fatal("capture pre-cut reader", before, err)
		}
		trace.Store(true)
		restart()
		js, err = jetstream.NewWithDomain(connection(), "ARCHIVE", apiTrace)
		if err != nil {
			t.Fatal(err)
		}
		// Stream admission and root reads must recover the original stores;
		// no replacement streams, readers or application roots are created.
		authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
		if err != nil {
			t.Fatal(err)
		}
		port.NativePort, err = graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
		if err != nil {
			t.Fatal(err)
		}
		// Metadata reads after all-peer restart can lose one response. Bound each
		// read-only lookup, retaining the original whole-fixture deadline.
		for attempts := 1; ; attempts++ {
			lookupCtx, stopLookup := context.WithTimeout(ctx, 3*time.Second)
			progressKV, err = js.KeyValue(lookupCtx, "ARCHIVE_PROGRESS")
			stopLookup()
			if err == nil {
				break
			}
			t.Logf("NATIVE_PROGRESS_LOOKUP_RETRY attempt=%d error=%v", attempts, err)
			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		cfg.CompactionCheckpoints = journal.NewCompactionCheckpointPort(progressKV)
		store, err := journal.OpenNativeGraphStore(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		after, err := port.ReadRoot(ctx, keys[0])
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("persisted authority changed across restart", before, after, err)
		}
		t.Logf("ARCHIVE_AUTHORITY_RECOVERED head=%d readers=%d unchanged=true", after.Head, len(after.Readers))
		if !store.ArchiveCheckpoints() {
			t.Fatal("native reopen lost v6 selection")
		}
		return store
	}
	checkpointArchiveScenario(t, ctx, 3, config, port, &now, reopen, true)
	objectStream, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
	if err != nil {
		t.Fatal(err)
	}
	info, err := objectStream.Info(ctx, jetstream.WithSubjectFilter("$O."+cfg.ObjectBucket+".>"))
	if err != nil || uint64(len(info.State.Subjects)) != info.State.NumSubjects {
		t.Fatal("incomplete chunk census", err)
	}
	for subject := range info.State.Subjects {
		if strings.Contains(subject, ".C.") {
			t.Fatal("physical chunks leaked", subject)
		}
	}
}
