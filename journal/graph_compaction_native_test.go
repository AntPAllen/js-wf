package journal_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/testcluster"
)

// Rebinding after a graceful server restart retains no process-local ownership
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
			js, err := jetstream.NewWithDomain(cluster.Clients[0], "ARCHIVE")
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
			config := journal.GraphConfig{Protocol: graphpublication.Protocol{Port: port}, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, Now: cfg.Now, PinTTL: cfg.PinTTL, IntentTTL: cfg.IntentTTL}
			reopen := func() *journal.GraphStore {
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
				var err error
				js, err = jetstream.NewWithDomain(cluster.Clients[0], "ARCHIVE")
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
				store, err := journal.OpenNativeGraphStore(ctx, js, cfg)
				if err != nil {
					t.Fatal(err)
				}
				if !store.ArchiveCheckpoints() {
					t.Fatal("native reopen lost v6 selection")
				}
				return store
			}
			checkpointArchiveScenario(t, ctx, 3, config, port, &now, reopen)
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
			t.Log("NATIVE_CHECKPOINT_ARCHIVE successive_compactions=2 all_servers_gracefully_restarted=true old_reader_preserved=true original_receipt_collected=true full_logical_audit=true raw_chunks_after_retirement=0")
		})
	}
}
