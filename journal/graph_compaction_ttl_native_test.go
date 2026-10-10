package journal

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func TestNativeGraphCompactionLifetimeForwarding(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	cfg := NativeGraphConfig{AuthorityStream: "TTL_AUTH", AuthorityPrefix: "wf.graph.ttl", ObjectBucket: "TTL_OBJECTS", IntentTTL: 6 * time.Second, CompactionIntentTTL: 30 * time.Minute}
	configs, err := NativeGraphStreamConfigs(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs {
		if _, err = js.CreateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	for _, indexed := range []bool{false, true} {
		// Indexed mode is a distinct namespace, not a migration.
		if indexed {
			cfg.AuthorityStream = "TTL_INDEX_AUTH"
			cfg.AuthorityPrefix = "wf.graph.ttlindex"
			cfg.ObjectBucket = "TTL_INDEX_OBJECTS"
			cfg.OwnerScopeIndex = true
			configs, err = NativeGraphStreamConfigs(cfg, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range configs {
				if _, err = js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
		}
		store, err := OpenNativeGraphStore(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if store.cfg.IntentTTL != 6*time.Second || store.cfg.CompactionIntentTTL != 30*time.Minute {
			t.Fatal("native lifetime forwarding lost", indexed)
		}
		cfg.CompactionIntentTTL = 0
		store, err = OpenNativeGraphStore(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if store.cfg.CompactionIntentTTL != cfg.IntentTTL {
			t.Fatal("native lifetime inheritance lost", indexed)
		}
		cfg.CompactionIntentTTL = 30 * time.Minute
	}
}
