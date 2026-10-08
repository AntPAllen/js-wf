package journal_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/testcluster"
)

func TestNativeGraphJournalPublicSDK(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "PUBLIC_GRAPH_AUTH", AuthorityPrefix: "wf.graph.public", ObjectBucket: "PUBLIC_GRAPH_OBJECTS", Encoding: journal.ProtobufV1, ExpectedReplicas: replicas}
			configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
			if err != nil {
				t.Fatal(err)
			}
			// Missing stores are not provisioned implicitly.
			if _, err = journal.OpenNativeGraphStore(ctx, js, cfg); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatal("missing authority", err)
			}
			if _, err = js.Stream(ctx, "OBJ_"+cfg.ObjectBucket); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatal("missing object stream created", err)
			}
			auth, err := js.CreateStream(ctx, configs[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err = journal.OpenNativeGraphStore(ctx, js, cfg); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatal("missing object stream", err)
			}
			objects, err := js.CreateStream(ctx, configs[1])
			if err != nil {
				t.Fatal(err)
			}
			beforeAuth, err := auth.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			beforeObjects, err := objects.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			wrongReplicas := cfg
			wrongReplicas.ExpectedReplicas = 2
			if _, err = journal.OpenNativeGraphStore(ctx, js, wrongReplicas); err == nil {
				t.Fatal("replica mismatch admitted")
			}
			store, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if store.PayloadReadLimit() != journal.DefaultGraphPayloadLimit {
				t.Fatal("default payload budget")
			}
			afterAuth, err := auth.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			afterObjects, err := objects.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(beforeAuth.Config, afterAuth.Config) || !reflect.DeepEqual(beforeObjects.Config, afterObjects.Config) || beforeAuth.State.LastSeq != afterAuth.State.LastSeq || beforeObjects.State.LastSeq != afterObjects.State.LastSeq {
				t.Fatal("admission changed stores")
			}
			tail, err := store.Begin(ctx, "flow", "public", 1)
			if err != nil {
				t.Fatal(err)
			}
			input := bytes.Repeat([]byte("sdk"), 50000)
			tail, err = store.Append(ctx, "flow", "public", 1, journal.Entry{Kind: journal.Started}, tail, [][]byte{input}, nil)
			if err != nil {
				t.Fatal(err)
			}
			view, err := store.Open(ctx, "flow", "public", 1)
			if err != nil {
				t.Fatal(err)
			}
			record, err := view.Read(ctx, 0)
			if err != nil || len(record.Blobs) != 1 {
				t.Fatal(record, err)
			}
			var link journal.GraphPayloadLink = record.Blobs[0]
			owned := []journal.GraphOwnedPayload{{Index: 0, Link: link}}
			tail, err = store.Append(ctx, "flow", "public", 1, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, owned)
			if err != nil {
				t.Fatal(err)
			}
			fresh, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			records, readTail, err := fresh.Read(ctx, "flow", "public", 1)
			if err != nil || readTail != tail || len(records) != 2 {
				t.Fatal(records, readTail, err)
			}
			terminalView, err := fresh.Open(ctx, "flow", "public", 1)
			if err != nil {
				t.Fatal(err)
			}
			terminal, err := terminalView.Read(ctx, 1)
			if err != nil || len(terminal.Blobs) != 1 {
				t.Fatal(terminal, err)
			}
			got, err := terminalView.Payload(ctx, 1, terminal.Blobs[0], len(input))
			if err != nil || !bytes.Equal(got, input) {
				t.Fatal("reopened reused payload", err)
			}
			if err = terminalView.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if err = store.Retire(ctx, "flow", "public", 1, tail); err != nil {
				t.Fatal(err)
			}
			got, err = view.Payload(ctx, 0, link, len(input))
			if err != nil || !bytes.Equal(got, input) {
				t.Fatal("old retained snapshot", err)
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = fresh.Open(ctx, "flow", "public", 1); !errors.Is(err, journal.ErrStale) {
				t.Fatal("retired generation reopened", err)
			}
		})
	}
}

func TestNativeGraphJournalPublicAdmission(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
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
	cfg := journal.NativeGraphConfig{AuthorityStream: "PUBLIC_ADMISSION", AuthorityPrefix: "wf.graph.admission", ObjectBucket: "PUBLIC_ADMISSION_OBJECTS"}
	configs, err := journal.NativeGraphStreamConfigs(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	unsafe := configs[0]
	unsafe.MaxAge = time.Hour
	auth, err := js.CreateStream(ctx, unsafe)
	if err != nil {
		t.Fatal(err)
	}
	before, err := auth.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = journal.OpenNativeGraphStore(ctx, js, cfg); err == nil {
		t.Fatal("unsafe authority admitted")
	}
	after, err := auth.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Config, after.Config) || after.State.LastSeq != before.State.LastSeq {
		t.Fatal("unsafe authority mutated")
	}
	if _, err = js.UpdateStream(ctx, configs[0]); err != nil {
		t.Fatal(err)
	}
	// A standard legacy ObjectStore must retain its bytes and configuration.
	legacy, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: cfg.ObjectBucket, Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = legacy.PutBytes(ctx, "keep", []byte("legacy bytes")); err != nil {
		t.Fatal(err)
	}
	objectStream, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
	if err != nil {
		t.Fatal(err)
	}
	before, err = objectStream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = journal.OpenNativeGraphStore(ctx, js, cfg); err == nil {
		t.Fatal("legacy object bucket admitted")
	}
	after, err = objectStream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Config, after.Config) || after.State.LastSeq != before.State.LastSeq {
		t.Fatal("legacy object store mutated")
	}
	data, err := legacy.GetBytes(ctx, "keep")
	if err != nil || string(data) != "legacy bytes" {
		t.Fatal(string(data), err)
	}
}

func TestGraphJournalPublicConfiguration(t *testing.T) {
	valid := journal.NativeGraphConfig{AuthorityStream: "GRAPH_AUTH", AuthorityPrefix: "wf.graph.sdk", ObjectBucket: "GRAPH_OBJECTS"}
	mutations := []func(*journal.NativeGraphConfig){
		func(c *journal.NativeGraphConfig) { c.AuthorityStream = "" },
		func(c *journal.NativeGraphConfig) { c.AuthorityPrefix = "wf.>" },
		func(c *journal.NativeGraphConfig) { c.ObjectBucket = "bad.bucket" },
		func(c *journal.NativeGraphConfig) { c.AuthorityStream = "OBJ_" + c.ObjectBucket },
		func(c *journal.NativeGraphConfig) { c.PinTTL = -time.Second },
		func(c *journal.NativeGraphConfig) { c.IntentTTL = -time.Second },
		func(c *journal.NativeGraphConfig) { c.PayloadReadLimit = -1 },
		func(c *journal.NativeGraphConfig) { c.Encoding = "bad" },
		func(c *journal.NativeGraphConfig) { c.ExpectedReplicas = -1 },
		func(c *journal.NativeGraphConfig) { c.ExpectedReplicas = 6 },
	}
	for i, mutate := range mutations {
		c := valid
		mutate(&c)
		if _, err := journal.NativeGraphStreamConfigs(c, 1); err == nil {
			t.Fatal("invalid configuration admitted", i)
		}
	}
	for _, replicas := range []int{0, 6} {
		if _, err := journal.NativeGraphStreamConfigs(valid, replicas); err == nil {
			t.Fatal("replicas", replicas)
		}
	}
	if _, err := journal.OpenNativeGraphStore(context.Background(), nil, valid); err == nil {
		t.Fatal("nil JetStream")
	}
}
