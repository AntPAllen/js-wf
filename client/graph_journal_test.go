package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
	"js-wf/wf"
)

func TestNativeGraphClientCanonicalTerminals(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if replicas > 1 {
				for {
					ready := false
					for _, s := range cluster.Servers {
						ready = ready || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas
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
			if err = provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "GRAPH_CLIENT_AUTH", AuthorityPrefix: "wf.graph.client", ObjectBucket: "GRAPH_CLIENT_OBJECTS", ExpectedReplicas: replicas}
			configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range configs {
				if _, err = js.CreateStream(ctx, config); err != nil {
					t.Fatal(err)
				}
			}
			store, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			c, err := client.NewWithGraphJournal(js, store)
			if err != nil {
				t.Fatal(err)
			}
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"inline", "external", "failed", "cancelled", "bad_generation", "bad_kind"} {
				t.Run(mode, func(t *testing.T) {
					handle, err := c.Start(ctx, "graph", mode, []byte(`7`))
					if err != nil {
						t.Fatal(err)
					}
					tail, err := store.Begin(ctx, "graph", mode, handle.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
					tail, err = store.Append(ctx, "graph", mode, handle.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					want := []byte(`42`)
					out := wf.Outcome{InvSeq: handle.InvSeq, Result: want}
					kind := journal.Completed
					var payloads [][]byte
					switch mode {
					case "external":
						want = bytes.Repeat([]byte("large"), 150000)
						sum := sha256.Sum256(want)
						hash := hex.EncodeToString(sum[:])
						out = wf.Outcome{InvSeq: handle.InvSeq, ResultRef: "terminal-result-" + hash, ResultHash: hash}
						payloads = [][]byte{want}
					case "failed", "cancelled":
						kind = journal.Failed
						reason := "graph failure"
						if mode == "cancelled" {
							reason = client.ErrCancelled.Error()
						}
						out = wf.Outcome{InvSeq: handle.InvSeq, Error: reason}
					case "bad_generation":
						out.InvSeq++
					case "bad_kind":
						out.Error = "invalid completion"
					}
					body, _ := json.Marshal(out)
					if _, err = store.Append(ctx, "graph", mode, handle.InvSeq, journal.Entry{Kind: kind, Index: 1, Payload: body}, tail, payloads, nil); err != nil {
						t.Fatal(err)
					}
					check := func() {
						t.Helper()
						value, e := c.Await(ctx, "graph", mode)
						switch mode {
						case "failed", "cancelled":
							if e == nil || e.Error() != out.Error {
								t.Fatal("graph failure differed", e)
							}
						case "bad_generation", "bad_kind":
							if !errors.Is(e, wf.ErrCorruptJournal) {
								t.Fatal("invalid terminal accepted", e)
							}
						default:
							if e != nil || !bytes.Equal(value, want) {
								t.Fatal("graph result differed", len(value), e)
							}
						}
					}
					// A missing legacy terminal must not hide a canonical graph completion.
					check()
					forged, _ := json.Marshal(wf.Outcome{InvSeq: handle.InvSeq, Result: []byte(`"forged legacy"`)})
					if _, err = state.Create(ctx, identity.Key("graph", mode), forged); err != nil {
						t.Fatal(err)
					}
					check()
					// Existing purge markers remain lifecycle fences until retention migrates.
					previous, err := state.Get(ctx, identity.Key("graph", mode))
					if err != nil {
						t.Fatal(err)
					}
					now := time.Now().UTC()
					tomb, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: handle.InvSeq, PurgedAt: now, ExpiresAt: now.Add(time.Hour)})
					if _, err = state.Update(ctx, identity.Key("graph", mode), tomb, previous.Revision()); err != nil {
						t.Fatal(err)
					}
					if _, err = c.Await(ctx, "graph", mode); !errors.Is(err, client.ErrPurged) {
						t.Fatal("purged result returned", err)
					}
				})
			}
		})
	}
}
