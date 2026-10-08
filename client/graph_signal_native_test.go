package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"testing"
	"time"
)

func TestNativeGraphClientSignalsUseCanonicalHistory(t *testing.T) {
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
			cfg := journal.NativeGraphConfig{AuthorityStream: "GRAPH_SIGNAL_CLIENT_AUTH", AuthorityPrefix: "wf.graph.signalclient", ObjectBucket: "GRAPH_SIGNAL_CLIENT_OBJECTS", ExpectedReplicas: replicas}
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

			for _, mode := range []string{"running", "terminal", "failed", "cancelled", "duplicate_inline", "duplicate_external", "duplicate_bad_hash", "duplicate_missing_edge", "fenced", "generation_retired"} {
				t.Run(mode, func(t *testing.T) {
					const typ = "signals"
					handle, err := c.Start(ctx, typ, mode, []byte(`7`))
					if err != nil {
						t.Fatal(err)
					}
					tail, err := store.Begin(ctx, typ, mode, handle.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
					index := uint64(0)
					appendEntry := func(kind journal.Kind, body []byte, payloads [][]byte) {
						tail, err = store.Append(ctx, typ, mode, handle.InvSeq, journal.Entry{Kind: kind, Index: index, Payload: body}, tail, payloads, nil)
						if err != nil {
							t.Fatal(err)
						}
						index++
					}
					appendEntry(journal.Started, nil, nil)
					if _, err = state.Put(ctx, identity.Key(typ, mode), []byte(`not a valid compatibility mirror`)); err != nil {
						t.Fatal(err)
					}
					if _, err = js.Publish(ctx, identity.JournalSubject(typ, mode), []byte(`{"kind":"completed","index":999}`)); err != nil {
						t.Fatal(err)
					}
					payload := []byte(`42`)
					hash := func(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }
					duplicate := mode == "duplicate_inline" || mode == "duplicate_external" || mode == "duplicate_bad_hash" || mode == "duplicate_missing_edge"
					first := uint64(0)
					if duplicate {
						if mode == "duplicate_external" || mode == "duplicate_missing_edge" {
							payload = bytes.Repeat([]byte("x"), client.MaxInlineSignal+1)
						}
						first, err = c.Signal(ctx, typ, mode, "go", payload, "once")
						if err != nil {
							t.Fatal(err)
						}
						event := map[string]any{"sig_seq": first, "name": "go", "hash": hash(payload), "payload": payload}
						var owned [][]byte
						if mode == "duplicate_external" || mode == "duplicate_missing_edge" {
							delete(event, "payload")
							event["ref"] = "signal-" + hash(payload)
							if mode == "duplicate_external" {
								owned = [][]byte{payload}
							}
						}
						if mode == "duplicate_bad_hash" {
							event["hash"] = hash([]byte(`43`))
						}
						body, _ := json.Marshal(event)
						appendEntry(journal.SignalConsumed, body, owned)
						signals, err := js.Stream(ctx, "WF_SIG")
						if err != nil {
							t.Fatal(err)
						}
						if err = signals.DeleteMsg(ctx, first); err != nil {
							t.Fatal(err)
						}
					}
					terminal := mode == "terminal" || mode == "failed" || mode == "cancelled" || mode == "fenced" || mode == "generation_retired"
					finish := func() {
						outcome := wf.Outcome{InvSeq: handle.InvSeq, Result: []byte(`42`)}
						kind := journal.Completed
						if mode == "failed" || mode == "cancelled" {
							kind = journal.Failed
							outcome = wf.Outcome{InvSeq: handle.InvSeq, Error: "failed"}
							if mode == "cancelled" {
								outcome.Error = client.ErrCancelled.Error()
							}
						}
						body, _ := json.Marshal(outcome)
						appendEntry(kind, body, nil)
					}
					if terminal {
						finish()
					}
					if mode == "fenced" {
						if err = store.FencePurge(ctx, typ, mode, handle.InvSeq, tail); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "generation_retired" {
						if err = store.Retire(ctx, typ, mode, handle.InvSeq, tail); err != nil {
							t.Fatal(err)
						}
						inv, err := js.Stream(ctx, "WF_INV")
						if err != nil {
							t.Fatal(err)
						}
						if err = inv.DeleteMsg(ctx, handle.InvSeq); err != nil {
							t.Fatal(err)
						}
					}
					var seq uint64
					if mode == "generation_retired" {
						seq, err = c.SignalToGeneration(ctx, typ, mode, "go", payload, "once", handle.InvSeq)
					} else {
						seq, err = c.SignalWithOptions(ctx, typ, mode, "go", payload, "once", client.SignalOptions{RequireRunning: !duplicate})
					}
					var want error
					switch mode {
					case "terminal", "failed", "cancelled":
						want = client.ErrNotRunning
					case "duplicate_bad_hash":
						want = client.ErrSignalMismatch
					case "duplicate_missing_edge":
						want = wf.ErrCorruptJournal
					case "fenced":
						want = client.ErrPurged
					case "generation_retired":
						want = client.ErrStaleGeneration
					}
					if want != nil {
						if !errors.Is(err, want) || seq != 0 {
							t.Fatal("canonical rejection", seq, err, want)
						}
					} else if err != nil || seq == 0 || duplicate && seq != first {
						t.Fatal("canonical signal", seq, err)
					}
					if !terminal {
						finish()
					}
					if mode != "generation_retired" {
						if err = store.Retire(ctx, typ, mode, handle.InvSeq, tail); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
