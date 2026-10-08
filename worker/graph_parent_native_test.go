package worker

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
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeGraphWorkerParentNotifications(t *testing.T) {
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
			cfg := journal.NativeGraphConfig{AuthorityStream: "GRAPH_PARENT_WORKER_AUTH", AuthorityPrefix: "wf.graph.parentworker", ObjectBucket: "GRAPH_PARENT_WORKER_OBJECTS", ExpectedReplicas: replicas}
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

			leasing, err := lease.New(ctx, js)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range []string{"active", "uninitialized", "forged_mirror", "purging", "retired", "retired_missing_inv", "replaced", "missing_unconfirmed", "consumed_inline", "consumed_external", "consumed_corrupt"} {
				t.Run(mode, func(t *testing.T) {
					const childType, parentType = "child", "parent"
					childID, parentID := mode, mode
					for identity.Partition(parentType, parentID, provision.Partitions) == identity.Partition(childType, childID, provision.Partitions) {
						parentID += "x"
					}
					parent, err := c.Start(ctx, parentType, parentID, []byte(`null`))
					if err != nil {
						t.Fatal(err)
					}
					child, err := c.StartChild(ctx, childType, childID, []byte(`null`), parentType, parentID, parent.InvSeq, "result")
					if err != nil {
						t.Fatal(err)
					}
					var parentTail, parentIndex uint64
					parentAppend := func(kind journal.Kind, payload []byte, owned [][]byte) {
						parentTail, err = store.Append(ctx, parentType, parentID, parent.InvSeq, journal.Entry{Kind: kind, Index: parentIndex, Payload: payload}, parentTail, owned, nil)
						if err != nil {
							t.Fatal(err)
						}
						parentIndex++
					}
					parentFinish := func() {
						body, _ := json.Marshal(wf.Outcome{InvSeq: parent.InvSeq, Result: []byte(`42`)})
						parentAppend(journal.Completed, body, nil)
					}
					if mode != "uninitialized" {
						parentTail, err = store.Begin(ctx, parentType, parentID, parent.InvSeq)
						if err != nil {
							t.Fatal(err)
						}
						parentAppend(journal.Started, nil, nil)
					}
					if mode == "forged_mirror" {
						if _, err = state.Put(ctx, identity.Key(parentType, parentID), []byte(`{"tombstone":true,"inv_seq":999,"purged_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-02T00:00:00Z"}`)); err != nil {
							t.Fatal(err)
						}
						if _, err = js.Publish(ctx, identity.JournalSubject(parentType, parentID), []byte(`{"kind":"completed","index":999}`)); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "purging" || mode == "retired" || mode == "retired_missing_inv" || mode == "replaced" {
						parentFinish()
						if mode == "purging" {
							err = store.FencePurge(ctx, parentType, parentID, parent.InvSeq, parentTail)
						} else {
							err = store.Retire(ctx, parentType, parentID, parent.InvSeq, parentTail)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					if mode == "retired_missing_inv" || mode == "replaced" || mode == "missing_unconfirmed" {
						inv, err := js.Stream(ctx, "WF_INV")
						if err != nil {
							t.Fatal(err)
						}
						if err = inv.DeleteMsg(ctx, parent.InvSeq); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "replaced" {
						if _, err = c.Start(ctx, parentType, parentID, []byte(`43`)); err != nil {
							t.Fatal(err)
						}
					}
					result := []byte(`42`)
					if mode == "consumed_external" {
						result = []byte(`"` + strings.Repeat("r", client.MaxInlineSignal+1) + `"`)
					}
					body, _ := json.Marshal(wf.Outcome{InvSeq: child.InvSeq, Result: result})
					childTail, err := store.Begin(ctx, childType, childID, child.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
					childTail, err = store.Append(ctx, childType, childID, child.InvSeq, journal.Entry{Kind: journal.Started}, childTail, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					childTail, err = store.Append(ctx, childType, childID, child.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1, Payload: body}, childTail, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					invocation, err := js.Stream(ctx, "WF_INV")
					if err != nil {
						t.Fatal(err)
					}
					childInput, err := invocation.GetMsg(ctx, child.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
					if strings.HasPrefix(mode, "consumed_") {
						request, _ := json.Marshal(map[string]any{"kind": "call_async", "name": "result", "child_type": childType, "child_id": childID})
						parentAppend(journal.StepRequested, request, nil)
						if err = NotifyParentWithClient(ctx, c, childType, childID, child.InvSeq, body, childInput.Header); err != nil {
							t.Fatal(err)
						}
						sig, err := js.Stream(ctx, "WF_SIG")
						if err != nil {
							t.Fatal(err)
						}
						original, err := sig.GetLastMsgForSubject(ctx, "wf.sig."+parentType+"."+parentID+".result")
						if err != nil {
							t.Fatal(err)
						}
						hash := sha256.Sum256(body)
						digest := hex.EncodeToString(hash[:])
						payload := body
						if mode == "consumed_corrupt" {
							payload = []byte(`{"inv_seq":2,"result":43}`)
						}
						event := map[string]any{"sig_seq": original.Sequence, "name": "result", "hash": digest, "payload": payload}
						var owned [][]byte
						if mode == "consumed_external" {
							delete(event, "payload")
							event["ref"] = "signal-" + digest
							owned = [][]byte{body}
						}
						eventBody, _ := json.Marshal(event)
						parentAppend(journal.SignalConsumed, eventBody, owned)
						if err = sig.DeleteMsg(ctx, original.Sequence); err != nil {
							t.Fatal(err)
						}
					}
					owner, err := leasing.Acquire(ctx, childType, childID, "healthy-child-owner")
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
						defer stop()
						if err := owner.Release(cleanup); err != nil {
							t.Error(err)
						}
					}()
					leaseKV, err := js.KeyValue(ctx, "WF_LEASE")
					if err != nil {
						t.Fatal(err)
					}
					// Dispatch starts only after its exact initialized fixture
					// is observable. A setup GET may lag the acknowledged KV
					// writes; no handler has run and no lease is renewed here.
					setup, endSetup := context.WithTimeout(ctx, 3*time.Second)
					defer endSetup()
					expectedLease, _ := json.Marshal(lease.Value{Worker: "healthy-child-owner", Epoch: owner.Epoch()})
					var before jetstream.KeyValueEntry
					for probe := 0; ; probe++ {
						before, err = leaseKV.Get(setup, identity.Key(childType, childID))
						if err == nil && before.Revision() > owner.Epoch() && bytes.Equal(before.Value(), expectedLease) {
							if probe > 0 {
								t.Logf("lease setup visibility: epoch=%d revision=%d probes=%d", owner.Epoch(), before.Revision(), probe+1)
							}
							break
						}
						if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
							t.Fatal(err)
						}
						if err == nil {
							var value lease.Value
							if json.Unmarshal(before.Value(), &value) != nil || value.Worker != "healthy-child-owner" || value.Epoch != 0 || before.Revision() != owner.Epoch() {
								t.Fatal("unexpected lease setup value", before.Revision(), string(before.Value()))
							}
						}
						select {
						case <-setup.Done():
							t.Fatal("initialized lease fixture not visible", setup.Err())
						case <-time.After(10 * time.Millisecond):
						}
					}
					var handlers, effects atomic.Int64
					runCtx, cancel := context.WithCancel(ctx)
					defer cancel()
					var stage string
					w, err := New(ctx, js, "graph-parent-"+mode, map[string]Handler{childType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
						handlers.Add(1)
						_, err := wf.Run(c, "unexpected", 1, func(context.Context) (int, error) { effects.Add(1); return 1, nil })
						return []byte(`1`), err
					}}, WithGraphJournal(store), WithDispatchObserver(func(event DispatchEvent) {
						if event.Stage == "ack" || event.Stage == "nak" {
							stage = event.Stage
							cancel()
						}
					}))
					if err != nil {
						t.Fatal(err)
					}
					if err = w.RunPartition(runCtx, identity.Partition(childType, childID, provision.Partitions)); err != nil {
						t.Fatal(err)
					}
					expected := "ack"
					if mode == "consumed_corrupt" || mode == "missing_unconfirmed" {
						expected = "nak"
					}
					if stage != expected || handlers.Load() != 0 || effects.Load() != 0 {
						t.Fatal("unsafe graph parent decision", stage, expected, handlers.Load(), effects.Load())
					}
					after, err := leaseKV.Get(ctx, identity.Key(childType, childID))
					if err != nil || before.Revision() != after.Revision() || !bytes.Equal(before.Value(), after.Value()) {
						t.Fatal("healthy child lease changed", err)
					}
					sig, err := js.Stream(ctx, "WF_SIG")
					if err != nil {
						t.Fatal(err)
					}
					signal, err := sig.GetLastMsgForSubject(ctx, "wf.sig."+parentType+"."+parentID+".result")
					sends := mode == "active" || mode == "uninitialized" || mode == "forged_mirror"
					if sends {
						if err != nil || !bytes.Equal(signal.Data, body) {
							t.Fatal("canonical parent bytes", err)
						}
					} else if err != jetstream.ErrMsgNotFound {
						t.Fatal("signal escaped retired/unconfirmed/consumed parent", err)
					}
					if err = store.Retire(ctx, childType, childID, child.InvSeq, childTail); err != nil {
						t.Fatal(err)
					}
					status, err := store.InspectRetirement(ctx, parentType, parentID)
					if err != nil {
						t.Fatal(err)
					}
					if !status.Retired && status.Invocation != 0 {
						if status.Kind != journal.Completed && status.Kind != journal.Failed {
							parentFinish()
						}
						if err = store.Retire(ctx, parentType, parentID, parent.InvSeq, parentTail); err != nil {
							t.Fatal(err)
						}
					}
				})
			}
		})
	}
}
