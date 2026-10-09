package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

// Test-only continuation registration and a lowered private worker budget.
// Public admission stays closed and the production 100,000-entry cap is unchanged.
func TestNativeGraphContinuationGlobalLimitAndTerminalSlot(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, archive := range []bool{false, true} {
			t.Run(fmt.Sprintf("R%d/archive=%t", replicas, archive), func(t *testing.T) {
				const budget = uint64(16)
				cluster, err := testcluster.StartWithDomain(t.TempDir(), replicas, "GRAPH_LIMIT")
				if err != nil {
					t.Fatal(err)
				}
				defer cluster.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				if replicas > 1 {
					for ctx.Err() == nil {
						ready := false
						for _, server := range cluster.Servers {
							ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
						}
						if ready {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
				}
				var peers []jetstream.JetStream
				for _, connection := range cluster.Clients {
					js, err := jetstream.NewWithDomain(connection, "GRAPH_LIMIT")
					if err != nil {
						t.Fatal(err)
					}
					peers = append(peers, js)
				}
				js := peers[0]
				if err = provision.Ensure(ctx, js, replicas); err != nil {
					t.Fatal(err)
				}
				cfg := journal.NativeGraphConfig{AuthorityStream: "LIMIT_AUTH", AuthorityPrefix: "wf.graph.limit", ObjectBucket: "LIMIT_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: archive}
				configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
				if err != nil {
					t.Fatal(err)
				}
				for _, config := range configs {
					if _, err = js.CreateStream(ctx, config); err != nil {
						t.Fatal(err)
					}
				}
				graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
				if err != nil {
					t.Fatal(err)
				}
				authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
				if err != nil {
					t.Fatal(err)
				}
				c, err := client.New(js).WithGraphJournal(graph)
				if err != nil {
					t.Fatal(err)
				}
				h, err := c.Start(ctx, "graph-limit", "global", []byte(`null`))
				if err != nil {
					t.Fatal(err)
				}
				calls, effects := map[string]int{}, 0
				handlers := map[string]Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					calls["initial"]++
					if err := c.SetState("value", 10); err != nil {
						return nil, err
					}
					return nil, wf.Continue(c, "middle", 10)
				}}
				stages := map[string]ContinuationHandler{
					"middle": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
						calls["middle"]++
						var value int
						if found, err := c.GetState("value", &value); err != nil || !found || value != 10 || string(locals) != "10" {
							return nil, wf.ErrCorruptJournal
						}
						return nil, wf.Continue(c, "finish", 10)
					},
					"finish": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
						calls["finish"]++
						if string(locals) != "10" {
							return nil, wf.ErrCorruptJournal
						}
						if _, err := wf.AwaitSignal(c, "gate"); err != nil {
							return nil, err
						}
						_, err := wf.Run(c, "must_not_run", 10, func(context.Context) (int, error) { effects++; return 20, nil })
						return json.RawMessage(`20`), err
					},
				}
				leases, err := lease.New(ctx, js)
				if err != nil {
					t.Fatal(err)
				}
				execute := func() {
					t.Helper()
					// Each delivery reconstructs the worker and all reader/runtime state.
					w, err := New(ctx, js, "graph-limit", handlers, WithGraphJournal(graph))
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					if w.maxEntries != journal.MaxEntries {
						t.Fatal("production default cap changed", w.maxEntries)
					}
					w.maxEntries = budget
					w.continuations = map[string]map[string]ContinuationHandler{h.Type: stages}
					owner, err := leases.Acquire(ctx, h.Type, h.ID, "graph-limit")
					if err != nil {
						t.Fatal(err)
					}
					var noOp bool
					err = w.execute(ctx, h.Type, h.ID, owner, time.Time{}, timerWakeup{}, &noOp, nil)
					releaseErr := owner.Release(ctx)
					if err != nil || releaseErr != nil {
						t.Fatal("delivery failed", err, releaseErr)
					}
				}
				for _, stage := range []string{"middle", "finish"} {
					execute()
					status, err := graph.InspectStart(ctx, h.Type, h.ID)
					if err != nil || status.Checkpoint == nil || status.Checkpoint.Stage != stage {
						t.Fatal("checkpoint absent", stage, status, err)
					}
					keys, err := authority.RootKeys(ctx)
					if err != nil || len(keys) != 1 {
						t.Fatal("unexpected root catalog", keys, err)
					}
					root, err := authority.ReadRoot(ctx, keys[0])
					if err != nil {
						t.Fatal(err)
					}
					// Application names are canonical snake_case, including the
					// retained offset that must remain separate from the global cap.
					var cursor struct {
						Count        uint64 `json:"count"`
						RetainedFrom uint64 `json:"retained_from"`
					}
					if err := json.Unmarshal(root.Application, &cursor); err != nil {
						t.Fatal(err)
					}
					wantRetained := uint64(0)
					if archive {
						wantRetained = status.Checkpoint.Index - 1
					}
					if cursor.RetainedFrom != wantRetained || cursor.Count < wantRetained || root.Graph.Count != cursor.Count-wantRetained || len(root.Readers) != 0 {
						t.Fatal("checkpoint compaction or reader drain missing", cursor, root)
					}
				}
				execute() // Finish suspends waiting for the enabling signal.
				records, _, err := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil || uint64(len(records)) != budget-3 || records[len(records)-1].Kind != journal.Suspended || records[len(records)-1].Index != budget-4 {
					t.Fatal("wrong pre-signal global cursor", len(records), err)
				}
				view, err := graph.Open(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				found, err := view.ReadCheckpoint(ctx, h.Type, h.ID)
				closeErr := view.Close(ctx)
				if err != nil || closeErr != nil || found == nil || found.Anchor.Index != budget-7 || found.Runtime.StepPosition != budget-8 {
					t.Fatal("wrong absolute checkpoint cursor", found, err, closeErr)
				}
				initial, middle := calls["initial"], calls["middle"]
				if initial != 1 || middle != 1 {
					t.Fatal("prefix stages reran", calls)
				}
				if _, err = c.Signal(ctx, h.Type, h.ID, "gate", []byte(`true`), "gate-key"); err != nil {
					t.Fatal(err)
				}
				execute()
				_, resultErr := c.Await(ctx, h.Type, h.ID)
				if resultErr == nil || resultErr.Error() != journal.ErrTooLong.Error() || effects != 0 || calls["initial"] != initial || calls["middle"] != middle {
					t.Fatal("limit reset across checkpoints", resultErr, effects, calls)
				}
				records, _, err = graph.Read(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil || uint64(len(records)) != budget || records[budget-1].Kind != journal.Failed || records[budget-3].Kind != journal.SignalConsumed || records[budget-2].Kind != journal.StepCompleted {
					t.Fatal("reserved terminal slot missing", len(records), err)
				}
				for index, record := range records {
					if record.Index != uint64(index) || index > 0 && (record.Sequence != records[index-1].Sequence+1 || record.Epoch < records[index-1].Epoch) {
						t.Fatal("global ordering changed", index, record)
					}
				}
				var outcome wf.Outcome
				if err = json.Unmarshal(records[budget-1].Payload, &outcome); err != nil || outcome.InvSeq != h.InvSeq || outcome.Error != journal.ErrTooLong.Error() || outcome.LimitEntry != nil {
					t.Fatal(outcome, err)
				}
				var rejected struct{ Kind, Name string }
				if err = json.Unmarshal(outcome.LimitRequest, &rejected); err != nil || rejected.Kind != "run" || rejected.Name != "must_not_run" {
					t.Fatal("lost rejected request", rejected, err)
				}
				state, err := js.KeyValue(ctx, "WF_STATE")
				if err != nil {
					t.Fatal(err)
				}
				terminal, err := state.Get(ctx, identity.Key(h.Type, h.ID))
				if err != nil || !bytes.Equal(terminal.Value(), records[budget-1].Payload) {
					t.Fatal("terminal projection differs", err)
				}
				for _, peer := range peers {
					store, err := journal.OpenNativeGraphStore(ctx, peer, cfg)
					if err != nil {
						t.Fatal(err)
					}
					bound, err := client.New(peer).WithGraphJournal(store)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = bound.Await(ctx, h.Type, h.ID); err == nil || err.Error() != resultErr.Error() {
						t.Fatal("peer result changed", err)
					}
				}
				execute() // Terminal redelivery cannot invoke another stage or effect.
				if effects != 0 || calls["initial"] != initial || calls["middle"] != middle || calls["finish"] != 2 {
					t.Fatal("terminal redelivery reran code", calls, effects)
				}
				legacy, err := js.Stream(ctx, "WF_JRN")
				if err != nil {
					t.Fatal(err)
				}
				info, err := legacy.Info(ctx)
				if err != nil || info.State.Msgs != 0 {
					t.Fatal("graph fixture wrote legacy journal", info, err)
				}
				t.Logf("GRAPH_CONTINUATION_LIMIT budget=%d entries=%d archive=%t checkpoints=2 effects=%d rejected_request=%s terminal_slot=%d prefix_stage_calls=%d/%d", budget, len(records), archive, effects, rejected.Name, records[budget-1].Index, initial, middle)
			})
		}
	}
}
