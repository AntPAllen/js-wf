package integrity_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/integrity"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestNativeRawGraphCheckpointAudit(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, indexed := range []bool{false, true} {
			t.Run(fmt.Sprintf("R%d/indexed%v", replicas, indexed), func(t *testing.T) {
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
						case <-time.After(10 * time.Millisecond):
						}
					}
				}
				js, err := jetstream.New(cluster.Clients[0])
				if err != nil {
					t.Fatal(err)
				}
				if err := provision.Ensure(ctx, js, replicas); err != nil {
					t.Fatal(err)
				}
				cfg := journal.NativeGraphConfig{AuthorityStream: "AUDIT_CHECKPOINT", AuthorityPrefix: "wf.graph.checkpoint", ObjectBucket: "AUDIT_FRAMES", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, OwnerScopeIndex: indexed, PinTTL: time.Minute, IntentTTL: time.Minute, PayloadReadLimit: checkpoint.MaxBytes}
				configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
				if err != nil {
					t.Fatal(err)
				}
				for _, config := range configs {
					if _, err := js.CreateStream(ctx, config); err != nil {
						t.Fatal(err)
					}
				}
				graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
				if err != nil {
					t.Fatal(err)
				}
				c, err := client.NewWithGraphJournal(js, graph)
				if err != nil {
					t.Fatal(err)
				}
				h, err := c.Start(ctx, "kind", "id", []byte(`42`))
				if err != nil {
					t.Fatal(err)
				}
				tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				var index uint64
				appendEntry := func(kind journal.Kind, payload []byte, objects ...[]byte) {
					t.Helper()
					tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: index, Epoch: 1, WorkerID: "worker", Kind: kind, Payload: payload}, tail, objects, nil)
					if err != nil {
						t.Fatal(err)
					}
					index++
				}
				sum := sha256.Sum256([]byte(`42`))
				appendEntry(journal.Started, []byte(`{"input_sha256":"`+hex.EncodeToString(sum[:])+`"}`), []byte(`42`))
				locals := json.RawMessage(`{"x":1}`)
				frame, hash, err := checkpoint.Encode(checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: 2, Epoch: 1}, StepPosition: 2})
				if err != nil {
					t.Fatal(err)
				}
				sum = sha256.Sum256(locals)
				appendEntry(journal.StepRequested, []byte(`{"kind":"checkpoint","name":"next","input_hash":"`+hex.EncodeToString(sum[:])+`"}`))
				appendEntry(journal.StepCompleted, []byte(`{"result_ref":"step-result-`+hash+`","result_hash":"`+hash+`"}`), frame)
				runtime := journal.RuntimeCheckpoint{InvSeq: h.InvSeq, Stage: "next", Sequence: tail, Index: 2, Epoch: 1, StepPosition: 2, Object: "step-result-" + hash, SHA256: hash}
				appendEntry(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`))
				ns := integrity.GraphAuditNamespace{AuthorityStream: cfg.AuthorityStream, AuthorityPrefix: cfg.AuthorityPrefix, ObjectBucket: cfg.ObjectBucket, PayloadLimit: cfg.PayloadReadLimit}
				audit := func(stage string, terminal int) {
					t.Helper()
					report, err := integrity.CheckNativeGraphJournals(ctx, js, ns)
					if err != nil || report.Entries != int(index) || report.Terminal != terminal || report.Journals != 1 || report.Pending != 0 {
						t.Fatal(stage, report, err)
					}
					t.Logf("RAW_CHECKPOINT_AUDIT stage=%s entries=%d terminals=%d R%d indexed=%v", stage, report.Entries, report.Terminal, replicas, indexed)
				}
				audit("unpublished", 0)
				if err := graph.PublishCheckpoint(ctx, h.Type, h.ID, runtime, tail); err != nil {
					t.Fatal(err)
				}
				audit("published", 0)
				if err := graph.CompactCheckpoint(ctx, h.Type, h.ID, runtime, tail); err != nil {
					t.Fatal(err)
				}
				audit("archived", 0)
				outcome := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"NDI="}`, h.InvSeq))
				appendEntry(journal.Completed, outcome)
				state, err := js.KeyValue(ctx, "WF_STATE")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := state.Put(ctx, "kind.id", outcome); err != nil {
					t.Fatal(err)
				}
				audit("terminal", 1)
			})
		}
	}
}
