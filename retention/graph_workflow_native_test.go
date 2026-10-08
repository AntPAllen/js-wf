package retention_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync/atomic"
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
	"js-wf/worker"
)

func TestNativeGraphRetentionWorkflowBindsAndPurgesTarget(t *testing.T) {
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
			if err = provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "WORKFLOW_PURGE_AUTH", AuthorityPrefix: "wf.graph.workflowpurge", ObjectBucket: "WORKFLOW_PURGE_OBJECTS", ExpectedReplicas: replicas}
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
			c, err := client.NewWithGraphJournal(js, graph)
			if err != nil {
				t.Fatal(err)
			}
			var effects atomic.Int64
			handlers := map[string]worker.Handler{"source": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				_, err := wf.Run(c, "once", 1, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
				return []byte(`42`), err
			}, "retention": retention.GraphHandler(js, graph, time.Minute)}
			run := func(typ, id, name string) {
				runCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				var stage string
				w, err := worker.New(ctx, js, name, handlers, worker.WithGraphJournal(graph), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
					if event.Stage == "ack" || event.Stage == "nak" {
						stage = event.Stage
						cancel()
					}
				}))
				if err != nil {
					t.Fatal(err)
				}
				if err = w.RunPartition(runCtx, identity.Partition(typ, id, provision.Partitions)); err != nil {
					t.Fatal(err)
				}
				if stage != "ack" {
					t.Fatal("worker decision", stage)
				}
			}
			source, err := c.Start(ctx, "source", "target", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			run("source", "target", "source-purge-fixture")
			if got, err := c.Await(ctx, "source", "target"); err != nil || !bytes.Equal(got, []byte(`42`)) {
				t.Fatal(string(got), err)
			}
			input, _ := json.Marshal(retention.Request{Type: "source", ID: "target"})
			operation, err := c.Start(ctx, "retention", "operation", input)
			if err != nil {
				t.Fatal(err)
			}
			run("retention", "operation", "graph-retention-workflow")
			if got, err := c.Await(ctx, "retention", "operation"); err != nil || !bytes.Equal(got, []byte(`true`)) {
				t.Fatal("retention result", string(got), err)
			}
			if effects.Load() != 1 {
				t.Fatal("source effect repeated", effects.Load())
			}
			status, err := graph.InspectRetirement(ctx, "source", "target")
			if err != nil || !status.Purging || !status.Retired || status.Invocation != source.InvSeq {
				t.Fatal(status, err)
			}
			records, _, err := graph.Read(ctx, "retention", "operation", operation.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			bound := false
			declaredGraph := false
			for i, record := range records {
				if record.Kind != journal.StepRequested {
					continue
				}
				var req struct {
					Kind      string `json:"kind"`
					Name      string `json:"name"`
					InputHash string `json:"input_hash"`
				}
				if json.Unmarshal(record.Payload, &req) != nil {
					t.Fatal("bad request")
				}
				if req.Name == "purge-target-0" {
					if i+1 >= len(records) || records[i+1].Kind != journal.StepCompleted {
						t.Fatal("unrecorded target lookup")
					}
					var done struct {
						Result json.RawMessage `json:"result"`
					}
					if json.Unmarshal(records[i+1].Payload, &done) != nil {
						t.Fatal("bad lookup result")
					}
					var seq uint64
					if json.Unmarshal(done.Result, &seq) != nil || seq != source.InvSeq {
						t.Fatal("target generation not bound", string(done.Result))
					}
					bound = true
				}
				if req.Name == "purge-0" {
					declared := struct {
						Request retention.Request `json:"request"`
						Grace   int64             `json:"grace_nanos"`
						Graph   bool              `json:"graph,omitempty"`
					}{retention.Request{Type: "source", ID: "target"}, int64(time.Minute), true}
					data, _ := json.Marshal(declared)
					sum := sha256.Sum256(data)
					if req.InputHash != hex.EncodeToString(sum[:]) {
						t.Fatal("graph mode not declared", req.InputHash)
					}
					declaredGraph = true
				}
			}
			if !bound || !declaredGraph {
				t.Fatal("retention workflow declarations absent")
			}
		})
	}
}
