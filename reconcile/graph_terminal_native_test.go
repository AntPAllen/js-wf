package reconcile_test

import (
	"bytes"
	"context"
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
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

func TestNativeGraphReconcileTerminalProjection(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, e := testcluster.Start(t.TempDir(), replicas)
			if e != nil {
				t.Fatal(e)
			}
			defer cluster.Close()
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
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			js, e := jetstream.New(cluster.Clients[0])
			if e != nil {
				t.Fatal(e)
			}
			if e = provision.Ensure(ctx, js, replicas); e != nil {
				t.Fatal(e)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "TERMINAL_CATALOG_AUTH", AuthorityPrefix: "wf.graph.terminalcatalog", ObjectBucket: "TERMINAL_CATALOG_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true}
			configs, e := journal.NativeGraphStreamConfigs(cfg, replicas)
			if e != nil {
				t.Fatal(e)
			}
			for _, config := range configs {
				if _, e = js.CreateStream(ctx, config); e != nil {
					t.Fatal(e)
				}
			}
			graph, e := journal.OpenNativeGraphStore(ctx, js, cfg)
			if e != nil {
				t.Fatal(e)
			}
			c, e := client.NewWithGraphJournal(js, graph)
			if e != nil {
				t.Fatal(e)
			}
			id := "terminal"
			for suffix := 0; identity.Partition("catalog", id, provision.Partitions) != 0; suffix++ {
				id = fmt.Sprintf("terminal%d", suffix)
			}
			h, e := c.Start(ctx, "catalog", id, []byte(`7`))
			if e != nil {
				t.Fatal(e)
			}
			var effects atomic.Int64
			handlers := map[string]worker.Handler{"catalog": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				value, e := wf.Run(c, "once", 0, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
				if e != nil {
					return nil, e
				}
				return json.Marshal(value)
			}}
			run := func(repair bool) {
				runCtx, stop := context.WithCancel(ctx)
				defer stop()
				w, e := worker.New(ctx, js, "terminalcatalog", handlers, worker.WithGraphJournal(graph), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
					if event.Stage == "ack" {
						stop()
					}
				}))
				if e != nil {
					t.Fatal(e)
				}
				done := make(chan error, 1)
				if repair {
					go func() {
						done <- reconcile.RunRepairLoopWithGraphJournal(runCtx, js, "terminalcatalog", "graph-terminal", 10*time.Millisecond, 1, graph, nil, nil, nil)
					}()
				}
				if e = w.RunPartition(runCtx, 0); e != nil {
					t.Fatal(e)
				}
				stop()
				if repair {
					if e = <-done; e != nil {
						t.Fatal(e)
					}
				}
				if e = w.Close(); e != nil {
					t.Fatal(e)
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
			}
			run(false)
			records, tail, e := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
			if e != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
				t.Fatal(records, e)
			}
			terminal := bytes.Clone(records[len(records)-1].Payload)
			state, e := js.KeyValue(ctx, "WF_STATE")
			if e != nil {
				t.Fatal(e)
			}
			runs, e := js.Stream(ctx, "WF_RUN")
			if e != nil {
				t.Fatal(e)
			}
			// Repeat before the message-ID deduplication window expires. There are no
			// retained wakeups and no caller repair key; catalog discovery is required.
			for pass := 0; pass < 2; pass++ {
				if e = state.Delete(ctx, identity.Key(h.Type, h.ID)); e != nil {
					t.Fatal(e)
				}
				if e = runs.Purge(ctx); e != nil {
					t.Fatal(e)
				}
				graph, e = journal.OpenNativeGraphStore(ctx, js, cfg)
				if e != nil {
					t.Fatal(e)
				}
				run(true)
				value, e := state.Get(ctx, identity.Key(h.Type, h.ID))
				if e != nil || !bytes.Equal(value.Value(), terminal) {
					t.Fatal("terminal projection not recovered", pass, e)
				}
				current, currentTail, e := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
				if e != nil || currentTail != tail || len(current) != len(records) || !bytes.Equal(current[len(current)-1].Payload, terminal) || effects.Load() != 1 {
					t.Fatal("catalog recovery changed journal or repeated effect", pass, e, effects.Load())
				}
			}
			t.Log("reopened fenced graph-terminal loop restored two deleted projections inside dedup window; exact terminal bytes, unchanged journal, effect one")
		})
	}
}
