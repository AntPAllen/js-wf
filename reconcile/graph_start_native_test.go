package reconcile_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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

func TestNativeGraphReconcileCanonicalPendingStarts(t *testing.T) {
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
			cfg := journal.NativeGraphConfig{AuthorityStream: "START_REPAIR_AUTH", AuthorityPrefix: "wf.graph.startrepair", ObjectBucket: "START_REPAIR_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true}
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
			const typ = "repair"
			ids := []string{}
			for _, mode := range []string{"reserved", "source_committed", "bound_no_enqueue"} {
				id := mode
				for suffix := 0; identity.Partition(typ, id, provision.Partitions) != 0; suffix++ {
					id = fmt.Sprintf("%s%d", mode, suffix)
				}
				ids = append(ids, id)
				state, e := graph.ReserveStart(ctx, journal.GraphStartRequest{Type: typ, ID: id}, []byte(`7`))
				if e != nil {
					t.Fatal(e)
				}
				if mode != "reserved" {
					msg := &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: state.Start.PointerBytes(), Header: nats.Header{}}
					msg.Header.Set(journal.GraphStartTokenHeader, state.Start.Token)
					msg.Header.Set("Wf-Input-SHA256", state.Start.InputSHA256)
					msg.Header.Set(jetstream.ExpectedLastSubjSeqHeader, "0")
					ack, e := js.PublishMsg(ctx, msg)
					if e != nil {
						t.Fatal(e)
					}
					if mode == "bound_no_enqueue" {
						if e = graph.BindStart(ctx, typ, id, state.Start.Token, ack.Sequence); e != nil {
							t.Fatal(e)
						}
					}
				}
			}
			inv, err := js.Stream(ctx, "WF_INV")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, ids[0])); err != jetstream.ErrMsgNotFound {
				t.Fatal("reserved fixture has invocation", err)
			}
			// Reopen before repair: no prepared object/client-local input is retained.
			graph, err = journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			c, err := client.NewWithGraphJournal(js, graph)
			if err != nil {
				t.Fatal(err)
			}
			var effects atomic.Int64
			var repairs atomic.Int64
			runCtx, cancel := context.WithCancel(ctx)
			handlers := map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
				if !bytes.Equal(input, []byte(`7`)) {
					return nil, fmt.Errorf("input mismatch")
				}
				value, e := wf.Run(c, "once", 1, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
				if e != nil {
					return nil, e
				}
				return json.Marshal(value)
			}}
			w, err := worker.New(ctx, js, "startrepair", handlers, worker.WithGraphJournal(graph))
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			workerDone := make(chan error, 1)
			repairDone := make(chan error, 1)
			go func() { workerDone <- w.RunPartition(runCtx, 0) }()
			go func() {
				repairDone <- reconcile.RunRepairLoopWithGraphJournal(runCtx, js, "startrepair", "graph-start", 10*time.Millisecond, 1, graph, func(e reconcile.RepairEvent) {
					if e.Outcome == "acknowledged" {
						repairs.Add(1)
					}
				}, nil, nil)
			}()
			defer func() {
				cancel()
				if e := <-repairDone; e != nil {
					t.Error(e)
				}
				if e := <-workerDone; e != nil {
					t.Error(e)
				}
				if e := w.Close(); e != nil {
					t.Error(e)
				}
			}()
			for _, id := range ids {
				result, e := c.Await(ctx, typ, id)
				if e != nil || !bytes.Equal(result, []byte(`42`)) {
					t.Fatal(id, string(result), e)
				}
			}
			if effects.Load() != 3 || repairs.Load() < 3 {
				t.Fatal("repair/effect counts", repairs.Load(), effects.Load())
			}
			t.Logf("reopened graph-start loop recovered 3 durable cuts without caller input; effects=%d", effects.Load())
		})
	}
}
