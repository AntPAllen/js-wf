package reconcile_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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

func TestNativeGraphReconcileCanonicalSignals(t *testing.T) {
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
			cfg := journal.NativeGraphConfig{AuthorityStream: "SIGNAL_REPAIR_AUTH", AuthorityPrefix: "wf.graph.signalrepair", ObjectBucket: "SIGNAL_REPAIR_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true}
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
			handles := make([]client.Handle, 0, 3)
			for _, mode := range []string{"reserved", "source_committed", "bound_source_purged"} {
				id := mode
				for suffix := 0; identity.Partition("repair", id, provision.Partitions) != 0; suffix++ {
					id = fmt.Sprintf("%s%d", mode, suffix)
				}
				h, e := c.Start(ctx, "repair", id, []byte(`7`))
				if e != nil {
					t.Fatal(e)
				}
				handles = append(handles, h)
				r := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: "key"}
				input, e := graph.ReserveSignal(ctx, r, []byte(`42`), false)
				if e != nil {
					t.Fatal(e)
				}
				if mode != "reserved" {
					msg := &nats.Msg{Subject: "wf.sig." + h.Type + "." + h.ID + ".signal", Data: input.PointerBytes(), Header: nats.Header{}}
					msg.Header.Set(journal.GraphSignalTokenHeader, input.Token)
					msg.Header.Set("Wf-Input-SHA256", input.InputSHA256)
					msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(h.InvSeq, 10))
					ack, e := js.PublishMsg(ctx, msg)
					if e != nil {
						t.Fatal(e)
					}
					if mode == "bound_source_purged" {
						for {
							progress, e := graph.BindNextSignal(ctx, h.Type, h.ID, h.InvSeq, ack.Sequence, nativeSignalRepairSource{js})
							if e != nil {
								t.Fatal(e)
							}
							if !progress {
								break
							}
						}
						stream, e := js.Stream(ctx, "WF_SIG")
						if e != nil {
							t.Fatal(e)
						}
						if e = stream.DeleteMsg(ctx, ack.Sequence); e != nil {
							t.Fatal(e)
						}
					}
				}
			}
			runs, err := js.Stream(ctx, "WF_RUN")
			if err != nil {
				t.Fatal(err)
			}
			if err = runs.Purge(ctx); err != nil {
				t.Fatal(err)
			}
			// Reopen canonical storage; repair has no original keys/body arguments.
			graph, err = journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			c, err = client.NewWithGraphJournal(js, graph)
			if err != nil {
				t.Fatal(err)
			}
			var effects atomic.Int64
			handlers := map[string]worker.Handler{"repair": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				data, e := wf.AwaitSignal(c, "signal")
				if e != nil {
					return nil, e
				}
				if string(data) != "42" {
					return nil, fmt.Errorf("unexpected Signal body %s", data)
				}
				value, e := wf.Run(c, "once", 0, func(context.Context) (int, error) { effects.Add(1); return 42, nil })
				if e != nil {
					return nil, e
				}
				return json.Marshal(value)
			}}
			w, err := worker.New(ctx, js, "signalrepair", handlers, worker.WithGraphJournal(graph))
			if err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithCancel(ctx)
			workerDone := make(chan error, 1)
			repairDone := make(chan error, 1)
			go func() { workerDone <- w.RunPartition(runCtx, 0) }()
			go func() {
				repairDone <- reconcile.RunRepairLoopWithGraphJournal(runCtx, js, "signalrepair", "graph-signal", 10*time.Millisecond, 1, graph, nil, nil, nil)
			}()
			defer func() {
				stop()
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
			for _, h := range handles {
				result, e := c.Await(ctx, h.Type, h.ID)
				if e != nil || string(result) != "42" {
					t.Fatal(h, result, e)
				}
				status, e := graph.InspectStart(ctx, h.Type, h.ID)
				if e != nil || status.SignalConsumed != 1 || status.SignalBindings != 1 {
					t.Fatal(status, e)
				}
			}
			if effects.Load() != 3 {
				t.Fatal("unexpected effects", effects.Load())
			}
		})
	}
}

type nativeSignalRepairSource struct{ js jetstream.JetStream }

func (s nativeSignalRepairSource) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := s.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, from, jetstream.WithGetMsgSubject(subject))
}
