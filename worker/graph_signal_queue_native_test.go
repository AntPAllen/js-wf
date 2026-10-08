package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

func TestNativeCanonicalSignalQueueWorkerReplay(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
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
			cfg := journal.NativeGraphConfig{AuthorityStream: "WORKER_QUEUE_AUTH", AuthorityPrefix: "wf.graph.workerqueue", ObjectBucket: "WORKER_QUEUE_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true}
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
			h, err := c.Start(ctx, "queued", "native", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			body := bytes.Repeat([]byte("x"), 5<<20)
			seq, err := c.Signal(ctx, h.Type, h.ID, "first", body, "key")
			if err != nil {
				t.Fatal(err)
			}
			sources, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				t.Fatal(err)
			}
			if err = sources.DeleteMsg(ctx, seq); err != nil {
				t.Fatal(err)
			}
			var effects atomic.Int64
			handlers := map[string]Handler{h.Type: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
				if _, e := wf.Run(c, "once", 0, func(context.Context) (int, error) { effects.Add(1); return 42, nil }); e != nil {
					return nil, e
				}
				data, e := wf.AwaitSignal(c, "first")
				if e != nil {
					return nil, e
				}
				if !bytes.Equal(data, body) {
					return nil, fmt.Errorf("owned Signal body mismatch")
				}
				if _, e = wf.AwaitSignal(c, "second"); e != nil {
					return nil, e
				}
				return json.RawMessage(`42`), nil
			}}
			run := func(id string, suspend bool) func() {
				suspended := make(chan struct{})
				var once sync.Once
				w, e := New(ctx, js, id, handlers, WithGraphJournal(store), WithOperationObserver(func(e OperationEvent) {
					if e.JournalKind == journal.Suspended && e.Operation == "journal_append" && e.Error == "" {
						once.Do(func() { close(suspended) })
					}
				}))
				if e != nil {
					t.Fatal(e)
				}
				runctx, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- w.RunPartition(runctx, identity.Partition(h.Type, h.ID, provision.Partitions)) }()
				cleanup := func() {
					stop()
					if e := <-done; e != nil {
						t.Fatal(e)
					}
					if e := w.Close(); e != nil {
						t.Fatal(e)
					}
				}
				if suspend {
					select {
					case <-suspended:
					case <-ctx.Done():
						cleanup()
						t.Fatal(ctx.Err())
					}
					cleanup()
					return func() {}
				}
				return cleanup
			}
			run("queue-first", true)
			records, _, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			consumed := 0
			for _, record := range records {
				if record.Kind == journal.SignalConsumed {
					var event signalRecord
					if json.Unmarshal(record.Payload, &event) != nil || event.Canonical == nil || event.Canonical.Index != 0 || event.Sequence != seq || event.Ref == "" {
						t.Fatal("consumption lacks canonical owned identity")
					}
					consumed++
				}
			}
			if consumed != 1 {
				t.Fatal("duplicate consumption", consumed)
			}
			// Replayed history must prove the same queue operation; neither a
			// forged locator nor matching legacy metadata can authorize it.
			g, e := openGraphDelivery(ctx, store, h.Type, h.ID, h.InvSeq)
			if e != nil {
				t.Fatal(e)
			}
			probe := &Worker{graphJournal: store, signalDrainPort: NewSignalDrainPort(js)}
			mutations := []func(*signalRecord){
				func(e *signalRecord) { e.Canonical = nil },
				func(e *signalRecord) { e.Canonical.Index++ },
				func(e *signalRecord) { e.Canonical.Token = "foreign" },
				func(e *signalRecord) { e.Sequence++ },
				func(e *signalRecord) { e.Name = "foreign" },
				func(e *signalRecord) { e.Hash = "foreign" },
				func(e *signalRecord) { e.Ref = "foreign" },
				func(e *signalRecord) { e.Ref = ""; e.Payload = append(json.RawMessage(nil), body...) },
				func(e *signalRecord) { e.Payload = append(json.RawMessage(nil), body...) },
			}
			for index, mutate := range mutations {
				hostile := append([]journal.Record(nil), records...)
				for i := range hostile {
					if hostile[i].Kind == journal.SignalConsumed {
						var event signalRecord
						if json.Unmarshal(hostile[i].Payload, &event) != nil {
							t.Fatal("bad fixture")
						}
						mutate(&event)
						hostile[i].Payload, _ = json.Marshal(event)
					}
				}
				writes := 0
				_, e = probe.drainCanonicalSignals(ctx, g, 0, hostile, func(journal.Kind, json.RawMessage) error { writes++; return nil }, nil)
				if !errors.Is(e, wf.ErrCorruptJournal) || writes != 0 {
					t.Fatal("forged canonical consumption accepted", index, writes, e)
				}
			}
			replayed, e := probe.drainCanonicalSignals(ctx, g, 0, records, func(journal.Kind, json.RawMessage) error { return fmt.Errorf("duplicate consumption") }, nil)
			if e != nil || len(replayed) != 1 || !bytes.Equal(replayed[0].Payload, body) {
				t.Fatal("healthy replay rejected", e)
			}
			if e = g.close(ctx); e != nil {
				t.Fatal(e)
			}
			fresh, err := journal.OpenNativeGraphStore(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			store = fresh
			c, err = client.NewWithGraphJournal(js, store)
			if err != nil {
				t.Fatal(err)
			}
			duplicate, err := c.Signal(ctx, h.Type, h.ID, "first", body, "key")
			if err != nil || duplicate != seq {
				t.Fatal(duplicate, err)
			}
			if _, err = c.Signal(ctx, h.Type, h.ID, "second", []byte(`true`), "next"); err != nil {
				t.Fatal(err)
			}
			stop := run("queue-second", false)
			result, err := c.Await(ctx, h.Type, h.ID)
			stop()
			if err != nil || string(result) != "42" || effects.Load() != 1 {
				t.Fatal(string(result), effects.Load(), err)
			}
			records, _, err = store.Read(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			consumed = 0
			for _, record := range records {
				if record.Kind == journal.SignalConsumed {
					consumed++
				}
			}
			if consumed != 2 {
				t.Fatal("replay consumed duplicates", consumed)
			}
			blobs, err := js.Stream(ctx, "OBJ_WF_BLOB")
			if err != nil {
				t.Fatal(err)
			}
			info, err := blobs.Info(ctx)
			if err != nil || info.State.Msgs != 0 {
				t.Fatal("legacy blob bytes written", err)
			}
			// Cancellation is itself a canonical queued operation and must be
			// applied before entering the handler.
			h, err = c.Start(ctx, "queued", "cancelled", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Cancel(ctx, h.Type, h.ID); err != nil {
				t.Fatal(err)
			}
			stop = run("queue-cancel", false)
			_, err = c.Await(ctx, h.Type, h.ID)
			stop()
			if !errors.Is(err, client.ErrCancelled) || effects.Load() != 1 {
				t.Fatal("canonical cancellation entered handler", effects.Load(), err)
			}
			t.Logf("R%d: purged source consumed as owned 5MiB Signal; suspended worker reopened; exactly two consumptions, one effect and result 42", replicas)
		})
	}
}
