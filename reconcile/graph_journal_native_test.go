package reconcile_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func TestNativeGraphReconcileRepairsStartSignalAndTimerHistory(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d", replicas), func(t *testing.T) {
			cluster, e := testcluster.Start(t.TempDir(), replicas)
			if e != nil {
				t.Fatal(e)
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
			js, e := jetstream.New(cluster.Clients[0])
			if e != nil {
				t.Fatal(e)
			}
			if e = provision.Ensure(ctx, js, replicas); e != nil {
				t.Fatal(e)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "RECONCILE_GRAPH_AUTH", AuthorityPrefix: "wf.graph.reconcile", ObjectBucket: "RECONCILE_GRAPH_OBJECTS", ExpectedReplicas: replicas}
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
			const typ, id = "graph", "repair"
			input := []byte(`7`)
			hash := sha256.Sum256(input)
			// Publish only the invocation: no initial dispatch exists.
			inv, e := js.PublishMsg(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: input, Header: nats.Header{"Wf-Input-SHA256": {hex.EncodeToString(hash[:])}}})
			if e != nil {
				t.Fatal(e)
			}
			start, e := reconcile.NewStartScanWithGraphJournal(js, graph)
			if e != nil {
				t.Fatal(e)
			}
			result, e := start.Scan(ctx, inv.Sequence, 1, false)
			if e != nil || result.Reenqueued != 1 {
				t.Fatal(result, e)
			}
			var effects atomic.Int64
			handlers := map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
				if !bytes.Equal(input, []byte(`7`)) {
					return nil, fmt.Errorf("input differs")
				}
				value, e := wf.Run(c, "once", 1, func(context.Context) (int, error) { effects.Add(1); return 7, nil })
				if e != nil || value != 7 {
					return nil, fmt.Errorf("effect replay differs: %v", e)
				}
				return wf.AwaitSignal(c, "go")
			}}
			run := func(name string) {
				runCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				var stage string
				w, e := worker.New(ctx, js, name, handlers, worker.WithGraphJournal(graph), worker.WithDispatchObserver(func(event worker.DispatchEvent) {
					if event.Stage == "ack" || event.Stage == "nak" {
						stage = event.Stage
						cancel()
					}
				}))
				if e != nil {
					t.Fatal(e)
				}
				if e = w.RunPartition(runCtx, identity.Partition(typ, id, provision.Partitions)); e != nil {
					t.Fatal(e)
				}
				if stage != "ack" {
					t.Fatalf("repair worker decision=%s", stage)
				}
			}
			run("graph-start-repair")
			records, _, e := graph.Read(ctx, typ, id, inv.Sequence)
			if e != nil || records[len(records)-1].Kind != journal.Suspended || effects.Load() != 1 {
				t.Fatal(records, e, effects.Load())
			}
			result, e = start.Scan(ctx, inv.Sequence, 1, false)
			if e != nil || result.Reenqueued != 0 {
				t.Fatal("graph start was re-enqueued", result, e)
			}
			// Publish retained signal only, after the first worker has joined.
			signal, e := js.PublishMsg(ctx, &nats.Msg{Subject: "wf.sig.graph.repair.go", Data: []byte(`42`), Header: nats.Header{"Wf-Inv-Seq": {strconv.FormatUint(inv.Sequence, 10)}}})
			if e != nil {
				t.Fatal(e)
			}
			signalScan, e := reconcile.NewSignalScanWithGraphJournal(js, graph)
			if e != nil {
				t.Fatal(e)
			}
			result, e = signalScan.Scan(ctx, signal.Sequence, 1, true)
			if e != nil || result.Reenqueued != 1 {
				t.Fatal(result, e)
			}
			suspended, e := reconcile.NewSuspendedScanWithGraphJournal(js, graph)
			if e != nil {
				t.Fatal(e)
			}
			result, e = suspended.Scan(ctx, inv.Sequence, 1, true)
			if e != nil || result.Reenqueued != 1 || result.Candidates[0].Reason != "signal" {
				t.Fatal(result, e)
			}
			result, e = signalScan.Scan(ctx, signal.Sequence, 1, false)
			if e != nil || result.Reenqueued != 1 {
				t.Fatal(result, e)
			}
			run("graph-signal-repair")
			state, e := js.KeyValue(ctx, "WF_STATE")
			if e != nil {
				t.Fatal(e)
			}
			if e = state.Delete(ctx, identity.Key(typ, id)); e != nil {
				t.Fatal(e)
			}
			c, e := client.NewWithGraphJournal(js, graph)
			if e != nil {
				t.Fatal(e)
			}
			outcome, e := c.Await(ctx, typ, id)
			if e != nil || !bytes.Equal(outcome, []byte(`42`)) || effects.Load() != 1 {
				t.Fatal(string(outcome), e, effects.Load())
			}
			result, e = signalScan.Scan(ctx, signal.Sequence, 1, false)
			if e != nil || result.Reenqueued != 0 {
				t.Fatal("terminal graph signal wakeup", result, e)
			}
			result, e = suspended.Scan(ctx, inv.Sequence, 1, false)
			if e != nil || result.Reenqueued != 0 {
				t.Fatal("terminal graph suspension wakeup", result, e)
			}
			legacy, _, e := journal.New(js).Read(ctx, typ, id)
			if e != nil || len(legacy) != 0 {
				t.Fatal("legacy journal written", legacy, e)
			}

			// Prepared timer history exercises native scanner decisions; the timer
			// fixture's repair publications deliberately remain pending.
			timerInv, e := js.PublishMsg(ctx, &nats.Msg{Subject: "wf.inv.graph.timer", Data: input, Header: nats.Header{"Wf-Input-SHA256": {hex.EncodeToString(hash[:])}}})
			if e != nil {
				t.Fatal(e)
			}
			tail, e := graph.Begin(ctx, typ, "timer", timerInv.Sequence)
			if e != nil {
				t.Fatal(e)
			}
			request, _ := json.Marshal(map[string]any{"kind": "timer", "name": "wake", "fire_at": time.Now().Add(-time.Hour)})
			entries := []journal.Entry{{Kind: journal.Started}, {Kind: journal.StepRequested, Index: 1, Payload: request}, {Kind: journal.Suspended, Index: 2, Payload: []byte(`{"waiting_on":"timer:wake"}`)}}
			for _, entry := range entries {
				tail, e = graph.Append(ctx, typ, "timer", timerInv.Sequence, entry, tail, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
			}
			// A forged legacy terminal must not suppress either graph repair.
			_, e = journal.New(js).Append(ctx, typ, "timer", journal.Entry{Kind: journal.Started}, 0)
			if e != nil {
				t.Fatal(e)
			}
			_, e = journal.New(js).Append(ctx, typ, "timer", journal.Entry{Kind: journal.Failed, Index: 1}, 1)
			if e != nil {
				t.Fatal(e)
			}
			timers, e := reconcile.NewTimerScanWithGraphJournal(js, graph)
			if e != nil {
				t.Fatal(e)
			}
			result, e = timers.Scan(ctx, timerInv.Sequence, 1, false)
			if e != nil || result.Reenqueued != 1 {
				t.Fatal(result, e)
			}
			result, e = suspended.Scan(ctx, timerInv.Sequence, 1, false)
			if e != nil || result.Reenqueued != 1 || result.Candidates[0].Reason != "timer" {
				t.Fatal(result, e)
			}
			tail, e = graph.Append(ctx, typ, "timer", timerInv.Sequence, journal.Entry{Kind: journal.Failed, Index: 3}, tail, nil, nil)
			if e != nil {
				t.Fatal(e)
			}
			hint, e := js.PublishMsg(ctx, &nats.Msg{Subject: "wf.schedule.graph.timer.1", Data: []byte("graph.timer"), Header: nats.Header{identity.TimerInvSeqHeader: {fmt.Sprint(timerInv.Sequence)}, identity.TimerStepHeader: {"1"}}})
			if e != nil {
				t.Fatal(e)
			}
			result, e = timers.Scan(ctx, timerInv.Sequence, 1, false)
			if e != nil || result.Reenqueued != 0 || result.Removed != 1 {
				t.Fatal(result, e)
			}
			runStream, e := js.Stream(ctx, "WF_RUN")
			if e != nil {
				t.Fatal(e)
			}
			if _, e = runStream.GetMsg(ctx, hint.Sequence); e != jetstream.ErrMsgNotFound {
				t.Fatal("terminal hint retained", e)
			}
			t.Log("actual start/signal repairs recover graph worker once; canonical client reads without state; prepared timer/suspended repairs and terminal hint retirement")
		})
	}
}
