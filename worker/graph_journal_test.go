package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

func TestNativeGraphWorkerReplayInputsSignalsAndResults(t *testing.T) {
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
						ready = ready || (s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas)
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
			if _, err = js.CreateStream(ctx, graphpublication.AuthorityStreamConfig("GRAPH_WORKER_AUTH", "wf.graph.worker", replicas)); err != nil {
				t.Fatal(err)
			}
			if _, err = js.CreateStream(ctx, graphpublication.NativeObjectStreamConfig("GRAPH_WORKER_OBJECTS", replicas)); err != nil {
				t.Fatal(err)
			}
			auth, err := graphpublication.OpenNativeAuthority(ctx, js, "GRAPH_WORKER_AUTH", "wf.graph.worker")
			if err != nil {
				t.Fatal(err)
			}
			port, err := graphpublication.OpenNativePort(ctx, auth, "GRAPH_WORKER_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			p := graphpublication.Protocol{Port: port}
			store, err := journal.OpenNativeGraphStore(ctx, js, journal.NativeGraphConfig{AuthorityStream: "GRAPH_WORKER_AUTH", AuthorityPrefix: "wf.graph.worker", ObjectBucket: "GRAPH_WORKER_OBJECTS", ExpectedReplicas: replicas, Now: func() time.Time { return now }, PinTTL: time.Minute, IntentTTL: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			c, err := client.NewWithGraphJournal(js, store)
			if err != nil {
				t.Fatal(err)
			}
			input, _ := json.Marshal(strings.Repeat("i", client.MaxInlineInput+2048))
			resultText := strings.Repeat("r", wf.MaxInlineResult+2048)
			terminal, _ := json.Marshal(resultText)
			largeSignal, _ := json.Marshal(strings.Repeat("s", client.MaxInlineSignal+2048))
			var effects atomic.Int64
			handlers := map[string]Handler{"graph": func(c *wf.Context, got json.RawMessage) (json.RawMessage, error) {
				if !bytes.Equal(got, input) {
					return nil, fmt.Errorf("graph input mismatch")
				}
				value, err := wf.Run(c, "large", 7, func(context.Context) (string, error) { effects.Add(1); return resultText, nil })
				if err != nil {
					return nil, err
				}
				if value != resultText {
					return nil, fmt.Errorf("graph result mismatch")
				}
				signal, err := wf.AwaitSignal(c, "first")
				if err != nil {
					return nil, err
				}
				if !bytes.Equal(signal, largeSignal) {
					return nil, fmt.Errorf("graph signal mismatch")
				}
				if _, err = wf.AwaitSignal(c, "second"); err != nil {
					return nil, err
				}
				return terminal, nil
			}}
			unsupported := []Option{
				WithContinuations("graph", map[string]ContinuationHandler{"next": func(*wf.Context, json.RawMessage, json.RawMessage) (json.RawMessage, error) { return nil, nil }}),
				WithJournalEncoding(journal.JSON), WithJournalStore(journal.New(js)), WithResultBlobPort(NewResultBlobPort(js)),
			}
			for i, option := range unsupported {
				for _, reverse := range []bool{false, true} {
					options := []Option{WithGraphJournal(store), option}
					if reverse {
						options[0], options[1] = options[1], options[0]
					}
					if _, err := New(ctx, js, "invalid-graph-options", handlers, options...); err == nil {
						t.Fatalf("unsupported graph option %d/order=%v accepted", i, reverse)
					}
				}
			}
			handle, err := c.Start(ctx, "graph", "native", input)
			if err != nil {
				t.Fatal(err)
			}
			invocation, err := js.Stream(ctx, "WF_INV")
			if err != nil {
				t.Fatal(err)
			}
			original, err := invocation.GetLastMsgForSubject(ctx, identity.InvocationSubject("graph", "native"))
			if err != nil {
				t.Fatal(err)
			}
			runStage := func(workerID string, doneWhenSuspended bool) (*Worker, func()) {
				t.Helper()
				suspended := make(chan struct{})
				firstError := make(chan string, 1)
				var once sync.Once
				w, err := New(ctx, js, workerID, handlers, WithGraphJournal(store), WithOperationObserver(func(e OperationEvent) {
					if e.Error != "" {
						select {
						case firstError <- e.Operation + ": " + e.Error:
						default:
						}
					}
					if e.Operation == "journal_append" && e.JournalKind == journal.Suspended && e.Error == "" {
						once.Do(func() { close(suspended) })
					}
				}))
				if err != nil {
					t.Fatal(err)
				}
				runCtx, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- w.RunPartition(runCtx, identity.Partition("graph", "native", provision.Partitions)) }()
				cleanup := func() {
					stop()
					if err := <-done; err != nil {
						t.Fatal(err)
					}
					if err := w.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if doneWhenSuspended {
					select {
					case <-suspended:
					case message := <-firstError:
						cleanup()
						t.Fatal(message)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					cleanup()
					return w, func() {}
				}
				return w, cleanup
			}
			runStage("graph-first", true)
			blobs, err := js.ObjectStore(ctx, "WF_BLOB")
			if err != nil {
				t.Fatal(err)
			}
			// Replay must use the journal-owned input after the original staging bytes disappear.
			if err = blobs.Delete(ctx, original.Header.Get("Wf-Input-Ref")); err != nil {
				t.Fatal(err)
			}
			if _, err = c.Signal(ctx, "graph", "native", "first", largeSignal, "signal-first"); err != nil {
				t.Fatal(err)
			}
			runStage("graph-second", true)
			signals, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				t.Fatal(err)
			}
			signalMsg, err := signals.GetLastMsgForSubject(ctx, "wf.sig.graph.native.first")
			if err != nil {
				t.Fatal(err)
			}
			if err = blobs.Delete(ctx, signalMsg.Header.Get("Wf-Signal-Ref")); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Minute)
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			if _, err = c.Signal(ctx, "graph", "native", "second", []byte(`true`), "signal-second"); err != nil {
				t.Fatal(err)
			}
			_, stopLast := runStage("graph-third", false)
			actual, err := c.Await(ctx, "graph", "native")
			if err != nil || !bytes.Equal(actual, terminal) {
				stopLast()
				t.Fatal(len(actual), err)
			}
			stopLast()
			if effects.Load() != 1 {
				t.Fatal("effect reran", effects.Load())
			}
			records, tail, err := store.Read(ctx, "graph", "native", handle.InvSeq)
			if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
				t.Fatal(len(records), err)
			}
			legacy, err := js.Stream(ctx, "WF_JRN")
			if err != nil {
				t.Fatal(err)
			}
			info, err := legacy.Info(ctx)
			if err != nil || info.State.Msgs != 0 {
				t.Fatal("graph delivery wrote legacy journal", err)
			}
			// Retention migration is still pending: retire only after the terminal's
			// legacy invocation and state are deliberately removed in this closed fixture.
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			if err = state.Delete(ctx, identity.Key("graph", "native")); err != nil {
				t.Fatal(err)
			}
			// The graph terminal remains authoritative with the legacy mirror gone.
			actual, err = c.Await(ctx, "graph", "native")
			if err != nil || !bytes.Equal(actual, terminal) {
				t.Fatal("missing legacy state hid graph result", err)
			}
			leaseKV, err := js.KeyValue(ctx, "WF_LEASE")
			if err != nil {
				t.Fatal(err)
			}
			for _, mirror := range []string{"absent", "forged", "owned"} {
				if mirror == "forged" {
					if err = state.Delete(ctx, identity.Key("graph", "native")); err != nil {
						t.Fatal(err)
					}
					forged, _ := json.Marshal(wf.Outcome{InvSeq: handle.InvSeq + 100, Error: "forged legacy failure"})
					if _, err = state.Create(ctx, identity.Key("graph", "native"), forged); err != nil {
						t.Fatal(err)
					}
				}
				probeCtx, stopProbe := context.WithTimeout(ctx, 10*time.Second)
				decisions := make(chan DispatchEvent, 1)
				probe, err := New(ctx, js, "graph-terminal-"+mirror, handlers, WithGraphJournal(store), WithDispatchObserver(func(e DispatchEvent) {
					if e.Stage == "ack" || e.Stage == "nak" {
						select {
						case decisions <- e:
						default:
						}
						stopProbe()
					}
				}))
				if err != nil {
					stopProbe()
					t.Fatal(err)
				}
				var owner *lease.Lease
				var before jetstream.KeyValueEntry
				if mirror == "owned" {
					// Seed the local hint from the canonical outcome already observed above.
					// It must still validate durable authority before acknowledging.
					probe.terminalHints.add(identity.Key("graph", "native"), true)
				} else {
					owner, err = probe.leases.Acquire(ctx, "graph", "native", "healthy-owner")
					if err != nil {
						stopProbe()
						t.Fatal(err)
					}
					// Anchor the baseline to the acknowledged owner initialization,
					// rather than accepting an older KV GET as the current lease.
					want, _ := json.Marshal(lease.Value{Worker: "healthy-owner", Epoch: owner.Epoch()})
					for read := 0; read < 6; read++ {
						before, err = leaseKV.Get(ctx, identity.Key("graph", "native"))
						if err != nil {
							t.Fatal(err)
						}
						if bytes.Equal(before.Value(), want) && before.Revision() > owner.Epoch() {
							break
						}
						if before.Revision() > owner.Epoch() {
							t.Fatalf("foreign lease changed before probe: mirror=%s epoch=%d revision=%d value=%q", mirror, owner.Epoch(), before.Revision(), before.Value())
						}
						t.Logf("older lease GET before probe: mirror=%s acknowledged epoch=%d observed revision=%d value=%q", mirror, owner.Epoch(), before.Revision(), before.Value())
						time.Sleep(20 * time.Millisecond)
					}
					if !bytes.Equal(before.Value(), want) || before.Revision() <= owner.Epoch() {
						t.Fatalf("acknowledged foreign lease not visible: mirror=%s epoch=%d revision=%d value=%q", mirror, owner.Epoch(), before.Revision(), before.Value())
					}
				}
				if _, err = js.Publish(ctx, identity.RunSubject("graph", "native", provision.Partitions), []byte(identity.Key("graph", "native"))); err != nil {
					t.Fatal(err)
				}
				if err = probe.RunPartition(probeCtx, identity.Partition("graph", "native", provision.Partitions)); err != nil {
					t.Fatal(err)
				}
				if mirror == "absent" {
					repaired, e := state.Get(ctx, identity.Key("graph", "native"))
					if e != nil || !bytes.Equal(repaired.Value(), records[len(records)-1].Payload) {
						t.Fatal("canonical terminal projection not repaired", e)
					}
				}

				stopProbe()
				select {
				case decision := <-decisions:
					if decision.Stage != "ack" || decision.Error != "" {
						t.Fatal("canonical terminal probe failed", mirror, decision)
					}
				default:
					t.Fatal("canonical terminal probe did not decide", mirror)
				}
				if mirror == "owned" {
					if probe.Metrics().LeaseAcquisitions != 1 || probe.Metrics().LeaseContentions != 0 {
						t.Fatal("owned duplicate path not exercised", probe.Metrics())
					}
				} else {
					after, err := leaseKV.Get(ctx, identity.Key("graph", "native"))
					if err != nil {
						t.Fatal("foreign lease read after probe", err)
					}
					for read := 0; read < 5 && after.Revision() < before.Revision(); read++ {
						t.Logf("older lease GET after probe: mirror=%s baseline=%d observed revision=%d value=%q", mirror, before.Revision(), after.Revision(), after.Value())
						time.Sleep(20 * time.Millisecond)
						after, err = leaseKV.Get(ctx, identity.Key("graph", "native"))
						if err != nil {
							t.Fatal("foreign lease read after probe", err)
						}
					}
					if before.Revision() != after.Revision() || !bytes.Equal(before.Value(), after.Value()) {
						t.Fatalf("terminal ACK changed foreign lease: mirror=%s before=%d/%q after=%d/%q metrics=%+v", mirror, before.Revision(), before.Value(), after.Revision(), after.Value(), probe.Metrics())
					}
					if probe.Metrics().LeaseContentions != 1 {
						t.Fatal("probe bypassed held lease", probe.Metrics())
					}
					if err = owner.Renew(ctx); err != nil {
						t.Fatal(err)
					}
					if err = owner.Release(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if effects.Load() != 1 {
					t.Fatal("duplicate reran effect", effects.Load())
				}
			}
			if err = state.Delete(ctx, identity.Key("graph", "native")); err != nil {
				t.Fatal(err)
			}
			if err = invocation.Purge(ctx, jetstream.WithPurgeSubject(identity.InvocationSubject("graph", "native"))); err != nil {
				t.Fatal(err)
			}
			if err = store.Retire(ctx, "graph", "native", handle.InvSeq, tail); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Minute)
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			objects, err := port.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal("graph objects remain", len(objects), err)
			}
			physical, err := js.Stream(ctx, "OBJ_GRAPH_WORKER_OBJECTS")
			if err != nil {
				t.Fatal(err)
			}
			physicalInfo, err := physical.Info(ctx, jetstream.WithSubjectFilter("$O.GRAPH_WORKER_OBJECTS.>"))
			if err != nil {
				t.Fatal(err)
			}
			for subject := range physicalInfo.State.Subjects {
				if strings.Contains(subject, ".C.") {
					t.Fatal("graph chunks remain", subject)
				}
			}
		})
	}
}
