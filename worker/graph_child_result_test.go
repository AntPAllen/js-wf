package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
	"js-wf/wf"
)

func TestNativeGraphChildResultTransferAndReplay(t *testing.T) {
	testNativeGraphChildResultTransferAndReplay(t, false)
}
func TestNativeCanonicalSignalQueueChildTransferAndReplay(t *testing.T) {
	testNativeGraphChildResultTransferAndReplay(t, true)
}
func testNativeGraphChildResultTransferAndReplay(t *testing.T, canonical bool) {
	for _, replicas := range []int{1, 3} {
		for _, scenario := range []struct{ async, externalResult bool }{{false, true}, {true, true}, {false, false}, {true, false}} {
			async, externalResult := scenario.async, scenario.externalResult
			t.Run(fmt.Sprintf("R%d/async=%v/external-result=%v", replicas, async, externalResult), func(t *testing.T) {
				cluster, err := testcluster.Start(t.TempDir(), replicas)
				if err != nil {
					t.Fatal(err)
				}
				defer cluster.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
				cfg := journal.NativeGraphConfig{AuthorityStream: "GRAPH_CHILD_AUTH", AuthorityPrefix: "wf.graph.child", ObjectBucket: "GRAPH_CHILD_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: canonical, CanonicalSignals: canonical, PinTTL: time.Minute, IntentTTL: time.Minute}
				now := time.Now().UTC()
				cfg.Now = func() time.Time { return now }
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
				auth, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
				if err != nil {
					t.Fatal(err)
				}
				port, err := graphpublication.OpenNativePort(ctx, auth, cfg.ObjectBucket)
				if err != nil {
					t.Fatal(err)
				}
				protocol := graphpublication.Protocol{Port: port}
				c, err := client.NewWithGraphJournal(js, graph)
				if err != nil {
					t.Fatal(err)
				}
				handle, err := c.Start(ctx, "parent", "transfer", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				parentPartition := identity.Partition("parent", "transfer", provision.Partitions)
				childType := "child"
				var childID string
				for {
					sum := sha256.Sum256([]byte(fmt.Sprintf("parent:transfer:%d:%s:0", handle.InvSeq, childType)))
					childID = "c-" + hex.EncodeToString(sum[:16])
					if identity.Partition(childType, childID, provision.Partitions) != parentPartition {
						break
					}
					childType += "x"
				}
				result, _ := json.Marshal(strings.Repeat("child-result", wf.MaxInlineResult/12+4096))
				if !externalResult {
					result, _ = json.Marshal(strings.Repeat("s", 500*1024))
				}
				if (len(result) > wf.MaxInlineTerminal) != externalResult {
					t.Fatal("incorrect result shape")
				}
				var effects atomic.Int64
				handlers := map[string]Handler{
					childType: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
						effects.Add(1)
						if !bytes.Equal(input, []byte(`9`)) {
							return nil, fmt.Errorf("child input differs")
						}
						return result, nil
					},
					"parent": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
						if !bytes.Equal(input, []byte(`7`)) {
							return nil, fmt.Errorf("parent input differs")
						}
						var got []byte
						var err error
						if async {
							var promise wf.Promise
							promise, err = wf.CallAsync(c, childType, []byte(`9`))
							if err == nil {
								got, err = wf.AwaitPromise(c, promise)
							}
							if err == nil {
								again, e := wf.AwaitPromise(c, promise)
								if e != nil || !bytes.Equal(again, got) {
									return nil, fmt.Errorf("repeated promise differs: %v", e)
								}
							}
						} else {
							got, err = wf.Call(c, childType, []byte(`9`))
						}
						if err != nil {
							return nil, err
						}
						if !bytes.Equal(got, result) {
							return nil, fmt.Errorf("child result differs")
						}
						if _, err = wf.AwaitSignal(c, "release"); err != nil {
							return nil, err
						}
						return []byte(`42`), nil
					},
				}
				stage := func(name string, partition uint32) {
					t.Helper()
					runCtx, stop := context.WithCancel(ctx)
					defer stop()
					var decision DispatchEvent
					w, err := New(ctx, js, name, handlers, WithGraphJournal(graph), WithDispatchObserver(func(e DispatchEvent) {
						if e.Stage == "ack" || e.Stage == "nak" {
							decision = e
							stop()
						}
					}))
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					if err = w.RunPartition(runCtx, partition); err != nil {
						t.Fatal(err)
					}
					if decision.Stage != "ack" || decision.Error != "" {
						t.Fatal("stage did not ACK", name, decision)
					}
				}
				stage("parent-first", parentPartition)
				inv, err := js.Stream(ctx, "WF_INV")
				if err != nil {
					t.Fatal(err)
				}
				childInput, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(childType, childID))
				if err != nil {
					t.Fatal(err)
				}
				stage("child", identity.Partition(childType, childID, provision.Partitions))
				childRecords, _, err := graph.Read(ctx, childType, childID, childInput.Sequence)
				if err != nil || len(childRecords) < 2 || childRecords[len(childRecords)-1].Kind != journal.Completed {
					t.Fatal("child did not complete", err)
				}
				var outcome wf.Outcome
				if err = json.Unmarshal(childRecords[len(childRecords)-1].Payload, &outcome); err != nil || (outcome.ResultRef != "") != externalResult {
					t.Fatal("child result shape differs", err)
				}
				childView, err := graph.OpenTerminal(ctx, childType, childID, childInput.Sequence)
				if err != nil {
					t.Fatal(err)
				}
				childRecord, err := childView.Read(ctx, childView.Count()-1)
				if err != nil {
					t.Fatal(err)
				}
				sourcePayload := childRecord.EntryBlob.Reference.Object
				for _, link := range childRecord.Blobs {
					if link.Hash == outcome.ResultHash {
						sourcePayload = link.Reference.Object
					}
				}
				if sourcePayload == "" {
					t.Fatal("missing source result edge")
				}
				if err = childView.Close(ctx); err != nil {
					t.Fatal(err)
				}
				if err = retention.PurgeGraph(ctx, js, graph, childType, childID, time.Minute); !errors.Is(err, retention.ErrNotTerminal) {
					t.Fatal("child purged before parent ownership", err)
				}
				stage("parent-transfer", parentPartition)
				parentRecords, _, err := graph.Read(ctx, "parent", "transfer", handle.InvSeq)
				if err != nil || parentRecords[len(parentRecords)-1].Kind != journal.Suspended {
					t.Fatal("parent did not suspend after transfer", err)
				}
				transferred := false
				for _, record := range parentRecords {
					if record.Kind == journal.SignalConsumed {
						var signal signalRecord
						if json.Unmarshal(record.Payload, &signal) != nil {
							t.Fatal("bad signal")
						}
						if signal.Child != nil {
							if signal.Child.Type != childType || signal.Child.ID != childID || signal.Child.Invocation != childInput.Sequence || signal.Child.Ref != outcome.ResultRef || signal.Child.Hash != outcome.ResultHash {
								t.Fatal("bad child provenance", signal.Child)
							}
							transferred = true
						}
					}
				}
				if !transferred {
					t.Fatal("missing parent child ownership declaration")
				}
				state, err := js.KeyValue(ctx, "WF_STATE")
				if err != nil {
					t.Fatal(err)
				}
				if err = state.Delete(ctx, identity.Key(childType, childID)); err != nil {
					t.Fatal(err)
				}
				if err = retention.PurgeGraph(ctx, js, graph, childType, childID, time.Minute); err != nil {
					t.Fatal("production child graph purge", err)
				}
				signals, err := js.Stream(ctx, "WF_SIG")
				if err != nil {
					t.Fatal(err)
				}
				signalMessage, err := signals.GetLastMsgForSubject(ctx, "wf.sig.parent.transfer.child_0")
				if err != nil {
					t.Fatal(err)
				}
				stagingRef := signalMessage.Header.Get("Wf-Signal-Ref")
				if !canonical && !externalResult && stagingRef == "" {
					t.Fatal("inline child outcome did not use external signal staging")
				}
				if stagingRef != "" {
					blobs, err := js.ObjectStore(ctx, "WF_BLOB")
					if err != nil {
						t.Fatal(err)
					}
					if err = blobs.Delete(ctx, stagingRef); err != nil {
						t.Fatal(err)
					}
				}
				if err = signals.Purge(ctx, jetstream.WithPurgeSubject("wf.sig.parent.transfer.child_0")); err != nil {
					t.Fatal(err)
				}
				now = now.Add(2 * time.Minute)
				if _, err = protocol.SweepWithReaders(ctx, now); err != nil {
					t.Fatal(err)
				}
				objects, err := port.Objects(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, object := range objects {
					if object.Reference.Object == sourcePayload {
						t.Fatal("child source payload survived collection")
					}
				}
				if len(objects) == 0 {
					t.Fatal("collector removed parent ownership")
				}
				if _, err = c.Signal(ctx, "parent", "transfer", "release", []byte(`true`), "release"); err != nil {
					t.Fatal(err)
				}
				stage("parent-replay", parentPartition)
				actual, err := c.Await(ctx, "parent", "transfer")
				if err != nil || !bytes.Equal(actual, []byte(`42`)) {
					t.Fatal("parent replay without child failed", err)
				}
				if effects.Load() != 1 {
					t.Fatal("child reran", effects.Load())
				}
				legacy, err := js.Stream(ctx, "WF_JRN")
				if err != nil {
					t.Fatal(err)
				}
				info, err := legacy.Info(ctx)
				if err != nil || info.State.Msgs != 0 {
					t.Fatal("legacy journal was used", err)
				}
				if err = state.Delete(ctx, identity.Key("parent", "transfer")); err != nil {
					t.Fatal(err)
				}
				if err = retention.PurgeGraph(ctx, js, graph, "parent", "transfer", time.Minute); err != nil {
					t.Fatal("production parent graph purge", err)
				}
				now = now.Add(2 * time.Minute)
				if _, err = protocol.SweepWithReaders(ctx, now); err != nil {
					t.Fatal(err)
				}
				objects, err = port.Objects(ctx)
				if err != nil || len(objects) != 0 {
					t.Fatal("graph did not drain", len(objects), err)
				}
				physical, err := js.Stream(ctx, "OBJ_"+cfg.ObjectBucket)
				if err != nil {
					t.Fatal(err)
				}
				physicalInfo, err := physical.Info(ctx, jetstream.WithSubjectFilter("$O."+cfg.ObjectBucket+".>"))
				if err != nil {
					t.Fatal(err)
				}
				for subject := range physicalInfo.State.Subjects {
					if strings.Contains(subject, ".C.") {
						t.Fatal("physical graph chunks remain", subject)
					}
				}
			})
		}
	}
}

func TestGraphChildReplayRejectsForgedProvenance(t *testing.T) {
	request := journal.Record{Entry: journal.Entry{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"call_async","name":"child_0","child_type":"child","child_id":"c-native"}`)}}
	body, _ := json.Marshal(wf.Outcome{InvSeq: 7, Result: []byte(`42`)})
	for _, limit := range []bool{false, true} {
		for _, mode := range []string{"valid", "missing", "foreign_type", "foreign_id", "foreign_generation", "foreign_reference", "duplicate_request", "ordinary_signal", "ordinary_forged_declaration"} {
			t.Run(fmt.Sprintf("limit=%v/%s", limit, mode), func(t *testing.T) {
				g := &graphDelivery{records: []journal.Record{request}}
				event := signalRecord{Sequence: 3, Name: "child_0", Payload: body, Child: &graphChildResult{Type: "child", ID: "c-native", Invocation: 7}}
				switch mode {
				case "missing":
					event.Child = nil
				case "foreign_type":
					event.Child.Type = "foreign"
				case "foreign_id":
					event.Child.ID = "foreign"
				case "foreign_generation":
					event.Child.Invocation++
				case "foreign_reference":
					event.Child.Ref = "terminal-result-" + graphHash([]byte(`foreign`))
					event.Child.Hash = graphHash([]byte(`foreign`))
				case "duplicate_request":
					g.records = append(g.records, request)
				case "ordinary_signal":
					event.Name = "user"
					event.Child = nil
					event.Payload = []byte(`{"result_ref":"unowned-user-string","result_hash":"arbitrary"}`)
				case "ordinary_forged_declaration":
					event.Name = "user"
				}
				payload, _ := json.Marshal(event)
				entry := journal.Entry{Kind: journal.SignalConsumed, Payload: payload}
				if limit {
					entry.Kind = journal.Failed
					entry.Payload, _ = json.Marshal(wf.Outcome{InvSeq: 1, Error: journal.ErrTooLong.Error(), LimitEntry: &wf.LimitEntry{Kind: string(journal.SignalConsumed), Payload: payload}})
				}
				err := g.validateChildSignal(context.Background(), entry)
				valid := mode == "valid" || mode == "ordinary_signal"
				if (err == nil) != valid {
					t.Fatal("provenance acceptance differs", err)
				}
				if mode == "ordinary_signal" {
					refs, err := graphReferences(entry)
					if err != nil || len(refs) != 0 {
						t.Fatal("user strings became ownership edges", refs, err)
					}
				}
			})
		}
	}
}

func TestGraphSelectedChildRequiresExactRecordedSignal(t *testing.T) {
	body := []byte(`{"inv_seq":7,"result":"NDI="}`)
	child := &graphChildResult{Type: "child", ID: "c-native", Invocation: 7}
	for _, external := range []bool{false, true} {
		for _, mode := range []string{"valid", "missing", "foreign_sequence", "foreign_name", "foreign_bytes", "ordinary_signal"} {
			t.Run(fmt.Sprintf("external=%v/%s", external, mode), func(t *testing.T) {
				event := signalRecord{Sequence: 3, Name: "child_0", Payload: body, Child: child}
				if external {
					event.Payload = nil
					event.Ref = "signal-owned"
					event.Hash = graphHash(body)
				}
				g := &graphDelivery{childSignals: map[uint64]signalRecord{3: event}}
				selected := wf.Signal{Sequence: 3, Name: "child_0", Payload: body}
				switch mode {
				case "missing":
					delete(g.childSignals, 3)
				case "foreign_sequence":
					selected.Sequence = 4
				case "foreign_name":
					selected.Name = "foreign"
				case "foreign_bytes":
					selected.Payload = []byte(`{"inv_seq":8}`)
				case "ordinary_signal":
					event.Child = nil
					g.childSignals[3] = event
				}
				err := g.validateSelectedChild(context.Background(), selected)
				if (err == nil) != (mode == "valid") {
					t.Fatal("selected signal provenance differs", err)
				}
			})
		}
	}
}
