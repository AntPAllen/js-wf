package journal_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type checkpointScanFixtureOptions struct {
	indexed bool
	wrap    func(*checkpointCostPort) graphpublication.Port
}

func checkpointScanFixture(t *testing.T, encoding journal.Encoding, completion []byte, padding int, options ...checkpointScanFixtureOptions) (*journal.GraphStore, *checkpointCostPort, *time.Time, journal.RuntimeCheckpoint) {
	t.Helper()
	ctx := context.Background()
	schedule := sim.NewScheduler(23)
	model := sim.NewGraphPublicationTransport(schedule)
	port := &checkpointCostPort{GraphPublicationTransport: model, entries: map[string]bool{}}
	protocol := model.Protocol()
	protocol.Port = port
	var option checkpointScanFixtureOptions
	if len(options) > 0 {
		option = options[0]
	}
	if option.wrap != nil {
		protocol.Port = option.wrap(port)
	}
	now := time.Unix(1000, 0)
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Encoding: encoding, Now: func() time.Time { return now }, PinTTL: 4 * time.Second, CanonicalStarts: option.indexed, CanonicalSignals: option.indexed, CheckpointIndex: option.indexed})
	if err != nil {
		t.Fatal(err)
	}
	invocation := uint64(7)
	var started []byte
	var inputObjects [][]byte
	if option.indexed {
		transport := sim.NewSignalTransport(schedule)
		c, err := client.NewWithSignalPorts(transport, transport).WithGraphJournal(store)
		if err != nil {
			t.Fatal(err)
		}
		h, err := c.Start(ctx, "flow", "scan", []byte(`7`))
		if err != nil {
			t.Fatal(err)
		}
		invocation = h.InvSeq
		status, err := store.InspectStart(ctx, "flow", "scan")
		if err != nil {
			t.Fatal(err)
		}
		started, _ = json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
		inputObjects = [][]byte{[]byte(`7`)}
	}
	tail, err := store.Begin(ctx, "flow", "scan", invocation)
	if err != nil {
		t.Fatal(err)
	}
	index := uint64(0)
	appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) {
		t.Helper()
		entry := journal.Entry{Kind: kind, Index: index, Epoch: 3, Payload: payload}
		raw, e := journal.MarshalEntry(entry, encoding)
		if e != nil {
			t.Fatal(e)
		}
		sum := sha256.Sum256(raw)
		port.entries[hex.EncodeToString(sum[:])] = true
		tail, e = store.Append(ctx, "flow", "scan", invocation, entry, tail, objects, nil)
		if e != nil {
			t.Fatal(e)
		}
		index++
	}
	appendRecord(journal.Started, started, inputObjects...)
	for i := 0; i < padding; i++ {
		request, _ := json.Marshal(map[string]string{"kind": "run", "name": fmt.Sprint("padding", i)})
		appendRecord(journal.StepRequested, request)
		appendRecord(journal.StepCompleted, completion)
	}
	locals := json.RawMessage(`42`)
	sum := sha256.Sum256(locals)
	frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: "flow", ID: "scan", InvSeq: invocation}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: index + 1, Epoch: 3}, StepPosition: uint64(2*padding + 2)}
	raw, hash, err := checkpoint.Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(sum[:])})
	appendRecord(journal.StepRequested, request)
	completion, _ = json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
	appendRecord(journal.StepCompleted, completion, raw)
	runtime := journal.RuntimeCheckpoint{InvSeq: invocation, Stage: "next", Sequence: tail, Index: index - 1, Epoch: 3, StepPosition: frame.StepPosition, Object: "step-result-" + hash, SHA256: hash}
	appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`))
	return store, port, &now, runtime
}

func TestGraphCheckpointRejectsAmbiguousEarlierCompletion(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		valid   bool
	}{
		{"duplicate", []byte(`{"result":1,"result":2}`), false},
		{"alias", []byte(`{"Result":1,"result":2}`), false},
		{"escaped", []byte(`{"result":1,"r\u0065sult":2}`), false},
		{"unknown", []byte(`{"foreign":true}`), false},
		{"missing", nil, false},
		{"null", []byte(`null`), false},
		{"valid", []byte(`{"result":7}`), true},
		{"opaque-result", []byte(`{"result":{"x":1,"x":2}}`), true},
	}
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, test := range cases {
			t.Run(string(encoding)+"/"+test.name, func(t *testing.T) {
				store, _, _, runtime := checkpointScanFixture(t, encoding, test.payload, 1)
				view, err := store.Open(context.Background(), "flow", "scan", 7)
				if err != nil {
					t.Fatal(err)
				}
				defer view.Close(context.Background())
				found, err := view.ReadCheckpoint(context.Background(), "flow", "scan")
				if test.valid {
					if err != nil || found == nil || found.Runtime != runtime {
						t.Fatal(found, err)
					}
				} else if !errors.Is(err, journal.ErrGap) || found != nil {
					t.Fatal("ambiguous earlier completion admitted", found, err)
				}
			})
		}
	}
}

func TestGraphCheckpointScanBatchesAndAuthority(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"paused", "renewing", "tail-change", "retire-reuse", "closed", "expired"} {
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				store, port, now, runtime := checkpointScanFixture(t, encoding, []byte(`{"result":{"nested":["opaque"]}}`), 16)
				view, err := store.Open(ctx, "flow", "scan", 7)
				if err != nil {
					t.Fatal(err)
				}
				defer view.Close(ctx)
				scan, err := view.NewCheckpointScan(ctx, "flow", "scan")
				if err != nil {
					t.Fatal(err)
				}
				if mode == "renewing" {
					port.clock = now
				}
				port.entryReads = 0
				found, done, err := scan.Advance(ctx, 3)
				if err != nil || done || found != nil || scan.NextIndex() != 3 || port.entryReads != 3 {
					t.Fatal("partial result or incorrect progress", found, done, err, scan.NextIndex(), port.entryReads)
				}
				attempts := port.entryReads
				if mode == "paused" {
					port.entryReads, port.maxEntryReads = 0, 5
					found, done, err = scan.Advance(ctx, 9)
					attempts += port.entryReads
					if !errors.Is(err, context.DeadlineExceeded) || done || found != nil || scan.NextIndex() != 8 {
						t.Fatal("deadline lost verified progress", found, done, err, scan.NextIndex())
					}
					port.maxEntryReads = 0
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					if found, done, err = scan.Advance(cancelled, 4); !errors.Is(err, context.Canceled) || done || found != nil || scan.NextIndex() != 8 {
						t.Fatal(found, done, err, scan.NextIndex())
					}
				}
				if mode == "tail-change" || mode == "retire-reuse" {
					kind, payload := journal.Suspended, []byte(`{"waiting_on":"changed"}`)
					if mode == "retire-reuse" {
						kind, payload = journal.Completed, []byte(`{"result":42}`)
					}
					tail, err := store.Append(ctx, "flow", "scan", 7, journal.Entry{Kind: kind, Index: view.Count(), Epoch: 3, Payload: payload}, view.Tail(), nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "retire-reuse" {
						if err = store.Retire(ctx, "flow", "scan", 7, tail); err != nil {
							t.Fatal(err)
						}
						if _, err = store.Begin(ctx, "flow", "scan", 8); err != nil {
							t.Fatal(err)
						}
					}
				}
				if mode == "closed" || mode == "expired" {
					if mode == "closed" {
						if err = view.Close(ctx); err != nil {
							t.Fatal(err)
						}
					} else {
						*now = now.Add(5 * time.Second)
					}
					keys, err := port.RootKeys(ctx)
					if err != nil || len(keys) != 1 {
						t.Fatal(keys, err)
					}
					before, err := port.ReadRoot(ctx, keys[0])
					if err != nil {
						t.Fatal(err)
					}
					port.entryReads = 0
					if found, done, err = scan.Advance(ctx, 4); !errors.Is(err, graphpublication.ErrRevoked) || done || found != nil || port.entryReads != 0 {
						t.Fatal("inactive scan resurrected", found, done, err, port.entryReads)
					}
					after, err := port.ReadRoot(ctx, keys[0])
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatal("inactive scan changed authority", before, after, err)
					}
					return
				}
				batches := 1
				for !done {
					before := scan.NextIndex()
					port.entryReads = 0
					found, done, err = scan.Advance(context.Background(), 4)
					attempts += port.entryReads
					batches++
					allowedReads := int(scan.NextIndex() - before)
					if done && found != nil {
						allowedReads++
					} // Fresh final frame ownership rechecks its anchor.
					if err != nil || scan.NextIndex()-before > 4 || port.entryReads > allowedReads {
						t.Fatal("batch work exceeded budget", found, done, err, before, scan.NextIndex(), port.entryReads)
					}
					if !done && found != nil {
						t.Fatal("partial scan certified a frame")
					}
				}
				wantAttempts := int(view.Count()) + 1 // Final frame ownership rechecks the anchor entry.
				if mode == "paused" {
					wantAttempts++
				}
				if attempts != wantAttempts || found == nil || found.Runtime != runtime || found.Tail != view.Tail() || len(found.Records) != 1 {
					t.Fatal("rescan or wrong captured result", attempts, wantAttempts, found)
				}
				if mode == "renewing" && (port.renewals == 0 || now.Sub(time.Unix(1000, 0)) != 37*time.Second) {
					t.Fatal("reader not renewed", port.renewals, *now)
				}
				port.clock = nil
				whole, err := view.ReadCheckpoint(ctx, "flow", "scan")
				if err != nil || !reflect.DeepEqual(whole, found) {
					t.Fatal("batched and whole scans disagree", whole, found, err)
				}
				found.Runtime.Stage = "edited"
				found.Frame[0], found.Request.Payload[0], found.Anchor.Payload[0], found.Records[0].Payload[0] = 'x', 'x', 'x', 'x'
				repeated, done, err := scan.Advance(ctx, 1)
				if err != nil || !done || !reflect.DeepEqual(repeated, whole) {
					t.Fatal("caller mutated verified scan state", repeated, whole, err)
				}
				found = repeated
				if mode == "tail-change" || mode == "retire-reuse" {
					if err = store.ConfirmCheckpoint(ctx, "flow", "scan", found.Runtime, found.Tail); !errors.Is(err, journal.ErrStale) {
						t.Fatal("captured old scan authorized fresh publication", err)
					}
				}
				t.Logf("CHECKPOINT_SCAN mode=%s records=%d batches=%d entry_read_attempts=%d renewals=%d captured_tail=%d", mode, view.Count(), batches, attempts, port.renewals, found.Tail)
			})
		}
	}
}
