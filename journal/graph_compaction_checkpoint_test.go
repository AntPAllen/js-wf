package journal_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type savedCompaction struct {
	Schema  string                    `json:"schema"`
	Type    string                    `json:"type"`
	ID      string                    `json:"id"`
	Runtime journal.RuntimeCheckpoint `json:"runtime"`
	Tail    uint64                    `json:"tail"`
	Stage   json.RawMessage           `json:"stage"`
	RenewTo *time.Time                `json:"renew_to,omitempty"`
}

func TestGraphCompactionCheckpointRejectsNoncanonicalEnvelope(t *testing.T) {
	ctx := context.Background()
	store, port, _, runtime := checkpointScanFixture(t, journal.JSON, []byte(`{"result":7}`), 4, checkpointScanFixtureOptions{indexed: true, archive: true})
	tail := runtime.Sequence + 1
	if err := store.PublishCheckpoint(ctx, "flow", "scan", runtime, tail); err != nil {
		t.Fatal(err)
	}
	op, err := store.BeginCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
	if err != nil {
		t.Fatal(err)
	}
	defer op.Close(ctx)
	for op.Phase() == "confirm" {
		if done, err := op.Advance(ctx, 2, 4); done || err != nil {
			t.Fatal(done, err)
		}
	}
	data, err := op.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	var envelope savedCompaction
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.RenewTo = new(time.Time)
	zeroExpiry, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"empty": nil, "oversize": []byte(strings.Repeat(" ", journal.MaxCompactionCheckpointBytes+1)),
		"null": []byte("null"), "unknown": []byte(strings.Replace(string(data), `"schema":`, `"unknown":true,"schema":`, 1)),
		"duplicate": []byte(strings.Replace(string(data), `"schema":`, `"schema":"foreign","schema":`, 1)),
		"alias":     []byte(strings.Replace(string(data), `"schema":`, `"Schema":`, 1)),
		"escaped":   []byte(strings.Replace(string(data), `"schema":`, `"s\u0063hema":`, 1)),
		"leading":   append([]byte(" "), data...), "trailing": append(append([]byte(nil), data...), '\n'),
		"second-object": append(append([]byte(nil), data...), []byte("{}")...), "zero-expiry": zeroExpiry,
	}
	keys, err := port.RootKeys(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatal(keys, err)
	}
	before, err := port.ReadRoot(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if resumed, err := store.ResumeCheckpointCompaction(ctx, "flow", "scan", runtime, tail, input); err == nil || resumed != nil {
				t.Fatal("invalid envelope admitted", resumed, err)
			}
			after, err := port.ReadRoot(ctx, keys[0])
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("invalid envelope changed authority", err)
			}
		})
	}
}

type savedCompactionStage struct {
	Schema                string
	Destination           string
	Expected, First, Next uint64
	Expires               time.Time
	MaxPayloadBytes       int
	Base, Publication     graphpublication.Root
}

func TestGraphCompactionCheckpointBindingRenewalAndResumption(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"normal", "renew", "renew-drop", "renew-lost", "verify", "every-batch", "type", "id", "runtime", "tail", "application", "cut", "limit", "append", "reader", "retire", "collector"} {
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				store, port, now, runtime := checkpointScanFixture(t, encoding, []byte(`{"result":7}`), 4, checkpointScanFixtureOptions{indexed: true, archive: true})
				tail := runtime.Sequence + 1
				if err := store.PublishCheckpoint(ctx, "flow", "scan", runtime, tail); err != nil {
					t.Fatal(err)
				}
				op, err := store.BeginCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
				if err != nil {
					t.Fatal(err)
				}
				defer op.Close(ctx)
				if _, err := op.Checkpoint(); err == nil {
					t.Fatal("confirmation emitted staging checkpoint")
				}
				for op.Phase() == "confirm" {
					if done, err := op.Advance(ctx, 2, 4); done || err != nil {
						t.Fatal(done, err)
					}
				}
				if done, err := op.Advance(ctx, 2, 4); done || err != nil || op.Phase() != "stage" {
					t.Fatal(done, err, op.Phase())
				}
				if mode == "verify" {
					for op.Phase() == "stage" {
						if done, err := op.Advance(ctx, 2, 4); done || err != nil {
							t.Fatal(done, err)
						}
					}
					if done, err := op.Advance(ctx, 2, 4); done || err != nil {
						t.Fatal(done, err)
					}
				}
				data, err := op.Checkpoint()
				if err != nil || len(data) > journal.MaxCompactionCheckpointBytes {
					t.Fatal(err)
				}
				keys, err := port.RootKeys(ctx)
				if err != nil || len(keys) != 1 {
					t.Fatal(keys, err)
				}
				original, err := port.ReadRoot(ctx, keys[0])
				if err != nil || len(original.Readers) != 0 {
					t.Fatal(original, err)
				}
				var envelope savedCompaction
				if err := json.Unmarshal(data, &envelope); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "type":
					envelope.Type = "foreign"
				case "id":
					envelope.ID = "foreign"
				case "runtime":
					envelope.Runtime.Stage = "foreign"
				case "tail":
					envelope.Tail++
				case "application", "cut", "limit":
					var stage savedCompactionStage
					if err := json.Unmarshal(envelope.Stage, &stage); err != nil {
						t.Fatal(err)
					}
					if mode == "application" {
						stage.Publication.Application = []byte("forged")
					}
					if mode == "cut" {
						stage.First++
					}
					if mode == "limit" {
						stage.MaxPayloadBytes++
					}
					envelope.Stage, err = json.Marshal(stage)
					if err != nil {
						t.Fatal(err)
					}
				case "append":
					if _, err := store.Append(ctx, "flow", "scan", runtime.InvSeq, journal.Entry{Kind: journal.Suspended, Index: 12, Epoch: 3, Payload: []byte(`{"waiting_on":"changed"}`)}, tail, nil, nil); err != nil {
						t.Fatal(err)
					}
				case "reader":
					view, err := store.OpenExisting(ctx, "flow", "scan", runtime.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
					defer view.Close(ctx)
				case "retire":
					terminal, err := store.Append(ctx, "flow", "scan", runtime.InvSeq, journal.Entry{Kind: journal.Completed, Index: 12, Epoch: 3, Payload: []byte(`{"result":7}`)}, tail, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := store.Retire(ctx, "flow", "scan", runtime.InvSeq, terminal); err != nil {
						t.Fatal(err)
					}
				case "collector":
					*now = now.Add(61 * time.Second)
					if _, err := (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, *now); err != nil {
						t.Fatal(err)
					}
				case "renew", "renew-drop", "renew-lost":
					requested := now.Add(3 * time.Minute)
					if err := op.BeginIntentRenewal(ctx, requested); err != nil {
						t.Fatal(err)
					}
					if op.Phase() != "renew" {
						t.Fatal("renewal phase missing")
					}
					data, err = op.Checkpoint()
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(data, &envelope); err != nil || envelope.RenewTo == nil || !envelope.RenewTo.Equal(requested) {
						t.Fatal("requested expiry not saved", err)
					}
					if mode != "renew" {
						fault := sim.DropBeforeCommit
						if mode == "renew-lost" {
							fault = sim.LoseAckAfterCommit
						}
						if err := port.QueueFault("cas_blob", fault); err != nil {
							t.Fatal(err)
						}
						for err == nil {
							_, err = op.Advance(ctx, 2, 4)
						}
						if op.Phase() != "renew" {
							t.Fatal("unknown renewal escaped phase")
						}
						if done, repeated := op.Advance(ctx, 2, 4); done || repeated != err {
							t.Fatal("failed operation reused", done, repeated)
						}
						retained, err := op.Checkpoint()
						if err != nil || string(retained) != string(data) {
							t.Fatal("uncertain renewal lost its input", err)
						}
					}
				}
				data, err = json.Marshal(envelope)
				if err != nil {
					t.Fatal(err)
				}
				op, err = store.ResumeCheckpointCompaction(ctx, "flow", "scan", runtime, tail, data)
				negative := mode == "type" || mode == "id" || mode == "runtime" || mode == "tail" || mode == "application" || mode == "cut" || mode == "limit" || mode == "append" || mode == "reader" || mode == "retire" || mode == "collector"
				if negative {
					if err == nil || op != nil {
						t.Fatal("foreign/stale transition resumed", op, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if mode == "verify" && op.Phase() != "stage" {
					t.Fatal("private verification certificate resumed")
				}
				renewBatches, verifyBatches := 0, 0
				for calls := 0; calls < 100; calls++ {
					phase := op.Phase()
					done, err := op.Advance(ctx, 2, 4)
					if err != nil {
						t.Fatal(err)
					}
					if phase == "renew" {
						renewBatches++
					}
					if phase == "verify" {
						verifyBatches++
					}
					if done {
						break
					}
					current, err := port.ReadRoot(ctx, keys[0])
					if err != nil || !reflect.DeepEqual(original, current) {
						t.Fatal("partial resumed operation published", err)
					}
					if phase == "renew" && op.Phase() == "stage" {
						*now = now.Add(61 * time.Second)
						if _, err := (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, *now); err != nil {
							t.Fatal(err)
						}
					}
					if mode == "every-batch" {
						saved, err := op.Checkpoint()
						if err != nil {
							t.Fatal(err)
						}
						// Private verification must finish in-process; stage-only
						// persistence deliberately restarts verification after death.
						if op.Phase() == "stage" {
							op, err = store.ResumeCheckpointCompaction(ctx, "flow", "scan", runtime, tail, saved)
							if err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				if op.Phase() != "done" || verifyBatches != 10 {
					t.Fatal("incomplete independent verification", op.Phase(), verifyBatches)
				}
				current, err := port.ReadRoot(ctx, keys[0])
				if err != nil || current.Head != original.Head+1 || len(current.Readers) != 0 {
					t.Fatal(current, err)
				}
				var cursor struct {
					RetainedFrom uint64 `json:"retained_from"`
				}
				if err := json.Unmarshal(current.Application, &cursor); err != nil || cursor.RetainedFrom != 9 {
					t.Fatal(cursor, err)
				}
				if _, err := store.ResumeCheckpointCompaction(ctx, "flow", "scan", runtime, tail, data); err == nil {
					t.Fatal("committed source descriptor revived")
				}
				if err := op.Close(ctx); err != nil {
					t.Fatal(err)
				}
				t.Logf("BOUND_COMPACTION mode=%s renewal_batches=%d verification_batches=%d retained_from=9 readers=0 checkpoint_bytes=%d", mode, renewBatches, verifyBatches, len(data))
			})
		}
	}
}
