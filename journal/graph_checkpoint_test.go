package journal_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
)

type checkpointPayloadFaultPort struct {
	*sim.GraphPublicationTransport
	failHash    string
	failRelease bool
}

func (p *checkpointPayloadFaultPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if link.Hash == p.failHash {
		return nil, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}

func (p *checkpointPayloadFaultPort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if p.failRelease && len(root.Readers) == 1 {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

func TestGraphCheckpointOwnedFrameAndSuffix(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"valid", "absent", "pending", "wrong-generation", "wrong-locals", "missing-edge", "borrowed-edge", "unreadable", "expired", "latest", "duplicate-request", "alias-request", "unknown-request", "duplicate-completion", "alias-completion", "unknown-completion"} {
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				_, model, now := graphModel(t, encoding)
				port := &checkpointPayloadFaultPort{GraphPublicationTransport: model}
				protocol := model.Protocol()
				protocol.Port = port
				store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Now: func() time.Time { return *now }, PinTTL: time.Minute, IntentTTL: time.Second, Encoding: encoding})
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				tail, err := store.Begin(ctx, "flow", "checkpoint", 7)
				if err != nil {
					t.Fatal(err)
				}
				appendRecord := func(index uint64, kind journal.Kind, payload []byte, objects ...[]byte) {
					t.Helper()
					tail, err = store.Append(ctx, "flow", "checkpoint", 7, journal.Entry{Index: index, Epoch: 3, Kind: kind, Payload: payload}, tail, objects, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				data := json.RawMessage(`{"count":1}`)
				digest := sha256.Sum256(data)
				request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})

				switch mode {
				case "duplicate-request":
					request = append([]byte(`{"kind":"run",`), request[1:]...)
				case "alias-request":
					request = append([]byte(`{"Kind":"run",`), request[1:]...)
				case "unknown-request":
					request = append([]byte(`{"foreign":true,`), request[1:]...)
				}
				frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: "flow", ID: "checkpoint", InvSeq: 7}, Stage: "next", Data: data, Anchor: checkpoint.Anchor{Index: 2, Epoch: 3}, StepPosition: 2, State: map[string]json.RawMessage{"value": json.RawMessage(`42`)}}
				if mode == "wrong-generation" {
					frame.Identity.InvSeq = 8
				}
				if mode == "wrong-locals" {
					frame.Data = json.RawMessage(`{"count":2}`)
				}
				encoded, hash, err := checkpoint.Encode(frame)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "borrowed-edge" {
					appendRecord(0, journal.Started, nil, encoded)
				} else {
					appendRecord(0, journal.Started, nil)
				}
				completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})

				switch mode {
				case "duplicate-completion":
					completion = append([]byte(`{"result_ref":"foreign",`), completion[1:]...)
				case "alias-completion":
					completion = append([]byte(`{"Result_ref":"foreign",`), completion[1:]...)
				case "unknown-completion":
					completion = append([]byte(`{"foreign":true,`), completion[1:]...)
				}
				if mode != "absent" {
					appendRecord(1, journal.StepRequested, request)
					if mode != "pending" {
						if mode == "missing-edge" || mode == "borrowed-edge" {
							appendRecord(2, journal.StepCompleted, completion)
						} else {
							appendRecord(2, journal.StepCompleted, completion, encoded)
						}
						appendRecord(3, journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`))
					}
				}
				view, err := store.OpenExisting(ctx, "flow", "checkpoint", 7)
				if err != nil {
					t.Fatal(err)
				}
				defer view.Close(context.Background())
				if mode == "unreadable" {
					port.failHash = hash
				}
				var expiredRoot graphpublication.Root
				var expiredKey string
				if mode == "expired" {
					keys, keyErr := model.RootKeys(ctx)
					if keyErr != nil || len(keys) != 1 {
						t.Fatal(keys, keyErr)
					}
					expiredKey = keys[0]
					expiredRoot, keyErr = model.ReadRoot(ctx, expiredKey)
					if keyErr != nil || len(expiredRoot.Readers) != 1 {
						t.Fatal(expiredRoot, keyErr)
					}
					*now = now.Add(2 * time.Minute)
				}
				got, err := view.ReadCheckpoint(ctx, "flow", "checkpoint")
				if mode == "expired" {
					after, rootErr := model.ReadRoot(ctx, expiredKey)
					if rootErr != nil || after.Head != expiredRoot.Head || len(after.Readers) != 1 || !after.Readers[0].Expires.Equal(expiredRoot.Readers[0].Expires) {
						t.Fatal("expired checkpoint pin was mutated or resurrected", after, rootErr)
					}
				}
				switch mode {
				case "absent", "pending":
					if err != nil || got != nil {
						t.Fatal("incomplete checkpoint selected", got, err)
					}
					return
				case "wrong-generation", "wrong-locals", "missing-edge", "borrowed-edge", "unreadable", "expired", "duplicate-request", "alias-request", "unknown-request", "duplicate-completion", "alias-completion", "unknown-completion":
					if err == nil || got != nil {
						t.Fatal("invalid checkpoint accepted", got, err)
					}
					return
				}
				if err != nil || got == nil || !bytes.Equal(got.Frame, encoded) || got.Runtime.Sequence != 3 || got.Runtime.Index != 2 || got.Runtime.StepPosition != 2 || len(got.Records) != 1 || got.Records[0].Kind != journal.Suspended || got.Tail != 4 {
					t.Fatal(got, err)
				}
				if _, err := view.ReadCheckpoint(ctx, "other", "id"); !errors.Is(err, journal.ErrCheckpointGeneration) {
					t.Fatal("foreign view identity accepted", err)
				}
				if mode == "valid" {
					if err := store.ConfirmCheckpoint(ctx, "flow", "checkpoint", got.Runtime, got.Tail); err != nil {
						t.Fatal("valid handoff confirmation", err)
					}
					if err := store.ConfirmCheckpoint(ctx, "flow", "checkpoint", got.Runtime, got.Tail+1); !errors.Is(err, journal.ErrStale) {
						t.Fatal("changed tail accepted", err)
					}
					foreign := got.Runtime
					foreign.Stage = "other"
					if err := store.ConfirmCheckpoint(ctx, "flow", "checkpoint", foreign, got.Tail); !errors.Is(err, journal.ErrGap) {
						t.Fatal("different checkpoint accepted", err)
					}
					port.failRelease = true
					if err := store.ConfirmCheckpoint(ctx, "flow", "checkpoint", got.Runtime, got.Tail); err == nil {
						t.Fatal("uncertain reader release confirmed handoff")
					}
					port.failRelease = false
				}
				if mode == "latest" {
					frame.Data = json.RawMessage(`{"count":3}`)
					frame.Anchor.Index = 5
					frame.StepPosition = 4
					newer, newHash, err := checkpoint.Encode(frame)
					if err != nil {
						t.Fatal(err)
					}
					digest = sha256.Sum256(frame.Data)
					request, _ = json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
					completion, _ = json.Marshal(map[string]string{"result_ref": "step-result-" + newHash, "result_hash": newHash})
					appendRecord(4, journal.StepRequested, request)
					appendRecord(5, journal.StepCompleted, completion, newer)
					// The old pin must retain its original checkpoint and suffix.
					old, err := view.ReadCheckpoint(ctx, "flow", "checkpoint")
					if err != nil || old.Runtime.Index != 2 || old.Tail != 4 {
						t.Fatal("pinned checkpoint changed", old, err)
					}
					fresh, err := store.OpenExisting(ctx, "flow", "checkpoint", 7)
					if err != nil {
						t.Fatal(err)
					}
					defer fresh.Close(context.Background())
					latest, err := fresh.ReadCheckpoint(ctx, "flow", "checkpoint")
					if err != nil || latest.Runtime.Index != 5 || latest.Runtime.StepPosition != 4 || !bytes.Equal(latest.Frame, newer) || len(latest.Records) != 0 {
						t.Fatal("latest checkpoint not selected", latest, err)
					}
				}
			})
		}
	}
}
