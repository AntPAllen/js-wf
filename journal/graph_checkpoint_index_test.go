package journal_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
)

type indexedCheckpointPort struct {
	*sim.GraphPublicationTransport
	blocked      map[string]bool
	fault        sim.AppendFault
	inject       bool
	race         func() error
	blockedReads int
	unconfirmed  bool
	failRead     bool
}

func (p *indexedCheckpointPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if p.blocked[link.Hash] {
		p.blockedReads++
		return nil, fmt.Errorf("prefix body read forbidden")
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}
func (p *indexedCheckpointPort) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	if p.failRead {
		p.failRead = false
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.ReadRoot(ctx, key)
}
func (p *indexedCheckpointPort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	injected := false
	var app struct {
		Checkpoint json.RawMessage `json:"checkpoint"`
	}
	_ = json.Unmarshal(root.Application, &app)
	if len(app.Checkpoint) > 0 {
		if p.inject {
			p.inject = false
			injected = true
			if err := p.QueueFault("cas_root", p.fault); err != nil {
				return graphpublication.Root{}, err
			}
		}
		if p.race != nil {
			race := p.race
			p.race = nil
			if err := race(); err != nil {
				return graphpublication.Root{}, err
			}
		}
	}
	ack, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	if injected && err != nil && p.unconfirmed {
		p.failRead = true
	}
	return ack, err
}

func TestGraphCheckpointIndexPublicationAndBoundedRead(t *testing.T) {
	for _, mode := range []string{"normal", "lost-ack", "lost-ack-unconfirmed", "lost-before", "tail-race"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(19)
			model := sim.NewGraphPublicationTransport(schedule)
			port := &indexedCheckpointPort{GraphPublicationTransport: model, blocked: map[string]bool{}}
			protocol := model.Protocol()
			protocol.Port = port
			config := journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true}
			store, err := journal.NewGraphStore(config)
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewSignalTransport(schedule)
			c, err := client.NewWithSignalPorts(transport, transport).WithGraphJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "flow", "indexed", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			status, err := store.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
			tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			index := uint64(0)
			appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) error {
				var err error
				tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: index, Epoch: 3, Kind: kind, Payload: payload}, tail, objects, nil)
				if err == nil {
					index++
				}
				return err
			}
			if err := appendRecord(journal.Started, started, []byte(`7`)); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 64; i++ {
				request, _ := json.Marshal(map[string]string{"kind": "run", "name": fmt.Sprint("padding", i)})
				if err := appendRecord(journal.StepRequested, request); err != nil {
					t.Fatal(err)
				}
				if err := appendRecord(journal.StepCompleted, []byte(`{"result":7}`)); err != nil {
					t.Fatal(err)
				}
			}
			locals := json.RawMessage(`{"value":42}`)
			digest := sha256.Sum256(locals)
			frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: index + 1, Epoch: 3}, StepPosition: 130}
			data, hash, err := checkpoint.Encode(frame)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
			completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
			if err := appendRecord(journal.StepRequested, request); err != nil {
				t.Fatal(err)
			}
			if err := appendRecord(journal.StepCompleted, completion, data); err != nil {
				t.Fatal(err)
			}
			if err := appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`)); err != nil {
				t.Fatal(err)
			}
			old, err := store.OpenExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close(context.Background())
			found, err := old.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			if found.Request.Index != 129 || found.Runtime.Index != 130 || found.Runtime.StepPosition != 130 {
				t.Fatal(found)
			}
			if mode == "lost-ack" || mode == "lost-ack-unconfirmed" {
				port.inject = true
				port.fault = sim.LoseAckAfterCommit
				port.unconfirmed = mode == "lost-ack-unconfirmed"
			}
			if mode == "lost-before" {
				port.inject = true
				port.fault = sim.DropBeforeCommit
			}
			if mode == "tail-race" {
				port.race = func() error { return appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`)) }
			}
			err = store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, found.Tail)
			if mode == "normal" || mode == "lost-ack" {
				if err != nil {
					t.Fatal(err)
				}
			} else if mode == "tail-race" {
				if !errors.Is(err, journal.ErrStale) {
					t.Fatal("tail race not fenced", err)
				}
			} else if !errors.Is(err, journal.ErrUnknown) {
				t.Fatal("lost CAS certified success", err)
			}
			if err := store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
				t.Fatal("fresh publication retry", err)
			}
			// Every old encoded entry before the checkpoint request is now unreadable.
			// Indexed lookup must still return the same frame plus its bounded suffix.
			for i := uint64(0); i < found.Request.Index; i++ {
				record, err := old.Read(ctx, i)
				if err != nil {
					t.Fatal(err)
				}
				port.blocked[record.EntryBlob.Hash] = true
			}
			fresh, err := store.OpenExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close(context.Background())
			indexed, err := fresh.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil || indexed.Runtime != found.Runtime || len(indexed.Records) != int(tail-found.Runtime.Sequence) || port.blockedReads != 0 {
				t.Fatal("indexed reader touched prefix", indexed, err, port.blockedReads)
			}
			if err := store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
				t.Fatal("idempotent indexed retry", err)
			}
			// The pre-publication pin has no pointer and cannot bypass unavailable
			// prefix bytes. A v4 reader must reject the v5 cursor rather than downgrade.
			if _, err := old.ReadCheckpoint(ctx, h.Type, h.ID); err == nil {
				t.Fatal("old pin changed its cursor")
			}
			oldConfig := config
			oldConfig.CheckpointIndex = false
			oldStore, err := journal.NewGraphStore(oldConfig)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := oldStore.OpenExisting(ctx, h.Type, h.ID, h.InvSeq); !errors.Is(err, journal.ErrGap) {
				t.Fatal("v4 reader admitted v5", err)
			}
			if mode == "normal" {
				sum := sha256.Sum256([]byte(identity.JournalSubject(h.Type, h.ID)))
				destination := "journal/" + hex.EncodeToString(sum[:])
				root, err := model.ReadRoot(ctx, destination)
				if err != nil {
					t.Fatal(err)
				}
				original := append([]byte(nil), root.Application...)
				for _, mutation := range [][2]string{{`"step_position":130`, `"step_position":128`}, {`"request_index":129`, `"request_index":0`}} {
					root, err = model.ReadRoot(ctx, destination)
					if err != nil {
						t.Fatal(err)
					}
					root.Application = bytes.Replace(original, []byte(mutation[0]), []byte(mutation[1]), 1)
					if bytes.Equal(root.Application, original) {
						t.Fatal("mutation did not apply")
					}
					if _, err := model.CASRoot(ctx, destination, root.Head, root); err != nil {
						t.Fatal(err)
					}
					damaged, err := store.OpenExisting(ctx, h.Type, h.ID, h.InvSeq)
					if err == nil {
						_, err = damaged.ReadCheckpoint(ctx, h.Type, h.ID)
						if closeErr := damaged.Close(ctx); closeErr != nil {
							t.Fatal(closeErr)
						}
					}
					if !errors.Is(err, journal.ErrGap) {
						t.Fatal("forged index admitted", mutation, err)
					}
					root, err = model.ReadRoot(ctx, destination)
					if err != nil {
						t.Fatal(err)
					}
					root.Application = append([]byte(nil), original...)
					if _, err := model.CASRoot(ctx, destination, root.Head, root); err != nil {
						t.Fatal(err)
					}
				}
			}
			t.Logf("mode=%s checkpoint=%d request=%d unreadable_prefix_records=%d suffix=%d", mode, indexed.Runtime.Index, indexed.Request.Index, len(port.blocked), len(indexed.Records))
		})
	}
}

func TestGraphCheckpointIndexRequiresCanonicalStores(t *testing.T) {
	model := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
	for _, config := range []journal.GraphConfig{{Protocol: model.Protocol(), CheckpointIndex: true}, {Protocol: model.Protocol(), CanonicalStarts: true, CheckpointIndex: true}} {
		if _, err := journal.NewGraphStore(config); err == nil {
			t.Fatal("index admitted without canonical Start/Signal")
		}
	}
	if _, err := journal.NativeGraphStreamConfigs(journal.NativeGraphConfig{AuthorityStream: "AUTH", AuthorityPrefix: "wf.graph", ObjectBucket: "OBJECTS", CheckpointIndex: true, CanonicalStarts: true}, 1); err == nil {
		t.Fatal("native index admitted without canonical Signals")
	}
}
