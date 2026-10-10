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
	"js-wf/internal/blobpublication"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type archiveObjectPort interface {
	graphpublication.Port
	Objects(context.Context) ([]blobpublication.Object, error)
}

func TestGraphCheckpointArchiveLogicalHistoryAndCollection(t *testing.T) {
	for seed := uint64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			model := sim.NewGraphPublicationTransport(sim.NewScheduler(int64(seed)))
			now := time.Unix(1000, 0).UTC()
			config := journal.GraphConfig{Protocol: model.Protocol(), Now: func() time.Time { return now }, PinTTL: 3 * time.Hour, IntentTTL: time.Second, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true}
			checkpointArchiveScenario(t, context.Background(), seed, config, model, &now, nil)
		})
	}
}

func checkpointArchiveScenario(t *testing.T, ctx context.Context, seed uint64, config journal.GraphConfig, objectsPort archiveObjectPort, now *time.Time, reopen func() *journal.GraphStore, stagedRestart ...bool) {
	t.Helper()
	protocol := config.Protocol
	scheduler := sim.NewScheduler(int64(seed))
	store, err := journal.NewGraphStore(config)
	if err != nil {
		t.Fatal(err)
	}
	transport := sim.NewSignalTransport(scheduler)
	c, err := client.NewWithSignalPorts(transport, transport).WithGraphJournal(store)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "flow", "archive", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.InspectStart(ctx, h.Type, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	index, position := uint64(0), uint64(0)
	appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) {
		t.Helper()
		var err error
		tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: kind, Index: index, Epoch: 3, Payload: payload}, tail, objects, nil)
		if err != nil {
			t.Fatal(index, err)
		}
		index++
		if kind == journal.StepRequested || kind == journal.StepCompleted {
			position++
		}
	}
	started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
	appendRecord(journal.Started, started, []byte(`7`))
	for i := uint64(0); i < seed; i++ {
		appendRecord(journal.StepRequested, []byte(fmt.Sprintf(`{"kind":"run","name":"padding%d"}`, i)))
		appendRecord(journal.StepCompleted, []byte(`{"result":7}`))
	}
	makeCheckpoint := func(stage string) *journal.GraphCheckpointRead {
		t.Helper()
		locals := json.RawMessage(`{"value":42}`)
		digest := sha256.Sum256(locals)
		frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: stage, Data: locals, Anchor: checkpoint.Anchor{Index: index + 1, Epoch: 3}, StepPosition: position + 2}
		data, hash, err := checkpoint.Encode(frame)
		if err != nil {
			t.Fatal(err)
		}
		request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": stage, "input_hash": hex.EncodeToString(digest[:])})
		completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
		appendRecord(journal.StepRequested, request)
		appendRecord(journal.StepCompleted, completion, data)
		appendRecord(journal.Suspended, []byte(fmt.Sprintf(`{"waiting_on":"continuation:%s"}`, stage)))
		view, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
		if err != nil {
			t.Fatal(err)
		}
		found, err := view.ReadCheckpoint(ctx, h.Type, h.ID)
		if err != nil || found == nil {
			t.Fatal(found, err)
		}
		if err = view.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if err = store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
			t.Fatal(err)
		}
		return found
	}
	first := makeCheckpoint("next")
	old, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	var originals []journal.GraphRecord
	for i := uint64(0); i < index; i++ {
		r, err := old.Read(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, r)
	}
	if err = store.CompactCheckpoint(ctx, h.Type, h.ID, first.Runtime, tail+1); !errors.Is(err, journal.ErrStale) {
		t.Fatal("wrong-tail compaction accepted", err)
	}
	if len(stagedRestart) > 0 && stagedRestart[0] {
		op, err := store.BeginCheckpointCompaction(ctx, h.Type, h.ID, first.Runtime, tail)
		if err != nil {
			t.Fatal(err)
		}
		for op.Phase() == "confirm" {
			if done, err := op.Advance(ctx, 2, 4); done || err != nil {
				t.Fatal(done, err)
			}
		}
		if done, err := op.Advance(ctx, 2, 4); done || err != nil {
			t.Fatal(done, err)
		}
		if err := op.BeginIntentRenewal(ctx, now.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
		saved, err := op.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		var storedRevision uint64
		if config.CompactionCheckpoints != nil {
			storedRevision, err = op.SaveCheckpoint(ctx, 0)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := op.Close(ctx); err != nil {
			t.Fatal(err)
		}
		// Persisted renewal input survives all-peer same-store restart;
		// the existing old reader remains canonical and protected.
		store = reopen()
		if storedRevision != 0 {
			var observed uint64
			op, observed, err = store.ResumeStoredCheckpointCompaction(ctx, h.Type, h.ID, first.Runtime, tail)
			if err == nil && (op == nil || observed != storedRevision) {
				t.Fatal("native stored descriptor lost", observed, storedRevision)
			}
		} else {
			op, err = store.ResumeCheckpointCompaction(ctx, h.Type, h.ID, first.Runtime, tail, saved)
		}
		if err != nil || op.Phase() != "renew" {
			t.Fatal("native renewal resume failed", err)
		}
		renewBatches, verifyBatches := 0, 0
		verificationRenewed := false
		for {
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
			if phase == "renew" && op.Phase() == "stage" {
				*now = now.Add(2 * time.Second)
				if _, err := protocol.SweepWithReaders(ctx, *now); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "renew" && op.Phase() == "verify" {
				*now = now.Add(time.Minute)
				if _, err := protocol.SweepWithReaders(ctx, *now); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "verify" && !done && !verificationRenewed {
				if err := op.BeginIntentRenewal(ctx, now.Add(90*time.Second)); err != nil {
					t.Fatal(err)
				}
				verificationRenewed = true
			}
			if done {
				break
			}
		}
		if err := op.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if !verificationRenewed || verifyBatches != 8 {
			t.Fatal("native private progress restarted", verificationRenewed, verifyBatches)
		}
		if storedRevision != 0 {
			if err := store.DeleteCompactionCheckpoint(ctx, h.Type, h.ID, first.Runtime, tail, storedRevision); err != nil {
				t.Fatal(err)
			}
			t.Logf("NATIVE_STORED_COMPACTION revision=%d all_peer_restart=true fresh_kv_adapter=true deletion_confirmed=true", storedRevision)
		}
		t.Logf("NATIVE_BOUND_COMPACTION renewal_batches=%d verification_batches=%d saved_bytes=%d old_reader_preserved=true expired_old_intents_swept=true", renewBatches, verifyBatches, len(saved))
	} else {
		if err = store.CompactCheckpoint(ctx, h.Type, h.ID, first.Runtime, tail); err != nil {
			t.Fatal(err)
		}
	}
	if reopen != nil {
		store = reopen()
	}
	if config.CompactionCheckpoints != nil {
		op, rev, err := store.ResumeStoredCheckpointCompaction(ctx, h.Type, h.ID, first.Runtime, tail)
		if err != nil || op != nil || rev != 0 {
			t.Fatal("deleted descriptor resurrected after restart", rev, err)
		}
	}
	if err = store.CompactCheckpoint(ctx, h.Type, h.ID, first.Runtime, tail); err != nil {
		t.Fatal("idempotent compact", err)
	}
	// Explicit schema selection prevents old readers adopting v6 state.
	oldConfig := config
	oldConfig.ArchiveCheckpoints = false
	oldStore, _ := journal.NewGraphStore(oldConfig)
	if _, err = oldStore.Open(ctx, h.Type, h.ID, h.InvSeq); !errors.Is(err, journal.ErrGap) {
		t.Fatal("v5 adopted v6", err)
	}
	*now = now.Add(time.Hour)
	if _, err = protocol.SweepWithReaders(ctx, *now); err != nil {
		t.Fatal(err)
	}
	for i, original := range originals {
		r, err := old.Read(ctx, uint64(i))
		if err != nil || !reflect.DeepEqual(r, original) {
			t.Fatal("old snapshot changed", i, err)
		}
	}
	if err = old.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = protocol.SweepWithReaders(ctx, *now); err != nil {
		t.Fatal(err)
	}
	if _, err = objectsPort.Get(ctx, originals[0].EntryBlob, journal.MaxGraphEntryBytes); err == nil {
		t.Fatal("unpinned original survived collection")
	}
	fresh, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Count() != index || fresh.Tail() != tail {
		t.Fatal("logical cursor changed")
	}
	oldHandle := *fresh
	if err := fresh.Refresh(ctx, h.Type, h.ID); err != nil {
		t.Fatal("refresh archived and live forests", err)
	}
	if _, err := oldHandle.Read(ctx, 0); !errors.Is(err, graphpublication.ErrRevoked) {
		t.Fatal("refresh retained old archive handle", err)
	}
	for i, original := range originals {
		r, err := fresh.Read(ctx, uint64(i))
		if err != nil || !reflect.DeepEqual(r.Record, original.Record) || r.EntryBlob.Reference == original.EntryBlob.Reference {
			t.Fatal("relocated logical history", i, err)
		}
	}
	// Ordered ranges must preserve logical indexes across the archive/live
	// boundary, including ranges wholly in either forest and empty ranges.
	for _, bounds := range [][2]uint64{{0, index}, {0, 1}, {first.Runtime.Index, index}, {index, index}} {
		next := bounds[0]
		if err := fresh.ReadRange(ctx, bounds[0], bounds[1], func(r journal.GraphRecord) error {
			if next >= bounds[1] || !reflect.DeepEqual(r.Record, originals[next].Record) {
				t.Fatal("range changed archived logical history", bounds, next, r)
			}
			next++
			return nil
		}); err != nil || next != bounds[1] {
			t.Fatal("archive range", bounds, next, err)
		}
	}
	canceled, cancelRange := context.WithCancel(ctx)
	cancelRange()
	if err := fresh.ReadRange(canceled, index, index, func(journal.GraphRecord) error {
		t.Fatal("canceled empty range invoked visitor")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled empty range accepted", err)
	}
	found, err := fresh.ReadCheckpoint(ctx, h.Type, h.ID)
	if err != nil || found.Runtime != first.Runtime {
		t.Fatal("relocated checkpoint", found, err)
	}
	// Reuse a live logical source; reject a receipt from archived history.
	live, err := fresh.Read(ctx, first.Runtime.Index)
	if err != nil {
		t.Fatal(err)
	}
	archived, err := fresh.Read(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	entry := journal.Entry{Kind: journal.StepRequested, Index: index, Epoch: 3, Payload: []byte(`{"kind":"run","name":"after"}`)}
	if _, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, nil, []journal.GraphOwnedPayload{{Index: 0, Link: archived.EntryBlob}}); !errors.Is(err, journal.ErrStale) {
		t.Fatal("archived grant adopted", err)
	}
	tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, nil, []journal.GraphOwnedPayload{{Index: first.Runtime.Index, Link: live.EntryBlob}})
	if err != nil {
		t.Fatal("logical owned append", err)
	}
	index++
	position++
	if err = fresh.Close(ctx); err != nil {
		t.Fatal(err)
	}
	appendRecord(journal.StepCompleted, []byte(`{"result":42}`))
	second := makeCheckpoint("finish")
	if err = store.CompactCheckpoint(ctx, h.Type, h.ID, second.Runtime, tail); err != nil {
		t.Fatal("successive compact", err)
	}
	appendRecord(journal.Completed, []byte(`{"result":42}`))
	if err = store.CompactCheckpoint(ctx, h.Type, h.ID, second.Runtime, tail); !errors.Is(err, journal.ErrStale) {
		t.Fatal("terminal compaction accepted", err)
	}
	records, readTail, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil || len(records) != int(index) || readTail != tail {
		t.Fatal("full audit after compaction", len(records), readTail, err)
	}
	if err = store.Retire(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(4 * time.Hour)
	if _, err = protocol.SweepWithReaders(ctx, *now); err != nil {
		t.Fatal(err)
	}
	objects, err := objectsPort.Objects(ctx)
	if err != nil || len(objects) != 0 {
		t.Fatal("retired archive leaked", len(objects), err)
	}
}

func TestGraphCheckpointArchiveConfiguration(t *testing.T) {
	model := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
	if _, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true, ArchiveCheckpoints: true}); err == nil {
		t.Fatal("archive without checkpoint index")
	}
	if _, err := journal.NativeGraphStreamConfigs(journal.NativeGraphConfig{AuthorityStream: "AUTH", AuthorityPrefix: "wf.graph", ObjectBucket: "OBJECTS", CanonicalStarts: true, CanonicalSignals: true, ArchiveCheckpoints: true}, 1); err == nil {
		t.Fatal("native archive without checkpoint index")
	}
}
