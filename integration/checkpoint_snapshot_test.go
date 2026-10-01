package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

func TestCheckpointManifestRetainsAnchorAndPromiseBlobs(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "checkpoint-manifest"
	handle, err := client.New(all[0]).Start(ctx, typ, id, []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	child := []byte("child-outcome")
	digest := sha256.Sum256(child)
	childName := "terminal-result-checkpoint-child"
	if _, err := objects.PutBytes(ctx, childName, child); err != nil {
		t.Fatal(err)
	}
	outcome, _ := json.Marshal(wf.Outcome{InvSeq: 23, ResultRef: childName, ResultHash: hex.EncodeToString(digest[:])})
	store := journal.New(all[0])
	var records []journal.Record
	var tail uint64
	appendEntry := func(kind journal.Kind, payload json.RawMessage) {
		t.Helper()
		entry := journal.Entry{Index: uint64(len(records)), Epoch: 51, Kind: kind, Payload: payload, WorkerID: "checkpoint-owner"}
		seq, err := store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		tail = seq
	}
	appendEntry(journal.Started, nil)
	appendEntry(journal.StepRequested, json.RawMessage(`{"kind":"run","name":"prefix"}`))
	appendEntry(journal.StepCompleted, json.RawMessage(`{"result":23}`))
	publishPair := func(stage string) journal.RuntimeCheckpoint {
		t.Helper()
		index := uint64(len(records) + 1)
		frame := checkpoint.Frame{Version: 1, Identity: checkpoint.Identity{Type: typ, ID: id, InvSeq: handle.InvSeq}, Stage: stage, Data: json.RawMessage(`{"total":23}`), Anchor: checkpoint.Anchor{Index: index, Epoch: 51}, StepPosition: index, State: map[string]json.RawMessage{"total": json.RawMessage(`23`)}, PromiseOutcomes: map[string]json.RawMessage{"child_0": outcome}}
		raw, hash, err := checkpoint.Encode(frame)
		if err != nil {
			t.Fatal(err)
		}
		name := "step-result-" + hash
		if _, err := objects.PutBytes(ctx, name, raw); err != nil {
			t.Fatal(err)
		}
		inputHash := sha256.Sum256(frame.Data)
		request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": stage, "input_hash": hex.EncodeToString(inputHash[:])})
		completion, _ := json.Marshal(map[string]string{"result_ref": name, "result_hash": hash})
		appendEntry(journal.StepRequested, request)
		appendEntry(journal.StepCompleted, completion)
		return journal.RuntimeCheckpoint{InvSeq: handle.InvSeq, Stage: stage, Sequence: tail, Index: index, Epoch: 51, StepPosition: index, Object: name, SHA256: hash}
	}
	runtime := publishPair("collect_v1")
	snap, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Version != 2 || snap.Runtime == nil || *snap.Runtime != runtime || snap.LastIndex != runtime.Index-1 || snap.LastSeq >= runtime.Sequence {
		t.Fatalf("snapshot=%+v", snap)
	}
	if err := journal.New(all[1]).PurgeSnapshot(ctx, typ, id, snap); err != nil {
		t.Fatal(err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetMsg(ctx, runtime.Sequence); err != nil {
		t.Fatalf("anchor missing: %v", err)
	}
	if _, err := stream.GetMsg(ctx, records[0].Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("prefix retained: %v", err)
	}
	logical, gotTail, err := journal.New(all[2]).Read(ctx, typ, id)
	if err != nil || gotTail != tail || !reflect.DeepEqual(logical, records) {
		t.Fatalf("logical replay differs: %v", err)
	}
	if _, err := store.WriteSnapshot(ctx, typ, id, 1); !errors.Is(err, journal.ErrCheckpointCompaction) {
		t.Fatalf("generic compaction=%v", err)
	}
	duplicate, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	if err != nil || !reflect.DeepEqual(duplicate, snap) {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	newer := publishPair("finish_v1")
	next, err := store.WriteCheckpointSnapshot(ctx, typ, id, newer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime); !errors.Is(err, journal.ErrSnapshotStale) {
		t.Fatalf("older pointer=%v", err)
	}
	if err := store.PurgeSnapshot(ctx, typ, id, next); err != nil {
		t.Fatal(err)
	}
	logical, gotTail, err = journal.New(all[1]).Read(ctx, typ, id)
	if err != nil || gotTail != tail || !reflect.DeepEqual(logical, records) {
		t.Fatalf("second logical replay differs: %v", err)
	}
	if _, err := objects.PutBytes(ctx, "input-checkpoint-orphan", []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	swept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || swept.Deleted < 1 {
		t.Fatalf("sweep=%+v err=%v", swept, err)
	}
	for _, name := range []string{runtime.Object, newer.Object, next.Object, childName} {
		if _, err := objects.GetInfo(ctx, name); err != nil {
			t.Fatalf("reachable %s: %v", name, err)
		}
	}
	// Corrupt the current content-addressed frame and prove sweep aborts before
	// deleting a newly eligible orphan; this is injected corruption, not NATS.
	if _, err := objects.PutBytes(ctx, newer.Object, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.PutBytes(ctx, "input-checkpoint-abort-orphan", []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	swept, err = retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err == nil || swept.Deleted != 0 {
		t.Fatalf("corrupt sweep=%+v err=%v", swept, err)
	}
	if _, err := objects.GetInfo(ctx, "input-checkpoint-abort-orphan"); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id)); err != nil {
		t.Fatal(err)
	}
}
