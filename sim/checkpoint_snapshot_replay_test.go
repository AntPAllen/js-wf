package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"js-wf/identity"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

func runSeededCheckpointSnapshot(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("checkpoint_snapshot"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	modes := []string{"clean", "object_drop", "object_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost", "update_drop", "update_ack_lost"}
	mode, err := schedule.Choose(modes)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ, id = "test", "checkpoint"
	live := NewJournalTransport(schedule)
	port := NewSnapshotReadTransport(schedule)
	port.BindJournal(live)
	store := journal.NewWithSnapshotPort(live, live, port)
	var records []journal.Record
	var tail uint64
	add := func(kind journal.Kind, payload []byte) error {
		entry := journal.Entry{Index: uint64(len(records)), Epoch: 51, Kind: kind, Payload: payload, WorkerID: "checkpoint-owner"}
		seq, err := store.Append(ctx, typ, id, entry, tail)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		tail = seq
		return nil
	}
	for _, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted} {
		if err := add(kind, nil); err != nil {
			return trace, err
		}
	}
	pair := func(stage string) (journal.RuntimeCheckpoint, error) {
		index := uint64(len(records) + 1)
		frame := checkpoint.Frame{Version: 1, Identity: checkpoint.Identity{Type: typ, ID: id, InvSeq: 17}, Stage: stage, Data: json.RawMessage(`23`), Anchor: checkpoint.Anchor{Index: index, Epoch: 51}, StepPosition: index, State: map[string]json.RawMessage{"value": json.RawMessage(`23`)}, PromiseOutcomes: map[string]json.RawMessage{}}
		raw, hash, err := checkpoint.Encode(frame)
		if err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		object := "step-result-" + hash
		if err := port.PutObject(ctx, object, raw); err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		input := sha256.Sum256(frame.Data)
		request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": stage, "input_hash": hex.EncodeToString(input[:])})
		done, _ := json.Marshal(map[string]string{"result_ref": object, "result_hash": hash})
		if err := add(journal.StepRequested, request); err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		if err := add(journal.StepCompleted, done); err != nil {
			return journal.RuntimeCheckpoint{}, err
		}
		return journal.RuntimeCheckpoint{InvSeq: 17, Stage: stage, Sequence: tail, Index: index, Epoch: 51, StepPosition: index, Object: object, SHA256: hash}, nil
	}
	runtime, err := pair("next_v1")
	if err != nil {
		return trace, err
	}
	faults := map[string]SnapshotFault{
		"object_drop": {"put_object", DropBeforeCommit}, "object_ack_lost": {"put_object", LoseAckAfterCommit},
		"manifest_drop": {"create_manifest", DropBeforeCommit}, "manifest_ack_lost": {"create_manifest", LoseAckAfterCommit},
		"purge_drop": {"purge_journal", DropBeforeCommit}, "purge_ack_lost": {"purge_journal", LoseAckAfterCommit},
	}
	if fault, ok := faults[mode]; ok {
		if err := port.QueueWriteFault(fault); err != nil {
			return trace, err
		}
	}
	snap, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	if err != nil {
		snap, err = store.WriteCheckpointSnapshot(ctx, typ, id, runtime)
	}
	if err != nil {
		return trace, err
	}
	if snap.Runtime == nil || *snap.Runtime != runtime {
		return trace, fmt.Errorf("wrong runtime pointer")
	}
	err = store.PurgeSnapshot(ctx, typ, id, snap)
	if err != nil {
		err = store.PurgeSnapshot(ctx, typ, id, snap)
	}
	if err != nil {
		return trace, err
	}
	if _, err := store.WriteSnapshot(ctx, typ, id, 1); !errors.Is(err, journal.ErrCheckpointCompaction) {
		return trace, fmt.Errorf("generic compaction=%v", err)
	}
	if len(live.Messages(identity.JournalSubject(typ, id))) != 1 {
		return trace, fmt.Errorf("anchor purge boundary")
	}
	next, err := pair("done_v1")
	if err != nil {
		return trace, err
	}
	if mode == "update_drop" || mode == "update_ack_lost" {
		fault := SnapshotFault{"update_manifest", DropBeforeCommit}
		if mode == "update_ack_lost" {
			fault.Kind = LoseAckAfterCommit
		}
		if err := port.QueueWriteFault(fault); err != nil {
			return trace, err
		}
	}
	second, err := store.WriteCheckpointSnapshot(ctx, typ, id, next)
	if err != nil {
		second, err = store.WriteCheckpointSnapshot(ctx, typ, id, next)
	}
	if err != nil {
		return trace, err
	}
	if _, err := store.WriteCheckpointSnapshot(ctx, typ, id, runtime); !errors.Is(err, journal.ErrSnapshotStale) {
		return trace, fmt.Errorf("older checkpoint=%v", err)
	}
	if err := store.PurgeSnapshot(ctx, typ, id, second); err != nil {
		return trace, err
	}
	logical, gotTail, err := store.Read(ctx, typ, id)
	if err != nil || gotTail != tail || !reflect.DeepEqual(logical, records) || len(live.Messages(identity.JournalSubject(typ, id))) != 1 {
		return trace, fmt.Errorf("logical reconstruction: %v", err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_checkpoint_snapshot", Subject: identity.JournalSubject(typ, id), Sequence: second.LastSeq, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededCheckpointSnapshotReplay(t *testing.T) {
	if os.Getenv("SIM_CHECKPOINT_SNAPSHOT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededCheckpointSnapshot(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CHECKPOINT_SNAPSHOT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runSeededCheckpointSnapshot(seed, nil)
		if err != nil {
			root := os.Getenv("FAULT_TRACE_OUT")
			if root == "" {
				dir, mkdirErr := os.MkdirTemp("", "js-wf-checkpoint-failure-")
				if mkdirErr != nil {
					t.Fatal(mkdirErr)
				}
				root = filepath.Join(dir, "trace.json")
			}
			_ = trace.Save(root)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, root, err)
		}
		observed[trace.Decisions[0].Chosen]++
		if seed <= 10 {
			again, err := runSeededCheckpointSnapshot(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "object_drop", "object_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost", "update_drop", "update_ack_lost"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s not covered", mode)
		}
	}
	var paths [2]string
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "checkpoint.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededCheckpointSnapshotReplay$")
		cmd.Env = append(os.Environ(), "SIM_CHECKPOINT_SNAPSHOT_HELPER=1", "FAULT_SEED=42", "SIM_CHECKPOINT_SNAPSHOT_OUT="+paths[i])
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v %s", i, err, output)
		}
	}
	a, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(paths[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("checkpoint replay changes across processes")
	}
}
