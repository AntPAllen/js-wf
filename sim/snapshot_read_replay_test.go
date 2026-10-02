package sim

import (
	"bytes"
	"context"
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
	"js-wf/journal"
)

func runSeededSnapshotRead(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("snapshot_read_compacted"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "stale_manifest_first", "transient_object_first", "stale_manifest_new", "transient_object_new"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ = "test"
	id := fmt.Sprintf("snapshot-%d", seed)
	subject := identity.JournalSubject(typ, id)
	live := NewJournalTransport(schedule)
	snapshots := NewSnapshotReadTransport(schedule)
	store := journal.NewWithSnapshotReadPort(live, live, snapshots)
	var records []journal.Record
	add := func(kind journal.Kind, payload []byte) error {
		entry := journal.Entry{Kind: kind, Index: uint64(len(records)), Epoch: 1, WorkerID: "worker", Payload: payload}
		expected := uint64(0)
		if len(records) > 0 {
			expected = records[len(records)-1].Sequence
		}
		seq, err := store.Append(ctx, typ, id, entry, expected)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: seq})
		return nil
	}
	if err := add(journal.Started, nil); err != nil {
		return trace, err
	}
	for step := 0; step < 12; step++ {
		request, _ := json.Marshal(map[string]any{"kind": "run", "name": fmt.Sprintf("step-%02d", step)})
		if err := add(journal.StepRequested, request); err != nil {
			return trace, err
		}
		if err := add(journal.StepCompleted, []byte(`{"result":42}`)); err != nil {
			return trace, err
		}
	}
	if err := add(journal.Completed, []byte(`{"inv_seq":1,"result":"NDI="}`)); err != nil {
		return trace, err
	}
	keepChoice, err := schedule.Choose([]string{"3", "4", "5", "6"})
	if err != nil {
		return trace, err
	}
	keep, _ := strconv.Atoi(keepChoice)
	firstCut := len(records) - keep - 2
	firstSnap, err := snapshots.SetSnapshot(typ, id, records[:firstCut])
	if err != nil {
		return trace, err
	}
	live.PurgeBefore(subject, firstSnap.LastSeq+1)
	if mode == "stale_manifest_first" {
		snapshots.QueueManifestMiss()
	}
	if mode == "transient_object_first" {
		snapshots.QueueObjectTransient()
	}
	read, tail, err := store.Read(ctx, typ, id)
	if err != nil || tail != records[len(records)-1].Sequence || !reflect.DeepEqual(read, records) {
		return trace, fmt.Errorf("seed %d first snapshot read entries=%d tail=%d err=%v", seed, len(read), tail, err)
	}
	secondCut := len(records) - keep
	secondSnap, err := snapshots.SetSnapshot(typ, id, records[:secondCut])
	if err != nil {
		return trace, err
	}
	live.PurgeBefore(subject, secondSnap.LastSeq+1)
	if mode == "stale_manifest_new" {
		snapshots.QueueManifestMiss()
	}
	if mode == "transient_object_new" {
		snapshots.QueueObjectTransient()
	}
	read, tail, err = store.Read(ctx, typ, id)
	if err != nil || tail != records[len(records)-1].Sequence || !reflect.DeepEqual(read, records) {
		return trace, fmt.Errorf("seed %d advanced snapshot read entries=%d tail=%d err=%v", seed, len(read), tail, err)
	}
	if len(live.Messages(subject)) != keep || secondSnap.LastSeq <= firstSnap.LastSeq {
		return trace, fmt.Errorf("seed %d retained live=%d first=%d second=%d", seed, len(live.Messages(subject)), firstSnap.LastSeq, secondSnap.LastSeq)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_snapshot_read", Subject: subject, Sequence: tail, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSnapshotReadReplay(t *testing.T) {
	if os.Getenv("SIM_SNAPSHOT_READ_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSnapshotRead(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SNAPSHOT_READ_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededSnapshotRead(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-snapshot-read-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-snapshot-read.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSnapshotRead(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d snapshot read replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("snapshot-read-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSnapshotReadReplay$")
		cmd.Env = append(os.Environ(), "SIM_SNAPSHOT_READ_HELPER=1", "SIM_SNAPSHOT_READ_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("snapshot read trace changed across processes")
	}
}

func TestSnapshotReadFailsClosedOnCorruptObject(t *testing.T) {
	schedule := NewScheduler(99)
	live := NewJournalTransport(schedule)
	snapshots := NewSnapshotReadTransport(schedule)
	store := journal.NewWithSnapshotReadPort(live, live, snapshots)
	ctx := context.Background()
	var records []journal.Record
	for index, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Completed} {
		entry := journal.Entry{Kind: kind, Index: uint64(index), Epoch: 1, WorkerID: "worker", Payload: []byte(`{}`)}
		expected := uint64(0)
		if len(records) != 0 {
			expected = records[len(records)-1].Sequence
		}
		sequence, err := store.Append(ctx, "test", "corrupt-snapshot", entry, expected)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, journal.Record{Entry: entry, Sequence: sequence})
	}
	snap, err := snapshots.SetSnapshot("test", "corrupt-snapshot", records[:2])
	if err != nil {
		t.Fatal(err)
	}
	live.PurgeBefore(identity.JournalSubject("test", "corrupt-snapshot"), snap.LastSeq+1)
	if err := snapshots.CorruptObject(snap.Object); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Read(ctx, "test", "corrupt-snapshot"); !errors.Is(err, journal.ErrGap) || schedule.NowMillis() != 1975 {
		t.Fatalf("corrupt snapshot: time=%d err=%v", schedule.NowMillis(), err)
	}
}
