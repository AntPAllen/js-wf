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
	"time"

	"js-wf/identity"
	"js-wf/journal"
)

func runTwoSnapshotCompactors(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("snapshot_two_compactors"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "object_drop", "object_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const typ = "test"
	id := fmt.Sprintf("snapshot-race-%d", seed)
	subject := identity.JournalSubject(typ, id)
	live := NewJournalTransport(schedule)
	snapshots := NewSnapshotReadTransport(schedule)
	snapshots.BindJournal(live)
	store := journal.NewWithSnapshotPort(live, live, snapshots)
	var records []journal.Record
	add := func(kind journal.Kind) error {
		entry := journal.Entry{Kind: kind, Index: uint64(len(records)), Epoch: 1, WorkerID: "worker", Payload: []byte(`{}`)}
		var expected uint64
		if len(records) > 0 {
			expected = records[len(records)-1].Sequence
		}
		sequence, err := store.Append(ctx, typ, id, entry, expected)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: sequence})
		return nil
	}
	if err := add(journal.Started); err != nil {
		return trace, err
	}
	for i := 0; i < 10; i++ {
		if err := add(journal.StepRequested); err != nil {
			return trace, err
		}
		if err := add(journal.StepCompleted); err != nil {
			return trace, err
		}
	}
	if _, err := store.SnapshotPrefix(ctx, typ, id, 6); err != nil {
		return trace, fmt.Errorf("initial snapshot: %w", err)
	}
	for i := 0; i < 2; i++ {
		if err := add(journal.StepRequested); err != nil {
			return trace, err
		}
		if err := add(journal.StepCompleted); err != nil {
			return trace, err
		}
	}
	faults := map[string]SnapshotFault{
		"object_drop":       {Operation: "put_object", Kind: DropBeforeCommit},
		"object_ack_lost":   {Operation: "put_object", Kind: LoseAckAfterCommit},
		"manifest_drop":     {Operation: "update_manifest", Kind: DropBeforeCommit},
		"manifest_ack_lost": {Operation: "update_manifest", Kind: LoseAckAfterCommit},
		"purge_drop":        {Operation: "purge_journal", Kind: DropBeforeCommit},
		"purge_ack_lost":    {Operation: "purge_journal", Kind: LoseAckAfterCommit},
	}
	if mode != "clean" {
		if err := snapshots.QueueWriteFault(faults[mode]); err != nil {
			return trace, err
		}
	}
	actors := []SnapshotActor{
		{Name: "alpha", Run: func(ctx context.Context, store *journal.Store) error {
			_, err := store.SnapshotPrefix(ctx, typ, id, 4)
			return err
		}},
		{Name: "beta", Run: func(ctx context.Context, store *journal.Store) error {
			_, err := store.SnapshotPrefix(ctx, typ, id, 2)
			return err
		}},
	}
	results, err := RunSnapshotActors(ctx, schedule, live, live, snapshots, actors)
	if err != nil {
		return trace, err
	}
	var succeeded, lost bool
	for name, result := range results {
		if result == nil {
			succeeded = true
		} else if errors.Is(result, ErrTransportLost) {
			lost = true
		} else if !errors.Is(result, journal.ErrSnapshotStale) {
			return trace, fmt.Errorf("seed %d actor %s: %w", seed, name, result)
		}
	}
	if mode == "clean" && !succeeded || mode != "clean" && !lost {
		return trace, fmt.Errorf("seed %d mode=%s unexpected compactor results: %v", seed, mode, results)
	}
	// A redelivered compactor resolves a committed write with a lost reply,
	// or performs the write that was dropped before commit.
	if _, err := store.SnapshotPrefix(ctx, typ, id, 4); err != nil {
		return trace, fmt.Errorf("seed %d mode=%s repair: %w", seed, mode, err)
	}
	manifest, err := snapshots.GetManifestRevision(ctx, "snap."+identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	var snap journal.Snapshot
	if err := json.Unmarshal(manifest.Value, &snap); err != nil {
		return trace, err
	}
	read, tail, err := store.Read(ctx, typ, id)
	wantLive := len(records) - 1 - int(snap.LastIndex)
	if err != nil || !reflect.DeepEqual(read, records) || tail != records[len(records)-1].Sequence || len(live.Messages(subject)) != wantLive || (manifest.Revision != 2 && manifest.Revision != 3) || (wantLive != 2 && wantLive != 4) {
		return trace, fmt.Errorf("seed %d race: read=%d live=%d revision=%d lastIndex=%d err=%v", seed, len(read), len(live.Messages(subject)), manifest.Revision, snap.LastIndex, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_snapshot_compactors", Subject: subject, Sequence: snap.LastSeq, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeSnapshotCompactorsReplay(t *testing.T) {
	if os.Getenv("SIM_SNAPSHOT_ACTORS_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoSnapshotCompactors(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SNAPSHOT_ACTORS_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	var conflicted int
	var staleLargerCut int
	observedModes := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runTwoSnapshotCompactors(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "snapshot-compactors-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observedModes[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runTwoSnapshotCompactors(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
		seedConflicted := false
		for _, event := range generated.Transport {
			if event.Operation == "update_snapshot_manifest" && event.Outcome == "revision_mismatch" {
				conflicted++
				seedConflicted = true
			}
		}
		if seedConflicted && generated.Transport[len(generated.Transport)-1].Sequence == 21 {
			staleLargerCut++
		}
	}
	if conflicted == 0 {
		t.Fatal("no seeded writer reached a conflicting manifest CAS")
	}
	if staleLargerCut == 0 {
		t.Fatal("no longer-prefix writer lost the CAS to the shorter-prefix writer")
	}
	for _, mode := range []string{"clean", "object_drop", "object_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost"} {
		if observedModes[mode] == 0 {
			t.Fatalf("fault mode %s was not scheduled", mode)
		}
	}
	t.Logf("conflicting manifest CAS attempts: %d", conflicted)
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("snapshot-compactors-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeSnapshotCompactorsReplay$")
		cmd.Env = append(os.Environ(), "SIM_SNAPSHOT_ACTORS_HELPER=1", "SIM_SNAPSHOT_ACTORS_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("snapshot compactor trace changed across processes")
	}
}
