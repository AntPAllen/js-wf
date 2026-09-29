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

	"github.com/nats-io/nats.go"
)

func runSeededSnapshotWrite(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("snapshot_write_compact"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "object_drop", "object_ack_lost", "manifest_drop", "manifest_ack_lost", "journal_purge_drop", "journal_purge_ack_lost", "signal_purge_drop", "signal_purge_ack_lost", "manifest_update_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	const typ = "test"
	id := fmt.Sprintf("compacted-%d", seed)
	subject := identity.JournalSubject(typ, id)
	live := NewJournalTransport(schedule)
	signals := NewSignalTransport(schedule)
	snapshot := NewSnapshotReadTransport(schedule)
	snapshot.BindJournal(live)
	snapshot.BindSignals(signals)
	store := journal.NewWithSnapshotPort(live, live, snapshot)
	signalSeq := signals.CommitSignal(&nats.Msg{Subject: "wf.sig." + typ + "." + id + ".go", Data: []byte(`true`)})
	var records []journal.Record
	add := func(kind journal.Kind, payload []byte) error {
		entry := journal.Entry{Kind: kind, Index: uint64(len(records)), Epoch: 1, WorkerID: "compactor-worker", Payload: payload}
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
	signalPayload, _ := json.Marshal(struct {
		Sequence uint64 `json:"sig_seq"`
	}{signalSeq})
	if err := add(journal.SignalConsumed, signalPayload); err != nil {
		return trace, err
	}
	for i := 0; i < 10; i++ {
		if err := add(journal.StepRequested, []byte(`{"kind":"run"}`)); err != nil {
			return trace, err
		}
		if err := add(journal.StepCompleted, []byte(`{"result":42}`)); err != nil {
			return trace, err
		}
	}
	if err := add(journal.Completed, []byte(`{"inv_seq":1,"result":"NDI="}`)); err != nil {
		return trace, err
	}
	fault := SnapshotFault{}
	switch mode {
	case "object_drop":
		fault = SnapshotFault{"put_object", DropBeforeCommit}
	case "object_ack_lost":
		fault = SnapshotFault{"put_object", LoseAckAfterCommit}
	case "manifest_drop":
		fault = SnapshotFault{"create_manifest", DropBeforeCommit}
	case "manifest_ack_lost":
		fault = SnapshotFault{"create_manifest", LoseAckAfterCommit}
	case "journal_purge_drop":
		fault = SnapshotFault{"purge_journal", DropBeforeCommit}
	case "journal_purge_ack_lost":
		fault = SnapshotFault{"purge_journal", LoseAckAfterCommit}
	case "signal_purge_drop":
		fault = SnapshotFault{"purge_signals", DropBeforeCommit}
	case "signal_purge_ack_lost":
		fault = SnapshotFault{"purge_signals", LoseAckAfterCommit}
	}
	if fault.Operation != "" {
		if err := snapshot.QueueWriteFault(fault); err != nil {
			return trace, err
		}
	}
	firstErr := store.MaybeSnapshot(ctx, typ, id, 10, 6)
	if fault.Operation != "" {
		if !errors.Is(firstErr, ErrTransportLost) {
			return trace, fmt.Errorf("seed %d first snapshot mode=%s err=%v", seed, mode, firstErr)
		}
		if err := store.MaybeSnapshot(ctx, typ, id, 10, 6); err != nil {
			return trace, fmt.Errorf("seed %d snapshot repair: %w", seed, err)
		}
	} else if firstErr != nil {
		return trace, firstErr
	}
	firstRead, tail, err := store.Read(ctx, typ, id)
	if err != nil || !reflect.DeepEqual(firstRead, records) || tail != records[len(records)-1].Sequence || len(live.Messages(subject)) != 6 {
		return trace, fmt.Errorf("seed %d first compaction entries=%d live=%d err=%v", seed, len(firstRead), len(live.Messages(subject)), err)
	}
	if got := signals.SignalFor(typ, id, "go"); len(got) != 0 {
		return trace, fmt.Errorf("seed %d consumed signal retained=%d", seed, len(got))
	}
	firstManifest, err := snapshot.GetManifestRevision(ctx, "snap."+identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	var firstSnap journal.Snapshot
	if err := json.Unmarshal(firstManifest.Value, &firstSnap); err != nil {
		return trace, err
	}
	if mode == "manifest_update_ack_lost" {
		if err := snapshot.QueueWriteFault(SnapshotFault{"update_manifest", LoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	second, secondErr := store.SnapshotPrefix(ctx, typ, id, 2)
	if mode == "manifest_update_ack_lost" {
		if !errors.Is(secondErr, ErrTransportLost) {
			return trace, fmt.Errorf("seed %d second snapshot ack err=%v", seed, secondErr)
		}
		second, secondErr = store.SnapshotPrefix(ctx, typ, id, 2)
	}
	if secondErr != nil || second.LastSeq <= firstSnap.LastSeq {
		return trace, fmt.Errorf("seed %d second snapshot=%+v first=%+v err=%v", seed, second, firstSnap, secondErr)
	}
	secondRead, tail, err := store.Read(ctx, typ, id)
	if err != nil || !reflect.DeepEqual(secondRead, records) || tail != records[len(records)-1].Sequence || len(live.Messages(subject)) != 2 {
		return trace, fmt.Errorf("seed %d second compaction entries=%d live=%d err=%v", seed, len(secondRead), len(live.Messages(subject)), err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_snapshot_write", Subject: subject, Sequence: second.LastSeq, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSnapshotWriteReplay(t *testing.T) {
	if os.Getenv("SIM_SNAPSHOT_WRITE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSnapshotWrite(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SNAPSHOT_WRITE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededSnapshotWrite(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-snapshot-write-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-snapshot-write.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSnapshotWrite(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d snapshot write replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("snapshot-write-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSnapshotWriteReplay$")
		cmd.Env = append(os.Environ(), "SIM_SNAPSHOT_WRITE_HELPER=1", "SIM_SNAPSHOT_WRITE_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("snapshot write trace changed across processes")
	}
}
