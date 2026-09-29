package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/retention"

	"github.com/nats-io/nats.go"
)

func runSeededBlobSweep(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("blob_sweep"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "young_orphan", "delete_drop", "delete_ack_lost", "snapshot_read_fault", "snapshot_corrupt"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	now := time.Unix(0, 0).UTC().Add(3 * time.Hour)
	old := now.Add(-2 * time.Hour)
	model := NewBlobSweepTransport(schedule)
	for _, name := range []string{"input-shared", "signal-snapshot", "step-result-snapshot", "terminal-result-state", "input-orphan", "user-unmanaged", "recent-orphan"} {
		created := old
		if name == "recent-orphan" || name == "input-orphan" && mode == "young_orphan" {
			created = now.Add(-30 * time.Minute)
		}
		model.PutObject(name, []byte(name), created)
	}
	var invSequences []uint64
	for range 2 {
		header := nats.Header{}
		header.Set("Wf-Input-Ref", "input-shared")
		sequence, err := model.Publish("WF_INV", header, []byte(`null`))
		if err != nil {
			return trace, err
		}
		invSequences = append(invSequences, sequence)
	}
	signalHeader := nats.Header{}
	signalHeader.Set("Wf-Signal-Ref", "signal-snapshot")
	signalSequence, err := model.Publish("WF_SIG", signalHeader, []byte(`signal`))
	if err != nil {
		return trace, err
	}
	step, err := json.Marshal(journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"step-result-snapshot"}`)})
	if err != nil {
		return trace, err
	}
	stepSequence, err := model.Publish("WF_JRN", nil, step)
	if err != nil {
		return trace, err
	}
	hole, err := model.Publish("WF_JRN", nil, []byte(`old`))
	if err != nil {
		return trace, err
	}
	terminal, err := json.Marshal(journal.Entry{Kind: journal.Completed, Payload: json.RawMessage(`{"result_ref":"terminal-result-state"}`)})
	if err != nil {
		return trace, err
	}
	terminalSequence, err := model.Publish("WF_JRN", nil, terminal)
	if err != nil {
		return trace, err
	}
	if err := model.Purge("WF_JRN", hole); err != nil {
		return trace, err
	}
	snapshotRecords := []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: json.RawMessage(`{"ref":"signal-snapshot"}`)}}}
	snapshotBytes, err := json.Marshal(snapshotRecords)
	if err != nil {
		return trace, err
	}
	snapshotHash := sha256.Sum256(snapshotBytes)
	model.PutObject("snapshot-one", snapshotBytes, old)
	manifest, err := json.Marshal(journal.Snapshot{Object: "snapshot-one", SHA256: hex.EncodeToString(snapshotHash[:])})
	if err != nil {
		return trace, err
	}
	snapshotRevision, err := model.State().Create(ctx, "snap.test.one", manifest)
	if err != nil {
		return trace, err
	}
	terminalRevision, err := model.State().Create(ctx, "test.two", []byte(`{"result_ref":"terminal-result-state"}`))
	if err != nil {
		return trace, err
	}
	if _, err := model.State().Create(ctx, "scan.cursor.v1", []byte(`1`)); err != nil {
		return trace, err
	}
	switch mode {
	case "delete_drop":
		if err := model.QueueDeleteFault(DropBeforeCommit); err != nil {
			return trace, err
		}
	case "delete_ack_lost":
		if err := model.QueueDeleteFault(LoseAckAfterCommit); err != nil {
			return trace, err
		}
	case "snapshot_read_fault":
		if err := model.QueueReadFault("snapshot-one"); err != nil {
			return trace, err
		}
	case "snapshot_corrupt":
		model.PutObject("snapshot-one", []byte(`corrupt`), old)
	}
	first, firstErr := retention.SweepBlobsQuiescentWithPort(ctx, model, time.Hour, now)
	wantFirstDeleted := 1
	if mode == "young_orphan" {
		wantFirstDeleted = 0
	}
	if mode == "delete_drop" || mode == "delete_ack_lost" || mode == "snapshot_read_fault" || mode == "snapshot_corrupt" {
		if firstErr == nil {
			return trace, fmt.Errorf("seed %d mode %s expected first sweep error: %+v", seed, mode, first)
		}
	} else if firstErr != nil || first.Objects != 8 || first.Referenced != 5 || first.Eligible != first.Deleted || first.Deleted != wantFirstDeleted {
		return trace, fmt.Errorf("seed %d mode %s first sweep=%+v err=%v", seed, mode, first, firstErr)
	}
	if mode == "snapshot_read_fault" || mode == "snapshot_corrupt" {
		if !model.HasObject("input-orphan") {
			return trace, fmt.Errorf("seed %d mark failure deleted orphan", seed)
		}
	}
	for _, name := range []string{"input-shared", "signal-snapshot", "step-result-snapshot", "terminal-result-state", "snapshot-one"} {
		if !model.HasObject(name) {
			return trace, fmt.Errorf("seed %d mode %s swept retained %s", seed, mode, name)
		}
	}
	if mode == "snapshot_corrupt" {
		model.PutObject("snapshot-one", snapshotBytes, old)
	}
	if mode != "young_orphan" {
		if exists := model.HasObject("input-orphan"); exists != (mode == "delete_drop" || mode == "snapshot_read_fault" || mode == "snapshot_corrupt") {
			return trace, fmt.Errorf("seed %d mode %s orphan state after uncertain sweep=%v", seed, mode, exists)
		}
	}
	recovered, err := retention.SweepBlobsQuiescentWithPort(ctx, model, time.Hour, now)
	wantRecoveredDeleted := 0
	if mode == "delete_drop" || mode == "snapshot_read_fault" || mode == "snapshot_corrupt" {
		wantRecoveredDeleted = 1
	}
	if err != nil || recovered.Deleted != wantRecoveredDeleted {
		return trace, fmt.Errorf("seed %d mode %s recovery=%+v err=%v", seed, mode, recovered, err)
	}
	for _, sequence := range invSequences {
		if err := model.Purge("WF_INV", sequence); err != nil {
			return trace, err
		}
	}
	for _, item := range []struct {
		stream   string
		sequence uint64
	}{{"WF_SIG", signalSequence}, {"WF_JRN", stepSequence}, {"WF_JRN", terminalSequence}} {
		if err := model.Purge(item.stream, item.sequence); err != nil {
			return trace, err
		}
	}
	if err := model.State().Delete(ctx, "snap.test.one", snapshotRevision); err != nil {
		return trace, err
	}
	if err := model.State().Delete(ctx, "test.two", terminalRevision); err != nil {
		return trace, err
	}
	last, err := retention.SweepBlobsQuiescentWithPort(ctx, model, time.Hour, now)
	if err != nil || last.Referenced != 0 || last.Deleted != 5 {
		return trace, fmt.Errorf("seed %d mode %s after retention=%+v err=%v", seed, mode, last, err)
	}
	for _, name := range []string{"input-shared", "signal-snapshot", "step-result-snapshot", "terminal-result-state", "snapshot-one"} {
		if model.HasObject(name) {
			return trace, fmt.Errorf("seed %d retained ref %s survived purge", seed, name)
		}
	}
	if !model.HasObject("recent-orphan") || !model.HasObject("user-unmanaged") {
		return trace, fmt.Errorf("seed %d young or unmanaged object removed", seed)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_blob_sweep", Outcome: mode, Sequence: uint64(last.Deleted), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededBlobSweepReplay(t *testing.T) {
	if os.Getenv("SIM_BLOB_SWEEP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededBlobSweep(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_BLOB_SWEEP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededBlobSweep(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "blob-sweep-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededBlobSweep(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "young_orphan", "delete_drop", "delete_ack_lost", "snapshot_read_fault", "snapshot_corrupt"} {
		if observed[mode] == 0 {
			t.Fatalf("mode %s was not scheduled", mode)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("blob-sweep-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededBlobSweepReplay$")
		cmd.Env = append(os.Environ(), "SIM_BLOB_SWEEP_HELPER=1", "SIM_BLOB_SWEEP_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("blob sweep trace changed across processes")
	}
}
