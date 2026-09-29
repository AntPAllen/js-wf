package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

func runSeededWorkerSnapshotExecution(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_snapshot_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "snapshot_object_drop", "snapshot_object_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost", "outcome_ack_lost", "consumer_leader_changed"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	snapshots := NewSnapshotReadTransport(schedule)
	snapshots.BindJournal(live)
	store := journal.NewWithSnapshotPort(live, live, snapshots)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	fault := SnapshotFault{}
	switch mode {
	case "snapshot_object_drop":
		fault = SnapshotFault{"put_object", DropBeforeCommit}
	case "snapshot_object_ack_lost":
		fault = SnapshotFault{"put_object", LoseAckAfterCommit}
	case "manifest_drop":
		fault = SnapshotFault{"create_manifest", DropBeforeCommit}
	case "manifest_ack_lost":
		fault = SnapshotFault{"create_manifest", LoseAckAfterCommit}
	case "purge_drop":
		fault = SnapshotFault{"purge_journal", DropBeforeCommit}
	case "purge_ack_lost":
		fault = SnapshotFault{"purge_journal", LoseAckAfterCommit}
	}
	if fault.Operation != "" {
		if err := snapshots.QueueWriteFault(fault); err != nil {
			return trace, err
		}
	}
	if mode == "outcome_ack_lost" {
		if err := outcomes.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	if mode == "consumer_leader_changed" {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "consumer", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	var calls, effects int
	w, err := worker.NewWithPorts("modeled-snapshot-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls++
		total := 0
		for i := 0; i < 130; i++ {
			value, err := wf.Run(c, "step", i, func(context.Context) (int, error) { effects++; return i, nil })
			if err != nil {
				return nil, err
			}
			total += value
		}
		return json.Marshal(total)
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	runCtx, stopRun := context.WithCancel(ctx)
	transport.Dispatch.StopWhenDrained(stopRun)
	if err := w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); err != nil || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d worker snapshot pending=%d err=%v", seed, transport.Dispatch.Pending(), err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 262 || records[261].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d snapshot records=%d tail=%d err=%v", seed, len(records), tail, err)
	}
	if calls != 1 || effects != 130 {
		return trace, fmt.Errorf("seed %d mode=%s calls=%d effects=%d", seed, mode, calls, effects)
	}
	liveCount := len(live.Messages(identity.JournalSubject(typ, id)))
	if liveCount != 16 {
		return trace, fmt.Errorf("seed %d live journal=%d", seed, liveCount)
	}
	manifest, err := snapshots.GetManifest(ctx, "snap."+identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	var snap journal.Snapshot
	if err := json.Unmarshal(manifest, &snap); err != nil || snap.LastIndex != 245 || snap.LastSeq != records[245].Sequence {
		return trace, fmt.Errorf("seed %d snapshot=%+v err=%v", seed, snap, err)
	}
	raw, err := retainedModelSnapshot(transport.StartTransport, live, outcomes)
	if err != nil {
		return trace, err
	}
	raw.Journals[identity.JournalSubject(typ, id)] = records
	report, err := integrity.CheckSnapshot(raw)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 262, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d snapshot retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_snapshot_execution", Subject: identity.JournalSubject(typ, id), Sequence: tail, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerSnapshotExecutionReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_SNAPSHOT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerSnapshotExecution(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_SNAPSHOT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededWorkerSnapshotExecution(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-snapshot-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-snapshot.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerSnapshotExecution(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker snapshot replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-snapshot-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerSnapshotExecutionReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_SNAPSHOT_HELPER=1", "SIM_WORKER_SNAPSHOT_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker snapshot trace changed across processes")
	}
}
