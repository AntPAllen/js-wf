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

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

// runWorkerCompactor executes a real handler while an independent compactor
// observes and purges the same journal at scheduler selected transport calls.
func runWorkerCompactor(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_compactor"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "object_drop", "object_ack_lost", "manifest_drop", "manifest_ack_lost", "purge_drop", "purge_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	subject := identity.JournalSubject(typ, id)
	transport := NewWorkerTransport(schedule, 3*time.Second)
	live := NewJournalTransport(schedule)
	snapshots := NewSnapshotReadTransport(schedule)
	snapshots.BindJournal(live)
	store := journal.NewWithSnapshotPort(live, live, snapshots)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	faults := map[string]SnapshotFault{
		"object_drop":       {Operation: "put_object", Kind: DropBeforeCommit},
		"object_ack_lost":   {Operation: "put_object", Kind: LoseAckAfterCommit},
		"manifest_drop":     {Operation: "create_manifest", Kind: DropBeforeCommit},
		"manifest_ack_lost": {Operation: "create_manifest", Kind: LoseAckAfterCommit},
		"purge_drop":        {Operation: "purge_journal", Kind: DropBeforeCommit},
		"purge_ack_lost":    {Operation: "purge_journal", Kind: LoseAckAfterCommit},
	}
	if mode != "clean" {
		if err := snapshots.QueueWriteFault(faults[mode]); err != nil {
			return trace, err
		}
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	var calls, effects, startEntries int
	actors := []CooperativeActor{
		{Name: "worker", Run: func(ctx context.Context, yield YieldFunc) error {
			workerStore := journal.NewWithSnapshotPort(
				yieldingAppendPort{yield: yield, transport: live},
				yieldingSnapshotReadPort{yield: yield, transport: live},
				yieldingSnapshotPort{yield: yield, transport: snapshots})
			w, err := worker.NewWithPorts("compactor-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				calls++
				total := 0
				for i := 0; i < 32; i++ {
					value, err := wf.Run(c, "step", i, func(context.Context) (int, error) { effects++; return i, nil })
					if err != nil {
						return nil, err
					}
					total += value
				}
				return json.Marshal(total)
			}}, worker.ModeledWorkerPorts{Journal: workerStore, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c})
			if err != nil {
				return err
			}
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			transport.Dispatch.StopWhenDrained(stop)
			if err := w.RunPartitionWithTransport(runCtx, 0, transport.Dispatch); err != nil {
				return err
			}
			if pending := transport.Dispatch.Pending(); pending != 0 {
				return fmt.Errorf("worker has %d pending messages", pending)
			}
			return nil
		}},
		{Name: "compactor", Run: func(ctx context.Context, yield YieldFunc) error {
			// Wait for an active handler journal; the scheduler chooses each
			// observation, so a replay preserves the compactor's start point.
			for attempts := 0; attempts < 200; attempts++ {
				var ready bool
				if err := yield(ctx, "observe_journal", func() {
					startEntries = len(live.Messages(subject))
					ready = startEntries >= 12
				}); err != nil {
					return err
				}
				if ready {
					compactorStore := journal.NewWithSnapshotPort(
						yieldingAppendPort{yield: yield, transport: live},
						yieldingSnapshotReadPort{yield: yield, transport: live},
						yieldingSnapshotPort{yield: yield, transport: snapshots})
					_, err := compactorStore.SnapshotPrefix(ctx, typ, id, 4)
					return err
				}
			}
			return fmt.Errorf("compactor never observed handler journal")
		}},
	}
	results, err := RunCooperative(ctx, schedule, actors)
	if err != nil {
		return trace, err
	}
	if results["worker"] != nil {
		return trace, fmt.Errorf("seed %d worker: %w", seed, results["worker"])
	}
	if compactErr := results["compactor"]; compactErr != nil && !errors.Is(compactErr, ErrTransportLost) && !errors.Is(compactErr, journal.ErrSnapshotStale) {
		return trace, fmt.Errorf("seed %d compactor: %w", seed, compactErr)
	}
	if mode == "clean" && results["compactor"] != nil || mode != "clean" && !errors.Is(results["compactor"], ErrTransportLost) {
		return trace, fmt.Errorf("seed %d mode=%s compactor: %v", seed, mode, results["compactor"])
	}
	if startEntries >= 66 {
		return trace, fmt.Errorf("seed %d compactor started after handler completed", seed)
	}
	if _, err := store.SnapshotPrefix(ctx, typ, id, 4); err != nil {
		return trace, fmt.Errorf("seed %d repair compaction: %w", seed, err)
	}
	records, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 66 || records[65].Kind != journal.Completed || calls != 1 || effects != 32 {
		return trace, fmt.Errorf("seed %d records=%d tail=%d calls=%d effects=%d: %v", seed, len(records), tail, calls, effects, err)
	}
	if got := len(live.Messages(subject)); got != 4 {
		return trace, fmt.Errorf("seed %d retained live entries=%d", seed, got)
	}
	raw, err := retainedModelSnapshot(transport.StartTransport, live, outcomes)
	if err != nil {
		return trace, err
	}
	raw.Journals[subject] = records
	report, err := integrity.CheckSnapshot(raw)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 66, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d integrity=%+v: %v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_compactor", Subject: subject, Sequence: tail, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerCompactorReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_COMPACTOR_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runWorkerCompactor(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_COMPACTOR_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runWorkerCompactor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-compactor.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runWorkerCompactor(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker compactor replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-compactor-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerCompactorReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_COMPACTOR_HELPER=1", "SIM_WORKER_COMPACTOR_OUT="+files[i], "FAULT_SEED=1")
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
		t.Fatal("worker compactor trace changed across processes")
	}
}
