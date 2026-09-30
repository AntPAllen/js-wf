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

func TestJournalAppendFaultBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("ack lost after commit", func(t *testing.T) {
		s := NewScheduler(1)
		model := NewJournalTransport(s)
		if err := model.QueueFault(Fault{Kind: LoseAckAfterCommit}); err != nil {
			t.Fatal(err)
		}
		store := journal.NewWithAppendPort(model)
		entry := journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}
		if _, err := store.Append(ctx, "test", "ack-lost", entry, 0); !errors.Is(err, journal.ErrUnknown) {
			t.Fatalf("lost acknowledgment: %v", err)
		}
		subject := identity.JournalSubject("test", "ack-lost")
		messages := model.Messages(subject)
		if len(messages) != 1 || messages[0].Sequence != 1 {
			t.Fatalf("commit after lost acknowledgment: %+v", messages)
		}
		if _, err := store.Append(ctx, "test", "ack-lost", entry, 0); !errors.Is(err, journal.ErrStale) {
			t.Fatalf("retry of committed start: %v", err)
		}
		if seq, err := store.Append(ctx, "test", "ack-lost", journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2}, 1); err != nil || seq != 2 {
			t.Fatalf("resolved append: seq=%d err=%v", seq, err)
		}
	})
	t.Run("drop before commit", func(t *testing.T) {
		model := NewJournalTransport(NewScheduler(2))
		if err := model.QueueFault(Fault{Kind: DropBeforeCommit}); err != nil {
			t.Fatal(err)
		}
		store := journal.NewWithAppendPort(model)
		entry := journal.Entry{Kind: journal.Started, Index: 0}
		if _, err := store.Append(ctx, "test", "drop", entry, 0); !errors.Is(err, journal.ErrUnknown) {
			t.Fatalf("dropped request: %v", err)
		}
		if len(model.Messages(identity.JournalSubject("test", "drop"))) != 0 {
			t.Fatal("dropped request committed")
		}
		if seq, err := store.Append(ctx, "test", "drop", entry, 0); err != nil || seq != 1 {
			t.Fatalf("retry of absent start: seq=%d err=%v", seq, err)
		}
	})
	t.Run("unchanged tail reject", func(t *testing.T) {
		model := NewJournalTransport(NewScheduler(3))
		if err := model.QueueFault(Fault{Kind: RejectUnchanged}); err != nil {
			t.Fatal(err)
		}
		seq, err := journal.NewWithAppendPort(model).Append(ctx, "test", "reject", journal.Entry{Kind: journal.Started, Index: 0}, 0)
		if err != nil || seq != 1 || model.schedule.NowMillis() != 25 {
			t.Fatalf("unchanged tail retry: seq=%d time=%d err=%v", seq, model.schedule.NowMillis(), err)
		}
	})
	t.Run("persistent unchanged tail reject", func(t *testing.T) {
		model := NewJournalTransport(NewScheduler(4))
		for i := 0; i < 40; i++ {
			if err := model.QueueFault(Fault{Kind: RejectUnchanged}); err != nil {
				t.Fatal(err)
			}
		}
		_, err := journal.NewWithAppendPort(model).Append(ctx, "test", "reject-forty", journal.Entry{Kind: journal.Started, Index: 0}, 0)
		if !errors.Is(err, journal.ErrUnknown) || errors.Is(err, journal.ErrStale) || model.schedule.NowMillis() != 975 {
			t.Fatalf("persistent reject: time=%d err=%v", model.schedule.NowMillis(), err)
		}
	})
	t.Run("competing writer advances tail", func(t *testing.T) {
		model := NewJournalTransport(NewScheduler(5))
		store := journal.NewWithAppendPort(model)
		first, err := store.Append(ctx, "test", "race", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
		if err != nil {
			t.Fatal(err)
		}
		competing, _ := json.Marshal(journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2})
		if err := model.QueueFault(Fault{Kind: CompetingCommit, CompetingData: competing}); err != nil {
			t.Fatal(err)
		}
		_, err = store.Append(ctx, "test", "race", journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 1}, first)
		if !errors.Is(err, journal.ErrStale) || len(model.Messages(identity.JournalSubject("test", "race"))) != 2 {
			t.Fatalf("competing CAS: err=%v messages=%+v", err, model.Messages(identity.JournalSubject("test", "race")))
		}
	})
}

func runSeededAppendScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
	var s *Scheduler
	if replay == nil {
		s = NewScheduler(seed)
	} else {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("journal_append_100"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	model := NewJournalTransport(s)
	store := journal.NewWithAppendPort(model)
	for i := 0; i < 100; i++ {
		choice, err := s.Choose([]string{"ok", "drop", "ack_lost", "reject_once", "reject_four", "reject_forty"})
		if err != nil {
			return Trace{}, err
		}
		switch choice {
		case "drop":
			err = model.QueueFault(Fault{Kind: DropBeforeCommit})
		case "ack_lost":
			err = model.QueueFault(Fault{Kind: LoseAckAfterCommit})
		case "reject_once":
			err = model.QueueFault(Fault{Kind: RejectUnchanged})
		case "reject_four", "reject_forty":
			count := 4
			if choice == "reject_forty" {
				count = 40
			}
			for j := 0; j < count; j++ {
				if err = model.QueueFault(Fault{Kind: RejectUnchanged}); err != nil {
					return Trace{}, err
				}
			}
		}
		if err != nil {
			return Trace{}, err
		}
		id := fmt.Sprintf("case-%03d", i)
		seq, appendErr := store.Append(context.Background(), "test", id, journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
		messages := model.Messages(identity.JournalSubject("test", id))
		wantMessages := 1
		if choice == "drop" || choice == "reject_forty" {
			wantMessages = 0
		}
		if len(messages) != wantMessages {
			return Trace{}, fmt.Errorf("seed %d case %d choice=%s messages=%d want=%d", seed, i, choice, len(messages), wantMessages)
		}
		if choice == "ok" || choice == "reject_once" || choice == "reject_four" {
			if appendErr != nil || seq == 0 {
				return Trace{}, fmt.Errorf("seed %d case %d choice=%s seq=%d err=%v", seed, i, choice, seq, appendErr)
			}
		} else if !errors.Is(appendErr, journal.ErrUnknown) {
			return Trace{}, fmt.Errorf("seed %d case %d choice=%s err=%v", seed, i, choice, appendErr)
		}
	}
	if err := s.Finish(); err != nil {
		return Trace{}, err
	}
	return s.Trace(), nil
}

func TestSeededAppendTraceReplaysAcrossProcesses(t *testing.T) {
	if os.Getenv("SIM_APPEND_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededAppendScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_APPEND_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	seed := int64(173)
	if raw := os.Getenv("FAULT_SEED"); raw != "" {
		var err error
		seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("trace-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededAppendTraceReplaysAcrossProcesses$")
		cmd.Env = append(os.Environ(), "SIM_APPEND_TRACE_HELPER=1", "SIM_APPEND_TRACE_OUT="+files[i], "FAULT_SEED="+strconv.FormatInt(seed, 10))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("seed %d child %d: %v: %s", seed, i, err, output)
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
		t.Fatalf("FAULT_SEED=%d yielded different process traces", seed)
	}
	loaded, err := LoadTrace(files[0])
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := runSeededAppendScenario(seed, &loaded)
	if err != nil || !reflect.DeepEqual(loaded, replayed) {
		t.Fatalf("FAULT_SEED=%d trace replay diverged: %v", seed, err)
	}
	s, err := ReplayScheduler(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Choose([]string{"changed_enabled_set"}); err == nil {
		t.Fatal("trace replay accepted a different enabled set")
	}
}

func TestThousandSeededJournalAppendScenarios(t *testing.T) {
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runSeededAppendScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-sim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			}
			if saveErr := trace.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
	}
}

func runTwoWriterCAS(seed int64, replay *Trace) (Trace, error) {
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
	if err := schedule.SetWorkload("journal_two_writer_cas"); err != nil {
		return Trace{}, err
	}
	model := NewJournalTransport(schedule)
	first, err := journal.NewWithAppendPort(model).Append(context.Background(), "test", "two-writers", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	if err != nil {
		return Trace{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	actors := make([]AppendActor, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		name := name
		actors = append(actors, AppendActor{Name: name, Run: func(ctx context.Context, port journal.AppendPort) error {
			_, err := journal.NewWithAppendPort(port).Append(ctx, "test", "two-writers", journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2, WorkerID: name}, first)
			return err
		}})
	}
	results, err := RunAppendActors(ctx, schedule, model, actors)
	if err != nil {
		return schedule.Trace(), err
	}
	successes, stale := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, journal.ErrStale):
			stale++
		default:
			return schedule.Trace(), fmt.Errorf("unexpected writer result: %v", err)
		}
	}
	messages := model.Messages(identity.JournalSubject("test", "two-writers"))
	if successes != 1 || stale != 1 || len(messages) != 2 || messages[1].Sequence != 2 {
		return schedule.Trace(), fmt.Errorf("CAS winners=%d stale=%d messages=%+v", successes, stale, messages)
	}
	var winner journal.Entry
	if err := json.Unmarshal(messages[1].Data, &winner); err != nil {
		return schedule.Trace(), err
	}
	winnerErr, knownWinner := results[winner.WorkerID]
	if !knownWinner || winnerErr != nil {
		return schedule.Trace(), fmt.Errorf("retained winner %q differs from results", winner.WorkerID)
	}
	if err := schedule.Finish(); err != nil {
		return schedule.Trace(), err
	}
	return schedule.Trace(), nil
}

func TestCooperativeTwoWriterCASReplay(t *testing.T) {
	if os.Getenv("SIM_CAS_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoWriterCAS(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CAS_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 100; seed++ {
		generated, err := runTwoWriterCAS(seed, nil)
		if err != nil {
			t.Fatalf("FAULT_SEED=%d: %v", seed, err)
		}
		replayed, err := runTwoWriterCAS(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("FAULT_SEED=%d CAS trace replay: %v", seed, err)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("cas-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeTwoWriterCASReplay$")
		cmd.Env = append(os.Environ(), "SIM_CAS_TRACE_HELPER=1", "SIM_CAS_TRACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("two-writer child %d: %v: %s", i, err, output)
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
		t.Fatal("cooperative two-writer trace changed across processes")
	}
}

func replayTrace(loaded Trace) (Trace, error) {
	var replayed Trace
	var err error
	switch loaded.Workload {
	case "journal_append_100":
		replayed, err = runSeededAppendScenario(loaded.Seed, &loaded)
	case "journal_batch_read_80":
		replayed, err = runSeededJournalBatchRead(loaded.Seed, &loaded)
	case "lease_heartbeat_reuse":
		replayed, err = runLeaseHeartbeatReuse(loaded.Seed, &loaded)
	case "journal_open_failure":
		replayed, err = runJournalOpenFailure(loaded.Seed, &loaded)
	case "journal_stalled_cursor":
		replayed, err = runStalledJournalCursor(loaded.Seed, &loaded)
	case "signal_history_duplicate_windows":
		replayed, err = runSignalHistoryWindow(loaded.Seed, &loaded)
	case "lease_100":
		replayed, err = runSeededLeaseScenario(loaded.Seed, &loaded)
	case "client_start_repair_20":
		replayed, err = runSeededStartScenario(loaded.Seed, &loaded)
	case "journal_two_writer_cas":
		replayed, err = runTwoWriterCAS(loaded.Seed, &loaded)
	case "journal_two_writer_unknown":
		replayed, err = runTwoWriterUnknownCAS(loaded.Seed, &loaded)
	case "journal_two_writer_tail_lookup":
		replayed, err = runTwoWriterTailLookupRecovery(loaded.Seed, &loaded)
	case "lease_two_acquirer_fresh":
		replayed, err = runTwoAcquirerRace(loaded.Seed, false, &loaded)
	case "lease_two_acquirer_orphan":
		replayed, err = runTwoAcquirerRace(loaded.Seed, true, &loaded)
	case "lease_expiry_32_acquirers":
		replayed, err = runLeaseExpiryRace(loaded.Seed, &loaded)
	case "lease_clock_skew":
		replayed, err = runSkewedLeaseAcquirers(loaded.Seed, &loaded)
	case "client_two_starter_same":
		replayed, err = runTwoStarterRace(loaded.Seed, false, &loaded)
	case "client_two_starter_mismatch":
		replayed, err = runTwoStarterRace(loaded.Seed, true, &loaded)
	case "client_start_scan_race":
		replayed, err = runConcurrentStartRepair(loaded.Seed, &loaded)
	case "dispatch_20":
		replayed, err = runSeededDispatchScenario(loaded.Seed, &loaded)
	case "dispatch_two_workers":
		replayed, err = runTwoDispatchWorkers(loaded.Seed, &loaded)
	case "signal_repair_20":
		replayed, err = runSeededSignalRepairScenario(loaded.Seed, &loaded)
	case "signal_publish_scan_race":
		replayed, err = runConcurrentSignalRepair(loaded.Seed, &loaded)
	case "client_signal_repair_20":
		replayed, err = runSeededClientSignalScenario(loaded.Seed, &loaded)
	case "client_signal_scan_race":
		replayed, err = runConcurrentClientSignalRepair(loaded.Seed, &loaded)
	case "worker_signal_drain_20":
		replayed, err = runSeededWorkerSignalDrain(loaded.Seed, &loaded)
	case "signal_pipeline_20":
		replayed, err = runSeededSignalPipeline(loaded.Seed, &loaded)
	case "child_notification_20":
		replayed, err = runSeededChildNotification(loaded.Seed, &loaded)
	case "outcome_persistence_20":
		replayed, err = runSeededOutcomePersistence(loaded.Seed, &loaded)
	case "worker_execution_5":
		replayed, err = runSeededWorkerExecution(loaded.Seed, &loaded)
	case "worker_signal_execution":
		replayed, err = runSeededWorkerSignalExecution(loaded.Seed, &loaded)
	case "journal_manifest_read_recovery":
		replayed, err = runJournalManifestRead(loaded.Seed, &loaded)
	case "journal_serial_read_recovery":
		replayed, err = runJournalSerialRead(loaded.Seed, &loaded)
	case "worker_running_cancel":
		replayed, err = runSeededWorkerRunningCancel(loaded.Seed, &loaded)
	case "worker_heartbeat_execution":
		replayed, err = runSeededWorkerHeartbeat(loaded.Seed, &loaded)
	case "worker_heartbeat_handoff":
		replayed, err = runSeededHeartbeatHandoff(loaded.Seed, &loaded)
	case "worker_signal_write_latency":
		replayed, err = runWorkerSignalWriteLatency(loaded.Seed, &loaded)
	case "lease_gate_cancellation":
		replayed, err = runLeaseGateCancellation(loaded.Seed, &loaded)
	case "handle_cache_cancellation":
		replayed, err = runHandleCacheCancellation(loaded.Seed, &loaded)
	case "worker_successive_kills", "worker_successive_kills_v2":
		replayed, err = runSuccessiveWorkerKills(loaded.Seed, &loaded)
	case "worker_kill_lease_expiry":
		replayed, err = runWorkerKillLeaseExpiry(&loaded)
	case "worker_canceled_retained_lease":
		replayed, err = runCanceledWorkerRetainedLease(&loaded)
	case "tombstone_sweep":
		replayed, err = runSeededTombstoneSweep(loaded.Seed, &loaded)
	case "blob_sweep":
		replayed, err = runSeededBlobSweep(loaded.Seed, &loaded)
	case "purge_blob":
		replayed, err = runSeededPurgeBlob(loaded.Seed, &loaded)
	case "snapshot_purge_lease":
		replayed, err = runSnapshotPurgeActors(loaded.Seed, &loaded)
	case "snapshot_append_purge":
		replayed, err = runSnapshotAppendPurgeActors(loaded.Seed, &loaded)
	case "failure_probe":
		replayed, err = runProbeFailure(&loaded)
	case "worker_timer_execution", "worker_timer_execution_v2":
		replayed, err = runSeededWorkerTimerExecution(loaded.Seed, &loaded)
	case "worker_fallback_timer_execution":
		replayed, err = runSeededWorkerFallbackTimerExecution(loaded.Seed, &loaded)
	case "worker_fallback_timer_loop_execution":
		replayed, err = runSeededWorkerFallbackTimerLoopExecution(loaded.Seed, &loaded)
	case "worker_select_execution":
		replayed, err = runSeededWorkerSelectExecution(loaded.Seed, &loaded)
	case "worker_fanout_6":
		replayed, err = runSeededWorkerFanout(loaded.Seed, &loaded, 6)
	case "worker_fanout_500":
		replayed, err = runSeededWorkerFanout(loaded.Seed, &loaded, 500)
	case "worker_child_execution":
		replayed, err = runSeededWorkerChildExecution(loaded.Seed, &loaded)
	case "worker_large_result":
		replayed, err = runSeededWorkerLargeResult(loaded.Seed, &loaded)
	case "snapshot_read_compacted":
		replayed, err = runSeededSnapshotRead(loaded.Seed, &loaded)
	case "snapshot_write_compact":
		replayed, err = runSeededSnapshotWrite(loaded.Seed, &loaded)
	case "snapshot_two_compactors":
		replayed, err = runTwoSnapshotCompactors(loaded.Seed, &loaded)
	case "worker_snapshot_execution":
		replayed, err = runSeededWorkerSnapshotExecution(loaded.Seed, &loaded)
	case "worker_compactor":
		replayed, err = runWorkerCompactor(loaded.Seed, &loaded)
	case "timer_scan_20":
		replayed, err = runSeededTimerScan(loaded.Seed, &loaded)
	case "reconcile_loop_20":
		replayed, err = runSeededReconcileLoop(loaded.Seed, &loaded)
	case "reconcile_two_scanners":
		replayed, err = runTwoReconcileLoops(loaded.Seed, &loaded)
	case "timer_schedule_20":
		replayed, err = runSeededTimerSchedule(loaded.Seed, &loaded)
	case "fallback_timer_pipeline_20":
		replayed, err = runSeededFallbackPipeline(loaded.Seed, &loaded)
	case "fallback_reconcile_loop_20":
		replayed, err = runSeededFallbackLoop(loaded.Seed, &loaded)
	case "fallback_two_scanners":
		replayed, err = runTwoFallbackLoops(loaded.Seed, &loaded)
	case "select_suspended_scan_20":
		replayed, err = runSeededSelectSuspendedScan(loaded.Seed, &loaded)
	case "suspended_scan_20":
		replayed, err = runSeededSuspendedScan(loaded.Seed, &loaded)
	case "suspended_reconcile_loop_20":
		replayed, err = runSeededSuspendedLoop(loaded.Seed, &loaded)
	case "suspended_two_scanners":
		replayed, err = runTwoSuspendedLoops(loaded.Seed, &loaded)
	case "retained_invariants_5":
		replayed, err = runSeededRetainedInvariantChecks(loaded.Seed, &loaded)
	case "workqueue_retention_33":
		replayed, err = runSeededWorkQueueRetention(loaded.Seed, &loaded)
	case "lease_cleanup_stale_read":
		replayed, err = runSeededCleanupStaleRead(loaded.Seed, &loaded)
	case "lease_cleanup_conflict":
		replayed, err = runSeededCleanupConflict(loaded.Seed, &loaded)
	case "manual_owner_drain":
		replayed, err = runSeededDeadOwnerDrain(loaded.Seed, &loaded)
	case "enqueue_transport_retry":
		replayed, err = runEnqueueTransportRetry(loaded.Seed, &loaded)
	case "paused_coordinator_claim":
		replayed, err = runPausedCoordinatorClaim(loaded.Seed, &loaded)
	case "unchanged_membership_coordinator":
		replayed, err = runUnchangedCoordinator(loaded.Seed, &loaded)
	case "paused_membership_coordinator":
		replayed, err = runPausedCoordinator(loaded.Seed, &loaded)
	case "automatic_membership":
		replayed, err = runSeededMembership(loaded.Seed, &loaded)
	case "select_many":
		replayed, err = runSeededSelectMany(loaded.Seed, &loaded)
	case "workflow_replay_5":
		replayed, err = runSeededWorkflowReplay(loaded.Seed, &loaded)
	default:
		return Trace{}, fmt.Errorf("unknown trace workload %q", loaded.Workload)
	}
	return replayed, err
}

func TestReplayFaultTrace(t *testing.T) {
	path := os.Getenv("FAULT_TRACE")
	if path == "" {
		t.Skip("set FAULT_TRACE to replay a saved Tier 1 trace")
	}
	loaded, err := LoadTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := replayTrace(loaded)
	if err != nil || !reflect.DeepEqual(loaded, replayed) {
		t.Fatalf("FAULT_TRACE=%s FAULT_SEED=%d: %v", path, loaded.Seed, err)
	}
}

func minimizeSavedFaultTrace(input, output string, maxRuns int) (int, error) {
	if input == output {
		return 0, fmt.Errorf("minimized trace would overwrite original")
	}
	loaded, err := LoadTrace(input)
	if err != nil {
		return 0, err
	}
	replayed, originalErr := replayTrace(loaded)
	if originalErr == nil || !reflect.DeepEqual(loaded, replayed) {
		return 0, fmt.Errorf("original failure trace did not replay exactly: %v", originalErr)
	}
	minimized, runs, err := MinimizeFailureTrace(loaded, func(guided *Trace) (Trace, error) {
		return replayTrace(*guided)
	}, func(candidate error) bool {
		return candidate != nil && candidate.Error() == originalErr.Error()
	}, maxRuns)
	if err != nil {
		return runs, err
	}
	if err := minimized.Save(output); err != nil {
		return runs, err
	}
	return runs, nil
}

func TestMinimizeFaultTrace(t *testing.T) {
	input := os.Getenv("FAULT_TRACE")
	if input == "" {
		t.Skip("set FAULT_TRACE to a failing saved Tier 1 trace")
	}
	output := os.Getenv("FAULT_TRACE_MIN_OUT")
	if output == "" {
		output = input + ".minimized.json"
	}
	runs, err := minimizeSavedFaultTrace(input, output, 50)
	if err != nil {
		t.Fatalf("FAULT_TRACE=%s: %v", input, err)
	}
	t.Logf("minimized %s to %s in %d reproductions", input, output, runs)
}

func TestPinnedRegressionCorpus(t *testing.T) {
	paths, err := filepath.Glob("testdata/regressions/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("pinned trace corpus missing: paths=%v err=%v", paths, err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			loaded, err := LoadTrace(path)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := replayTrace(loaded)
			if err != nil || !reflect.DeepEqual(loaded, replayed) {
				t.Fatalf("pinned trace %s seed=%d: %v", path, loaded.Seed, err)
			}
		})
	}
}
