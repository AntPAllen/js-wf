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

	"js-wf/lease"

	"github.com/nats-io/nats.go/jetstream"
)

func TestLeaseKVFaultBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("stale predecessor read cannot delete initialized lease", func(t *testing.T) {
		schedule := NewScheduler(21)
		model := NewKVTransport(schedule, 30*time.Second)
		store := lease.NewWithKVPort(model)
		owner, err := store.Acquire(ctx, "test", "one", "owner")
		if err != nil {
			t.Fatal(err)
		}
		if err := schedule.AdvanceMillis(1500); err != nil {
			t.Fatal(err)
		}
		if err := model.QueueFault(KVFault{Operation: "get", Kind: KVStaleRead}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Acquire(ctx, "test", "one", "challenger"); !errors.Is(err, lease.ErrHeld) {
			t.Fatalf("stale contender acquired initialized lease: %v", err)
		}
		entry, err := model.Get(ctx, "test.one")
		if err != nil || entry.Revision <= owner.Epoch() {
			t.Fatalf("initialized lease changed: entry=%+v err=%v", entry, err)
		}
		var value lease.Value
		if err := json.Unmarshal(entry.Value, &value); err != nil || value.Worker != "owner" || value.Epoch != owner.Epoch() {
			t.Fatalf("initialized lease value=%+v err=%v", value, err)
		}
		var stale, fenced bool
		for _, event := range schedule.Trace().Transport {
			stale = stale || event.Operation == "kv_get" && event.Outcome == "stale_read"
			fenced = fenced || event.Operation == "kv_delete" && event.Outcome == "revision_mismatch"
		}
		if !stale || !fenced {
			t.Fatalf("missing stale-read/CAS-rejection evidence: stale=%v fenced=%v", stale, fenced)
		}
	})
	t.Run("old uninitialized lease can be reclaimed", func(t *testing.T) {
		schedule := NewScheduler(1)
		model := NewKVTransport(schedule, 30*time.Second)
		oldValue, _ := json.Marshal(lease.Value{Worker: "crashed"})
		oldRevision, err := model.Create(ctx, "test.one", oldValue)
		if err != nil {
			t.Fatal(err)
		}
		store := lease.NewWithKVPort(model)
		if _, err := store.Acquire(ctx, "test", "one", "survivor"); !errors.Is(err, lease.ErrHeld) {
			t.Fatalf("fresh uninitialized lease: %v", err)
		}
		if err := schedule.AdvanceMillis(1100); err != nil {
			t.Fatal(err)
		}
		acquired, err := store.Acquire(ctx, "test", "one", "survivor")
		if err != nil || acquired.Epoch() <= oldRevision {
			t.Fatalf("reclaimed epoch=%d old=%d err=%v", acquired.Epoch(), oldRevision, err)
		}
		entry, err := model.Get(ctx, "test.one")
		if err != nil {
			t.Fatal(err)
		}
		var value lease.Value
		if err := json.Unmarshal(entry.Value, &value); err != nil || value.Worker != "survivor" || value.Epoch != acquired.Epoch() {
			t.Fatalf("reclaimed value=%+v err=%v", value, err)
		}
	})
	t.Run("expired holder cannot renew or remove successor", func(t *testing.T) {
		schedule := NewScheduler(2)
		model := NewKVTransport(schedule, 30*time.Second)
		store := lease.NewWithKVPort(model)
		old, err := store.Acquire(ctx, "test", "one", "old")
		if err != nil {
			t.Fatal(err)
		}
		if err := schedule.AdvanceMillis(30001); err != nil {
			t.Fatal(err)
		}
		newLease, err := store.Acquire(ctx, "test", "one", "new")
		if err != nil || newLease.Epoch() <= old.Epoch() {
			t.Fatalf("successor epoch=%d old=%d err=%v", newLease.Epoch(), old.Epoch(), err)
		}
		if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("expired renewal: %v", err)
		}
		if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("expired cleanup: %v", err)
		}
		entry, err := model.Get(ctx, "test.one")
		if err != nil {
			t.Fatal(err)
		}
		var value lease.Value
		if err := json.Unmarshal(entry.Value, &value); err != nil || value.Worker != "new" || value.Epoch != newLease.Epoch() {
			t.Fatalf("successor was changed: value=%+v err=%v", value, err)
		}
	})
	t.Run("lost release acknowledgment cleans only old generation", func(t *testing.T) {
		model := NewKVTransport(NewScheduler(3), 30*time.Second)
		store := lease.NewWithKVPort(model)
		old, err := store.Acquire(ctx, "test", "one", "old")
		if err != nil {
			t.Fatal(err)
		}
		if err := model.QueueFault(KVFault{Operation: "delete", Kind: KVLoseAckAfterCommit}); err != nil {
			t.Fatal(err)
		}
		if err := old.Release(ctx); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("lost release acknowledgment: %v", err)
		}
		newLease, err := store.Acquire(ctx, "test", "one", "new")
		if err != nil || newLease.Epoch() <= old.Epoch() {
			t.Fatalf("successor epoch=%d old=%d err=%v", newLease.Epoch(), old.Epoch(), err)
		}
		if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("cleanup removed successor: %v", err)
		}
		entry, err := model.Get(ctx, "test.one")
		if err != nil || entry.Revision == 0 {
			t.Fatalf("successor absent after cleanup: entry=%+v err=%v", entry, err)
		}
	})
	t.Run("lost renewal acknowledgment cleans committed update", func(t *testing.T) {
		model := NewKVTransport(NewScheduler(6), 30*time.Second)
		store := lease.NewWithKVPort(model)
		old, err := store.Acquire(ctx, "test", "one", "old")
		if err != nil {
			t.Fatal(err)
		}
		before, err := model.Get(ctx, "test.one")
		if err != nil {
			t.Fatal(err)
		}
		if err := model.QueueFault(KVFault{Operation: "update", Kind: KVLoseAckAfterCommit}); err != nil {
			t.Fatal(err)
		}
		if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("lost renewal acknowledgment: %v", err)
		}
		committed, err := model.Get(ctx, "test.one")
		if err != nil || committed.Revision <= before.Revision {
			t.Fatalf("renewal did not commit: entry=%+v err=%v", committed, err)
		}
		if err := old.Cleanup(ctx); err != nil {
			t.Fatalf("cleanup after uncertain renewal: %v", err)
		}
		next, err := store.Acquire(ctx, "test", "one", "next")
		if err != nil || next.Epoch() <= old.Epoch() {
			t.Fatalf("successor acquisition: lease=%+v err=%v", next, err)
		}
		if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("stale cleanup: %v", err)
		}
	})
	t.Run("create acknowledgment lost before initialization", func(t *testing.T) {
		schedule := NewScheduler(4)
		model := NewKVTransport(schedule, 30*time.Second)
		if err := model.QueueFault(KVFault{Operation: "create", Kind: KVLoseAckAfterCommit}); err != nil {
			t.Fatal(err)
		}
		store := lease.NewWithKVPort(model)
		if _, err := store.Acquire(ctx, "test", "one", "old"); !errors.Is(err, ErrTransportLost) {
			t.Fatalf("lost create acknowledgment: %v", err)
		}
		if _, err := store.Acquire(ctx, "test", "one", "new"); !errors.Is(err, lease.ErrHeld) {
			t.Fatalf("fresh uncertain create should remain held: %v", err)
		}
		if err := schedule.AdvanceMillis(1100); err != nil {
			t.Fatal(err)
		}
		newLease, err := store.Acquire(ctx, "test", "one", "new")
		if err != nil || newLease.Epoch() <= 1 {
			t.Fatalf("reclaim after lost create acknowledgment: epoch=%d err=%v", newLease.Epoch(), err)
		}
	})
}

func runSeededLeaseScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("lease_100"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewKVTransport(schedule, 30*time.Second)
	store := lease.NewWithKVPort(model)
	for i := 0; i < 100; i++ {
		choice, err := schedule.Choose([]string{"release", "expire", "lost_release_ack", "orphan", "stale_predecessor"})
		if err != nil {
			return Trace{}, err
		}
		id := fmt.Sprintf("case-%03d", i)
		var oldEpoch uint64
		if choice == "orphan" {
			value, _ := json.Marshal(lease.Value{Worker: "crashed"})
			oldEpoch, err = model.Create(context.Background(), "test."+id, value)
			if err != nil {
				return Trace{}, err
			}
			if err := schedule.AdvanceMillis(1100); err != nil {
				return Trace{}, err
			}
		} else {
			old, err := store.Acquire(context.Background(), "test", id, "old")
			if err != nil {
				return Trace{}, err
			}
			oldEpoch = old.Epoch()
			switch choice {
			case "release":
				if err := old.Release(context.Background()); err != nil {
					return Trace{}, err
				}
			case "expire":
				if err := schedule.AdvanceMillis(30001); err != nil {
					return Trace{}, err
				}
			case "lost_release_ack":
				if err := model.QueueFault(KVFault{Operation: "delete", Kind: KVLoseAckAfterCommit}); err != nil {
					return Trace{}, err
				}
				if err := old.Release(context.Background()); !errors.Is(err, lease.ErrLost) {
					return Trace{}, fmt.Errorf("lost release ack: %v", err)
				}
			case "stale_predecessor":
				if err := schedule.AdvanceMillis(1100); err != nil {
					return Trace{}, err
				}
				if err := model.QueueFault(KVFault{Operation: "get", Kind: KVStaleRead}); err != nil {
					return Trace{}, err
				}
				if _, err := store.Acquire(context.Background(), "test", id, "stale"); !errors.Is(err, lease.ErrHeld) {
					return Trace{}, fmt.Errorf("stale predecessor bypassed revision fence: %v", err)
				}
				if err := old.Release(context.Background()); err != nil {
					return Trace{}, err
				}
			}
		}
		next, err := store.Acquire(context.Background(), "test", id, "next")
		if err != nil {
			return Trace{}, fmt.Errorf("seed %d case %d choice=%s acquire: %w", seed, i, choice, err)
		}
		if next.Epoch() <= oldEpoch {
			return Trace{}, fmt.Errorf("seed %d case %d choice=%s next epoch=%d old=%d", seed, i, choice, next.Epoch(), oldEpoch)
		}
		if err := next.Renew(context.Background()); err != nil {
			return Trace{}, err
		}
		entry, err := model.Get(context.Background(), "test."+id)
		if err != nil || entry.Revision <= next.Epoch() {
			return Trace{}, fmt.Errorf("seed %d case %d renewed entry=%+v err=%v", seed, i, entry, err)
		}
	}
	if err := schedule.Finish(); err != nil {
		return Trace{}, err
	}
	return schedule.Trace(), nil
}

func TestSeededLeaseModelReplay(t *testing.T) {
	if os.Getenv("SIM_LEASE_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededLeaseScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_LEASE_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededLeaseScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-lease-sim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-lease.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededLeaseScenario(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d lease trace replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("lease-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededLeaseModelReplay$")
		cmd.Env = append(os.Environ(), "SIM_LEASE_TRACE_HELPER=1", "SIM_LEASE_TRACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("seeded lease child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded lease trace changed across processes")
	}
}

func TestKVRevisionMismatchDoesNotDeleteReplacement(t *testing.T) {
	ctx := context.Background()
	model := NewKVTransport(NewScheduler(5), 30*time.Second)
	first, err := model.Create(ctx, "test.one", []byte(`one`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := model.Update(ctx, "test.one", []byte(`two`), first); err != nil {
		t.Fatal(err)
	}
	if err := model.Delete(ctx, "test.one", first); !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("stale delete: %v", err)
	}
	entry, err := model.Get(ctx, "test.one")
	if err != nil || string(entry.Value) != "two" {
		t.Fatalf("replacement missing: entry=%+v err=%v", entry, err)
	}
}

func runTwoAcquirerRace(seed int64, orphan bool, replay *Trace) (trace Trace, runErr error) {
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
	workload := "lease_two_acquirer_fresh"
	if orphan {
		workload = "lease_two_acquirer_orphan"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewKVTransport(schedule, 30*time.Second)
	if orphan {
		value, _ := json.Marshal(lease.Value{Worker: "crashed"})
		if _, err := model.Create(context.Background(), "test.race", value); err != nil {
			return Trace{}, err
		}
		if err := schedule.AdvanceMillis(1100); err != nil {
			return Trace{}, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	actors := make([]LeaseActor, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		name := name
		actors = append(actors, LeaseActor{Name: name, Run: func(ctx context.Context, port lease.KVPort) error {
			_, err := lease.NewWithKVPort(port).Acquire(ctx, "test", "race", name)
			return err
		}})
	}
	results, err := RunLeaseActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	successes, conflicts := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, lease.ErrHeld), errors.Is(err, lease.ErrLost):
			conflicts++
		default:
			return trace, fmt.Errorf("unexpected acquire result: %v", err)
		}
	}
	entry, err := model.Get(ctx, "test.race")
	if err != nil {
		return trace, err
	}
	var retained lease.Value
	if err := json.Unmarshal(entry.Value, &retained); err != nil {
		return trace, err
	}
	winnerErr, known := results[retained.Worker]
	if successes != 1 || conflicts != 1 || !known || winnerErr != nil || retained.Epoch == 0 || entry.Revision <= retained.Epoch {
		return trace, fmt.Errorf("winner=%+v revision=%d results=%v", retained, entry.Revision, results)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeLeaseAcquireRacesReplay(t *testing.T) {
	for seed := range seededSchedules(t) {
		for _, orphan := range []bool{false, true} {
			generated, err := runTwoAcquirerRace(seed, orphan, nil)
			if err != nil {
				t.Fatalf("FAULT_SEED=%d orphan=%v: %v", seed, orphan, err)
			}
			replayed, err := runTwoAcquirerRace(seed, orphan, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d orphan=%v lease race replay: %v", seed, orphan, err)
			}
		}
	}
}
