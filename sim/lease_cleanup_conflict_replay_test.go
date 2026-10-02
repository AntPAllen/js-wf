package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/lease"
	"js-wf/provision"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// Deliver a delayed same-owner update or a successor acquisition exactly
// between cleanup's retained read and revision-CAS delete.
type cleanupConflictPort struct {
	lease.KVPort
	schedule  *Scheduler
	mode      string
	conflicts int
	deletes   int
	successor *lease.Lease
}

func (p *cleanupConflictPort) Delete(ctx context.Context, key string, revision uint64) error {
	p.deletes++
	if p.conflicts > 0 {
		p.conflicts--
		if err := p.schedule.AdvanceMillis(200); err != nil {
			return err
		}
		entry, err := p.KVPort.Get(ctx, key)
		if err != nil {
			return err
		}
		if p.mode == "same_owner" || p.mode == "persistent_same_owner" {
			if _, err := p.KVPort.Update(ctx, key, entry.Value, entry.Revision); err != nil {
				return err
			}
		} else {
			if err := p.KVPort.Delete(ctx, key, entry.Revision); err != nil {
				return err
			}
			worker := "old"
			if p.mode == "foreign_worker" {
				worker = "new"
			}
			p.successor, err = lease.NewWithKVPort(p.KVPort).Acquire(ctx, "test", "cleanup", worker)
			if err != nil {
				return err
			}
		}
	}
	return p.KVPort.Delete(ctx, key, revision)
}

func runSeededCleanupConflict(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("lease_cleanup_conflict"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"same_owner", "same_worker_new_epoch", "foreign_worker", "persistent_same_owner"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	kv := NewKVTransport(schedule, provision.LeaseTTL)
	port := &cleanupConflictPort{KVPort: kv, schedule: schedule, mode: mode}
	owner, err := lease.NewWithKVPort(port).Acquire(ctx, "test", "cleanup", "old")
	if err != nil {
		return trace, err
	}
	if err := kv.QueueFault(KVFault{Operation: "update", Kind: KVLoseAckAfterCommit}); err != nil {
		return trace, err
	}
	if err := owner.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("uncertain renewal=%v", err)
	}
	port.conflicts = 1
	if mode == "persistent_same_owner" {
		port.conflicts = 3
	}
	err = owner.Cleanup(ctx)
	switch mode {
	case "same_owner":
		if err != nil || port.deletes != 2 {
			return trace, fmt.Errorf("same-owner cleanup abandoned: deletes=%d err=%v", port.deletes, err)
		}
	case "persistent_same_owner":
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, lease.ErrLost) || port.deletes != 3 {
			return trace, fmt.Errorf("bounded conflicts misclassified: deletes=%d err=%v", port.deletes, err)
		}
		if _, err := kv.Get(ctx, "test.cleanup"); err != nil {
			return trace, fmt.Errorf("conflicted own lease disappeared: %v", err)
		}
		if err := owner.Cleanup(ctx); err != nil {
			return trace, fmt.Errorf("cleanup retry=%v", err)
		}
	default:
		if !errors.Is(err, lease.ErrLost) || port.deletes != 1 || port.successor == nil || port.successor.Epoch() <= owner.Epoch() {
			return trace, fmt.Errorf("successor cleanup: deletes=%d successor=%v err=%v", port.deletes, port.successor, err)
		}
		before, err := kv.Get(ctx, "test.cleanup")
		if err != nil {
			return trace, err
		}
		var value lease.Value
		if err := json.Unmarshal(before.Value, &value); err != nil || value.Epoch != port.successor.Epoch() {
			return trace, fmt.Errorf("successor state=%+v err=%v", value, err)
		}
		if err := owner.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
			return trace, fmt.Errorf("repeat old cleanup=%v", err)
		}
		after, err := kv.Get(ctx, "test.cleanup")
		if err != nil || after.Revision != before.Revision || !bytes.Equal(after.Value, before.Value) {
			return trace, fmt.Errorf("successor changed: %v", err)
		}
		if err := port.successor.Release(ctx); err != nil {
			return trace, err
		}
	}
	if _, err := kv.Get(ctx, "test.cleanup"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("cleanup left lease: %v", err)
	}
	if err := owner.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("old owner renewed: %v", err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_cleanup_conflict", Outcome: mode, AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}

func TestSeededCleanupConflictReplay(t *testing.T) {
	if os.Getenv("SIM_CLEANUP_CONFLICT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededCleanupConflict(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CLEANUP_CONFLICT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededCleanupConflict(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "cleanup-conflict-failure.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatal(saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededCleanupConflict(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d cleanup-conflict replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("cleanup-conflict-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededCleanupConflictReplay$")
		cmd.Env = append(os.Environ(), "SIM_CLEANUP_CONFLICT_HELPER=1", "SIM_CLEANUP_CONFLICT_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("cleanup-conflict trace changed across processes")
	}
}
