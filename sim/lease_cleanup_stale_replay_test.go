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

// Preserve the pre-initialization value as a stale read after acquisition.
// Production cleanup must not mistake its epoch zero for a successor.
func runSeededCleanupStaleRead(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("lease_cleanup_stale_read"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"one_stale_read", "three_stale_reads"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	kv := NewKVTransport(schedule, provision.LeaseTTL)
	owner, err := lease.NewWithKVPort(kv).Acquire(ctx, "test", "cleanup", "old")
	if err != nil {
		return trace, err
	}
	// A dropped renewal fences execution while leaving the acknowledged epoch
	// update current and the earlier initialization value in previous state.
	if err := kv.QueueFault(KVFault{Operation: "update", Kind: KVDropBeforeCommit}); err != nil {
		return trace, err
	}
	if err := owner.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("uncertain renewal=%v", err)
	}
	stale := 1
	if mode == "three_stale_reads" {
		stale = 3
	}
	for range stale {
		if err := kv.QueueFault(KVFault{Operation: "get", Kind: KVStaleRead}); err != nil {
			return trace, err
		}
	}
	err = owner.Cleanup(ctx)
	if stale == 1 {
		if err != nil {
			return trace, fmt.Errorf("stale initialization misclassified as successor: %v", err)
		}
	} else {
		if !errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, lease.ErrLost) {
			return trace, fmt.Errorf("persistent stale reads misclassified: %v", err)
		}
		before, err := kv.Get(ctx, "test.cleanup")
		if err != nil {
			return trace, err
		}
		var value lease.Value
		if err := json.Unmarshal(before.Value, &value); err != nil || value.Epoch != owner.Epoch() {
			return trace, fmt.Errorf("own lease changed: %+v %v", value, err)
		}
		if err := owner.Cleanup(ctx); err != nil {
			return trace, err
		}
	}
	if _, err := kv.Get(ctx, "test.cleanup"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("cleanup left lease: %v", err)
	}
	if err := owner.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("old owner renewed: %v", err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_cleanup_stale_read", Outcome: mode, AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}

func TestSeededCleanupStaleReadReplay(t *testing.T) {
	if os.Getenv("SIM_CLEANUP_STALE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededCleanupStaleRead(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CLEANUP_STALE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededCleanupStaleRead(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "cleanup-stale-read-failure.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatal(saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededCleanupStaleRead(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d cleanup-stale-read replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("cleanup-stale-read-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededCleanupStaleReadReplay$")
		cmd.Env = append(os.Environ(), "SIM_CLEANUP_STALE_HELPER=1", "SIM_CLEANUP_STALE_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("cleanup-stale-read trace changed across processes")
	}
}
