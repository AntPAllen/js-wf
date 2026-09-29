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
)

func runSkewedLeaseAcquirers(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("lease_clock_skew"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"before_fast_threshold", "after_fast_threshold"})
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	model := NewKVTransport(schedule, 30*time.Second)
	oldValue, _ := json.Marshal(lease.Value{Worker: "crashed"})
	oldRevision, err := model.Create(ctx, "test.skew", oldValue)
	if err != nil {
		return trace, err
	}
	advance := int64(500)
	if mode == "after_fast_threshold" {
		advance = 800
	}
	if err := schedule.AdvanceMillis(advance); err != nil {
		return trace, err
	}
	actors := []LeaseActor{
		{Name: "slow", WallClockOffset: -400 * time.Millisecond, Run: func(ctx context.Context, port lease.KVPort) error {
			_, err := lease.NewWithKVPort(port).Acquire(ctx, "test", "skew", "slow")
			return err
		}},
		{Name: "fast", WallClockOffset: 400 * time.Millisecond, Run: func(ctx context.Context, port lease.KVPort) error {
			_, err := lease.NewWithKVPort(port).Acquire(ctx, "test", "skew", "fast")
			return err
		}},
	}
	results, err := RunLeaseActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	entry, err := model.Get(ctx, "test.skew")
	if err != nil {
		return trace, err
	}
	var retained lease.Value
	if err := json.Unmarshal(entry.Value, &retained); err != nil {
		return trace, err
	}
	var deletes int
	for _, event := range schedule.Trace().Transport {
		if event.Operation == "kv_delete" && event.Subject == "test.skew" && event.Outcome == "ok" {
			deletes++
		}
	}
	if mode == "before_fast_threshold" {
		if !errors.Is(results["slow"], lease.ErrHeld) || !errors.Is(results["fast"], lease.ErrHeld) || retained.Worker != "crashed" || retained.Epoch != 0 || entry.Revision != oldRevision || deletes != 0 {
			return trace, fmt.Errorf("seed %d premature reclaim: results=%v retained=%+v revision=%d deletes=%d", seed, results, retained, entry.Revision, deletes)
		}
	} else {
		var success int
		for _, result := range results {
			if result == nil {
				success++
			} else if !errors.Is(result, lease.ErrHeld) && !errors.Is(result, lease.ErrLost) {
				return trace, fmt.Errorf("seed %d unexpected acquisition: %v", seed, result)
			}
		}
		winnerResult, known := results[retained.Worker]
		if success != 1 || !known || winnerResult != nil || retained.Epoch <= oldRevision || entry.Revision <= retained.Epoch || deletes != 1 {
			return trace, fmt.Errorf("seed %d skewed reclaim: results=%v retained=%+v revision=%d deletes=%d", seed, results, retained, entry.Revision, deletes)
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_lease_clock_skew", Subject: "test.skew", Sequence: entry.Revision, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededLeaseClockSkewReplay(t *testing.T) {
	if os.Getenv("SIM_LEASE_CLOCK_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSkewedLeaseAcquirers(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_LEASE_CLOCK_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSkewedLeaseAcquirers(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "lease-clock-skew.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSkewedLeaseAcquirers(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d skewed lease replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("lease-clock-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededLeaseClockSkewReplay$")
		cmd.Env = append(os.Environ(), "SIM_LEASE_CLOCK_HELPER=1", "SIM_LEASE_CLOCK_OUT="+files[i], "FAULT_SEED=1")
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
		t.Fatal("lease clock-skew trace changed across processes")
	}
}
