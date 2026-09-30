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

// Match the expired-key contention contract exercised against three real nodes.
// The model exposes normalized KV errors; the adapter's raw API-error mapping
// is checked separately in lease and in the real-cluster contract test.
func runLeaseExpiryRace(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("lease_expiry_32_acquirers"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	model := NewKVTransport(schedule, 2*time.Second)
	old, err := lease.NewWithKVPort(model).Acquire(ctx, "test", "race", "expired")
	if err != nil {
		return trace, err
	}
	if err := schedule.AdvanceMillis(2001); err != nil {
		return trace, err
	}
	actors := make([]LeaseActor, 32)
	for i := range actors {
		name := fmt.Sprintf("contender-%02d", i)
		actors[i] = LeaseActor{Name: name, Run: func(ctx context.Context, port lease.KVPort) error {
			_, err := lease.NewWithKVPort(port).Acquire(ctx, "test", "race", name)
			return err
		}}
	}
	results, err := RunLeaseActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	successes, held := 0, 0
	for _, actor := range actors {
		result, known := results[actor.Name]
		if !known {
			return trace, fmt.Errorf("missing result for %s", actor.Name)
		}
		switch {
		case result == nil:
			successes++
		case errors.Is(result, lease.ErrHeld):
			held++
		default:
			return trace, fmt.Errorf("%s acquire: %w", actor.Name, result)
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
	if successes != 1 || held != 31 || !known || winnerErr != nil || retained.Epoch <= old.Epoch() || entry.Revision <= retained.Epoch {
		return trace, fmt.Errorf("winner=%+v revision=%d successes=%d held=%d", retained, entry.Revision, successes, held)
	}
	if err := old.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("expired holder renew: %v", err)
	}
	if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		return trace, fmt.Errorf("expired holder cleanup: %v", err)
	}
	after, err := model.Get(ctx, "test.race")
	if err != nil || after.Revision != entry.Revision || !bytes.Equal(after.Value, entry.Value) {
		return trace, fmt.Errorf("expired holder changed successor: before=%+v after=%+v err=%v", entry, after, err)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededLeaseExpiryRaceReplay(t *testing.T) {
	if os.Getenv("SIM_LEASE_EXPIRY_RACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runLeaseExpiryRace(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_LEASE_EXPIRY_RACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runLeaseExpiryRace(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "lease-expiry-race-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runLeaseExpiryRace(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("lease-expiry-race-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededLeaseExpiryRaceReplay$")
		cmd.Env = append(os.Environ(), "SIM_LEASE_EXPIRY_RACE_HELPER=1", "SIM_LEASE_EXPIRY_RACE_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("lease expiry race trace changed across processes")
	}
}
