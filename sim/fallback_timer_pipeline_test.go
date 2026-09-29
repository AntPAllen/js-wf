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
	"js-wf/reconcile"
	"js-wf/retention"
	"js-wf/worker"
)

func runSeededFallbackPipeline(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("fallback_timer_pipeline_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewTimerScheduleTransport(schedule, base)
	scan := reconcile.NewFallbackTimerScanWithPort(model, func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
	})
	var dueCount, futureCount int
	var dueIDs, futureIDs []string
	for i := 0; i < 20; i++ {
		mode := "due"
		if i > 0 {
			var err error
			mode, err = schedule.Choose([]string{"due", "future", "purging", "tombstone"})
			if err != nil {
				return trace, err
			}
		}
		id := fmt.Sprintf("fallback-%02d", i)
		fireAt := base.Add(-time.Second)
		if mode == "future" {
			fireAt = base.Add(time.Hour)
			futureCount++
			futureIDs = append(futureIDs, id)
		}
		if mode == "due" {
			dueCount++
			dueIDs = append(dueIDs, id)
		}
		if mode == "purging" {
			model.SetState("purging."+identity.Key("test", id), []byte("7"))
		}
		if mode == "tombstone" {
			data, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 7, PurgedAt: base, ExpiresAt: base.Add(24 * time.Hour)})
			if err != nil {
				return trace, err
			}
			model.SetState(identity.Key("test", id), data)
		}
		if newPublish, err := worker.ScheduleTimerWithPort(ctx, model, false, "test", id, 7, 3, fireAt); err != nil || !newPublish {
			return trace, fmt.Errorf("seed %d timer %d mode=%s new=%t err=%v", seed, i, mode, newPublish, err)
		}
	}
	wakeupFault, err := schedule.Choose([]string{string(DropBeforeCommit), string(LoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	deleteFault, err := schedule.Choose([]string{string(DropBeforeCommit), string(LoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	if err := model.QueueWakeupFault(AppendFault(wakeupFault)); err != nil {
		return trace, err
	}
	if err := model.QueueDeleteFault(AppendFault(deleteFault)); err != nil {
		return trace, err
	}
	if result, err := scan.Scan(ctx, 1, 20, false); !errors.Is(err, ErrTransportLost) || result.Reenqueued != 1 {
		return trace, fmt.Errorf("seed %d wakeup fault result=%+v err=%v", seed, result, err)
	}
	if result, err := scan.Scan(ctx, 1, 20, false); !errors.Is(err, ErrTransportLost) || result.Reenqueued != 1 {
		return trace, fmt.Errorf("seed %d delete fault result=%+v err=%v", seed, result, err)
	}
	if _, err := scan.Scan(ctx, 1, 20, false); err != nil {
		return trace, fmt.Errorf("seed %d retry: %w", seed, err)
	}
	if len(model.Runs()) != dueCount || len(model.RetainedFallbackRecords()) != futureCount {
		return trace, fmt.Errorf("seed %d after repair runs=%d/%d timers=%d/%d", seed, len(model.Runs()), dueCount, len(model.RetainedFallbackRecords()), futureCount)
	}
	for i, run := range model.Runs() {
		if string(run.Data) != identity.Key("test", dueIDs[i]) {
			return trace, fmt.Errorf("seed %d due wakeup %d data=%s", seed, i, run.Data)
		}
	}
	if err := model.Advance(2 * time.Hour); err != nil {
		return trace, err
	}
	if _, err := scan.Scan(ctx, 1, 20, false); err != nil {
		return trace, fmt.Errorf("seed %d future scan: %w", seed, err)
	}
	if len(model.Runs()) != dueCount+futureCount || len(model.RetainedFallbackRecords()) != 0 {
		return trace, fmt.Errorf("seed %d final runs=%d/%d retained=%d", seed, len(model.Runs()), dueCount+futureCount, len(model.RetainedFallbackRecords()))
	}
	for i, id := range futureIDs {
		if string(model.Runs()[dueCount+i].Data) != identity.Key("test", id) {
			return trace, fmt.Errorf("seed %d future wakeup %d=%s", seed, i, model.Runs()[dueCount+i].Data)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededFallbackTimerPipelineReplay(t *testing.T) {
	if os.Getenv("SIM_FALLBACK_PIPELINE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededFallbackPipeline(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_FALLBACK_PIPELINE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededFallbackPipeline(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-fallback-pipeline-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-fallback-pipeline.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededFallbackPipeline(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d fallback pipeline replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("fallback-pipeline-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededFallbackTimerPipelineReplay$")
		cmd.Env = append(os.Environ(), "SIM_FALLBACK_PIPELINE_HELPER=1", "SIM_FALLBACK_PIPELINE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fallback pipeline child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded fallback pipeline trace changed across processes")
	}
}
