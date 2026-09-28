package sim

import (
	"bytes"
	"context"
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
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

type timeoutFallbackPort struct {
	reconcile.FallbackTimerScanPort
}

func (p timeoutFallbackPort) PublishWakeup(ctx context.Context, message *nats.Msg, messageID string) error {
	err := p.FallbackTimerScanPort.PublishWakeup(ctx, message, messageID)
	if errors.Is(err, ErrTransportLost) {
		return nats.ErrTimeout
	}
	return err
}

func (p timeoutFallbackPort) DeleteTimer(ctx context.Context, sequence uint64) error {
	err := p.FallbackTimerScanPort.DeleteTimer(ctx, sequence)
	if errors.Is(err, ErrTransportLost) {
		return nats.ErrTimeout
	}
	return err
}

func runSeededFallbackLoop(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("fallback_reconcile_loop_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewTimerScheduleTransport(schedule, base)
	loop := NewLoopTransport(schedule)
	scan := reconcile.NewFallbackTimerScanWithPort(timeoutFallbackPort{model}, func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond), nil
	})
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("loop-timer-%02d", i)
		if newPublish, err := worker.ScheduleTimerWithPort(ctx, model, false, "test", id, 7, 3, base.Add(-time.Second)); err != nil || !newPublish {
			return trace, fmt.Errorf("seed %d timer %d new=%t err=%v", seed, i, newPublish, err)
		}
	}
	choices := make([]string, 6)
	for i := range choices {
		choices[i] = strconv.Itoa(i + 1)
	}
	selected, err := schedule.Choose(choices)
	if err != nil {
		return trace, err
	}
	at, _ := strconv.Atoi(selected)
	faultChoice, err := schedule.Choose([]string{string(KVDropBeforeCommit), string(KVLoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	if err := loop.RejectCursorSaveAt(at, KVFaultKind(faultChoice)); err != nil {
		return trace, err
	}
	wakeupChoice, err := schedule.Choose([]string{string(DropBeforeCommit), string(LoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	deleteChoice, err := schedule.Choose([]string{string(DropBeforeCommit), string(LoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	if err := model.QueueWakeupFault(AppendFault(wakeupChoice)); err != nil {
		return trace, err
	}
	if err := model.QueueDeleteFault(AppendFault(deleteChoice)); err != nil {
		return trace, err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	loop.StopAfterWaits(10, stopFirst)
	if err := reconcile.RunLoopWithPort(firstCtx, loop, "first", "fallback-timer", 100*time.Millisecond, 1, scan.Scan); err != nil {
		return trace, fmt.Errorf("seed %d first loop: %w", seed, err)
	}
	if loop.saves < at {
		return trace, fmt.Errorf("seed %d first scanner missed cursor fault at save %d after %d attempts", seed, at, loop.saves)
	}
	cursor, revision, err := loop.LoadCursor(ctx, "fallback-timer")
	if err != nil || revision == 0 || cursor < 1 || cursor > 11 || len(model.Runs()) < 7 || len(model.Runs()) > 10 {
		return trace, fmt.Errorf("seed %d first cursor=%d revision=%d runs=%d err=%v", seed, cursor, revision, len(model.Runs()), err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	loop.StopAfterWaits(25, stopSecond)
	if err := reconcile.RunLoopWithPort(secondCtx, loop, "replacement", "fallback-timer", 100*time.Millisecond, 1, scan.Scan); err != nil {
		return trace, fmt.Errorf("seed %d replacement loop: %w", seed, err)
	}
	if len(model.Runs()) != 20 || len(model.RetainedFallbackRecords()) != 0 {
		return trace, fmt.Errorf("seed %d final runs=%d retained=%d", seed, len(model.Runs()), len(model.RetainedFallbackRecords()))
	}
	for i, run := range model.Runs() {
		id := fmt.Sprintf("loop-timer-%02d", i)
		if string(run.Data) != identity.Key("test", id) {
			return trace, fmt.Errorf("seed %d run %d data=%s", seed, i, run.Data)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededFallbackLoopReplay(t *testing.T) {
	if os.Getenv("SIM_FALLBACK_LOOP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededFallbackLoop(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_FALLBACK_LOOP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededFallbackLoop(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-fallback-loop-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-fallback-loop.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededFallbackLoop(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d fallback loop replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("fallback-loop-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededFallbackLoopReplay$")
		cmd.Env = append(os.Environ(), "SIM_FALLBACK_LOOP_HELPER=1", "SIM_FALLBACK_LOOP_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fallback loop child %d: %v: %s", i, err, output)
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
		t.Fatal("fallback loop trace changed across processes")
	}
}
