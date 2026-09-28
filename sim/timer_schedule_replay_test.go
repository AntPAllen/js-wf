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
	"js-wf/provision"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func runSeededTimerSchedule(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("timer_schedule_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewTimerScheduleTransport(schedule, base)
	var nativeCount, fallbackCount int
	for i := 0; i < 20; i++ {
		backend, err := schedule.Choose([]string{"native", "fallback"})
		if err != nil {
			return trace, err
		}
		mode, err := schedule.Choose([]string{"normal", "duplicate", "lost_ack", "drop"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("scheduled-%02d", i)
		native := backend == "native"
		if native {
			nativeCount++
		} else {
			fallbackCount++
		}
		if mode == "lost_ack" || mode == "drop" {
			fault := LoseAckAfterCommit
			if mode == "drop" {
				fault = DropBeforeCommit
			}
			if err := model.QueueFault(fault); err != nil {
				return trace, err
			}
		}
		call := func() (bool, error) {
			return worker.ScheduleTimerWithPort(ctx, model, native, "test", id, uint64(i+1), 3, base.Add(time.Second))
		}
		newPublish, err := call()
		if mode == "lost_ack" || mode == "drop" {
			if !errors.Is(err, ErrTransportLost) || newPublish {
				return trace, fmt.Errorf("seed %d case %d fault=%s first new=%t err=%v", seed, i, mode, newPublish, err)
			}
			newPublish, err = call()
			wantNew := mode == "drop"
			if err != nil || newPublish != wantNew {
				return trace, fmt.Errorf("seed %d case %d retry=%s new=%t want=%t err=%v", seed, i, mode, newPublish, wantNew, err)
			}
		} else {
			if err != nil || !newPublish {
				return trace, fmt.Errorf("seed %d case %d first=%s new=%t err=%v", seed, i, mode, newPublish, err)
			}
			if mode == "duplicate" {
				if again, err := call(); err != nil || again {
					return trace, fmt.Errorf("seed %d case %d duplicate new=%t err=%v", seed, i, again, err)
				}
			}
		}
		if len(model.NativeSources()) != nativeCount || len(model.FallbackRecords()) != fallbackCount {
			return trace, fmt.Errorf("seed %d case %d retained native=%d/%d fallback=%d/%d", seed, i, len(model.NativeSources()), nativeCount, len(model.FallbackRecords()), fallbackCount)
		}
	}
	if len(model.Runs()) != 0 {
		return trace, fmt.Errorf("seed %d native timers fired before due time", seed)
	}
	if err := model.Advance(500 * time.Millisecond); err != nil {
		return trace, err
	}
	if len(model.Runs()) != 0 {
		return trace, fmt.Errorf("seed %d native timers fired early", seed)
	}
	if err := model.Advance(700 * time.Millisecond); err != nil {
		return trace, err
	}
	if len(model.Runs()) != nativeCount {
		return trace, fmt.Errorf("seed %d delivered=%d native=%d", seed, len(model.Runs()), nativeCount)
	}
	for _, source := range model.NativeSources() {
		target := source.Header.Get(jetstream.ScheduleTargetHeader)
		if target == "" || source.Header.Get(jetstream.ScheduleHeader) == "" || source.Header.Get(identity.TimerInvSeqHeader) == "" || source.Header.Get(identity.TimerStepHeader) != "3" {
			return trace, fmt.Errorf("seed %d malformed native source: %+v", seed, source)
		}
	}
	for _, run := range model.Runs() {
		parts := string(run.Data)
		if run.Subject != identity.RunSubject("test", parts[len("test."):], provision.Partitions) {
			return trace, fmt.Errorf("seed %d native target=%s data=%s", seed, run.Subject, run.Data)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededTimerScheduleReplay(t *testing.T) {
	if os.Getenv("SIM_TIMER_SCHEDULE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededTimerSchedule(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TIMER_SCHEDULE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededTimerSchedule(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-timer-schedule-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-timer-schedule.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededTimerSchedule(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d timer schedule replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("timer-schedule-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededTimerScheduleReplay$")
		cmd.Env = append(os.Environ(), "SIM_TIMER_SCHEDULE_HELPER=1", "SIM_TIMER_SCHEDULE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("timer schedule child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded timer schedule trace changed across processes")
	}
}
