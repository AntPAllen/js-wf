package sim

import (
	"bytes"
	"context"
	"encoding/json"
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
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func runSeededTimerScan(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("timer_scan_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewSignalTransport(schedule)
	scanner := reconcile.NewTimerScanWithPort(model)
	base := time.Unix(1_700_000_000, 0).UTC()
	scanner.Now = func() time.Time { return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond) }
	var observationErr error
	var events []reconcile.RepairEvent
	scanner.Observe = func(e reconcile.RepairEvent) { events = append(events, e) }
	scan := func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
		before := len(events)
		result, err := scanner.Scan(ctx, next, budget, dry)
		if len(events)-before != result.Reenqueued {
			observationErr = fmt.Errorf("timer event count disagrees with repair decisions")
			return result, err
		}
		for _, e := range events[before:] {
			want := "acknowledged"
			if dry {
				want = "dry_run"
			} else if err != nil {
				want = "uncertain"
			}
			if e.Kind != "timer" || e.Outcome != want || e.Type != "test" || e.SourceSequence != next || e.InvocationSequence != next || e.JournalSequence != next*10+2 || e.FireAt == nil || e.FireAt.IsZero() || e.FireAt.After(scanner.Now()) || e.At.IsZero() || (e.Error != "") != (want == "uncertain") {
				observationErr = fmt.Errorf("invalid timer repair evidence: %+v", e)
				return result, err
			}
		}
		return result, err
	}
	var future []uint64
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"timer", "timer_start", "timer_await", "future", "completed", "terminal", "no_request", "other_kind", "hole", "lost_ack", "drop_enqueue"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("timer-%02d", i)
		sequence, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
		if err != nil {
			return trace, err
		}
		if mode == "hole" {
			model.PurgeInvocation(identity.InvocationSubject("test", id))
		} else if mode != "no_request" {
			kind := "timer"
			switch mode {
			case "timer_start", "timer_await":
				kind = mode
			case "other_kind":
				kind = "effect"
			}
			fireAt := base.Add(-time.Second)
			if mode == "future" {
				fireAt = base.Add(time.Second)
				future = append(future, sequence)
			}
			payload, err := json.Marshal(struct {
				Kind   string    `json:"kind"`
				FireAt time.Time `json:"fire_at"`
			}{kind, fireAt})
			if err != nil {
				return trace, err
			}
			records := []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started, Index: 0}, Sequence: sequence*10 + 1},
				{Entry: journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: payload}, Sequence: sequence*10 + 2},
			}
			if mode == "completed" {
				records = append(records, journal.Record{Entry: journal.Entry{Kind: journal.StepCompleted, Index: 2}, Sequence: sequence*10 + 3})
			}
			if mode == "terminal" {
				records = append(records, journal.Record{Entry: journal.Entry{Kind: journal.Completed, Index: 2}, Sequence: sequence*10 + 3})
			}
			model.SetJournal("test", id, records)
		}
		due := mode == "timer" || mode == "timer_start" || mode == "timer_await" || mode == "lost_ack" || mode == "drop_enqueue"
		before := len(model.Runs())
		if due {
			result, err := scan(ctx, sequence, 1, true)
			if err != nil || result.Reenqueued != 1 || len(model.Runs()) != before {
				return trace, fmt.Errorf("seed %d case %d dry run mode=%s result=%+v runs=%d/%d err=%v", seed, i, mode, result, len(model.Runs()), before, err)
			}
		}
		if mode == "lost_ack" || mode == "drop_enqueue" {
			fault := "lose_ack_after_commit"
			if mode == "drop_enqueue" {
				fault = "drop_before_commit"
			}
			if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: fault}); err != nil {
				return trace, err
			}
			result, err := scan(ctx, sequence, 1, false)
			if err == nil || result.Reenqueued != 1 {
				return trace, fmt.Errorf("seed %d case %d uncertain enqueue result=%+v err=%v", seed, i, result, err)
			}
			if got := len(model.Runs()); got != before+map[bool]int{true: 1, false: 0}[mode == "lost_ack"] {
				return trace, fmt.Errorf("seed %d case %d fault=%s retained runs=%d", seed, i, mode, got)
			}
		}
		result, err := scan(ctx, sequence, 1, false)
		if err != nil || result.NextSequence != sequence+1 || result.Reenqueued != map[bool]int{true: 1, false: 0}[due] {
			return trace, fmt.Errorf("seed %d case %d mode=%s result=%+v err=%v", seed, i, mode, result, err)
		}
		wantRuns := before
		if due {
			wantRuns++
		}
		if got := len(model.Runs()); got != wantRuns {
			return trace, fmt.Errorf("seed %d case %d mode=%s runs=%d want=%d", seed, i, mode, got, wantRuns)
		}
		if due {
			if _, err := scan(ctx, sequence, 1, false); err != nil || len(model.Runs()) != wantRuns {
				return trace, fmt.Errorf("seed %d case %d dedup mode=%s runs=%d err=%v", seed, i, mode, len(model.Runs()), err)
			}
		}
	}
	if err := model.Wait(ctx, 2*time.Second); err != nil {
		return trace, err
	}
	for _, sequence := range future {
		before := len(model.Runs())
		result, err := scan(ctx, sequence, 1, false)
		if err != nil || result.Reenqueued != 1 || len(model.Runs()) != before+1 {
			return trace, fmt.Errorf("seed %d future %d result=%+v runs=%d/%d err=%v", seed, sequence, result, len(model.Runs()), before, err)
		}
	}
	if observationErr != nil {
		return trace, observationErr
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededTimerScanReplay(t *testing.T) {
	if os.Getenv("SIM_TIMER_SCAN_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededTimerScan(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TIMER_SCAN_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededTimerScan(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-timer-scan-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-timer-scan.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededTimerScan(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d timer scan replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("timer-scan-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededTimerScanReplay$")
		cmd.Env = append(os.Environ(), "SIM_TIMER_SCAN_HELPER=1", "SIM_TIMER_SCAN_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("timer scan child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded timer scan trace changed across processes")
	}
}
