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

func runSeededReconcileLoop(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("reconcile_loop_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	mode, err := schedule.Choose([]string{"start", "timer"})
	if err != nil {
		return trace, err
	}
	model := NewSignalTransport(schedule)
	starts := model.StartTransport
	loop := NewLoopTransport(schedule)
	scan := reconcile.NewStartScanWithPort(starts).Scan
	if mode == "timer" {
		timerScan := reconcile.NewTimerScanWithPort(model)
		base := time.Unix(1_700_000_000, 0).UTC()
		timerScan.Now = func() time.Time { return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond) }
		scan = timerScan.Scan
	}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("loop-%02d", i)
		sequence, err := starts.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
		if err != nil {
			return trace, err
		}
		if mode == "timer" {
			payload, err := json.Marshal(struct {
				Kind   string    `json:"kind"`
				FireAt time.Time `json:"fire_at"`
			}{"timer", time.Unix(1_700_000_000, 0).Add(-time.Second)})
			if err != nil {
				return trace, err
			}
			model.SetJournal("test", id, []journal.Record{
				{Entry: journal.Entry{Kind: journal.Started, Index: 0}, Sequence: sequence*10 + 1},
				{Entry: journal.Entry{Kind: journal.StepRequested, Index: 1, Payload: payload}, Sequence: sequence*10 + 2},
			})
		}
	}
	choices := make([]string, 6)
	for i := range choices {
		choices[i] = strconv.Itoa(i + 1)
	}
	chosen, err := schedule.Choose(choices)
	if err != nil {
		return trace, err
	}
	at, _ := strconv.Atoi(chosen)
	kindChoice, err := schedule.Choose([]string{string(KVDropBeforeCommit), string(KVLoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	if err := loop.RejectCursorSaveAt(at, KVFaultKind(kindChoice)); err != nil {
		return trace, err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	loop.StopAfterWaits(10, stopFirst)
	if err := reconcile.RunLoopWithPort(firstCtx, loop, "first", mode, 100*time.Millisecond, 1, scan); err != nil {
		return trace, fmt.Errorf("seed %d first loop: %w", seed, err)
	}
	if loop.saves < at {
		return trace, fmt.Errorf("seed %d first scanner missed cursor fault at save %d after %d attempts", seed, at, loop.saves)
	}
	cursor, revision, err := loop.LoadCursor(ctx, mode)
	if err != nil || revision == 0 || cursor < 1 || cursor > 11 || len(starts.Runs()) < 9 || len(starts.Runs()) > 10 {
		return trace, fmt.Errorf("seed %d first cursor=%d revision=%d runs=%d err=%v", seed, cursor, revision, len(starts.Runs()), err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	loop.StopAfterWaits(25, stopSecond)
	if err := reconcile.RunLoopWithPort(secondCtx, loop, "replacement", mode, 100*time.Millisecond, 1, scan); err != nil {
		return trace, fmt.Errorf("seed %d replacement loop: %w", seed, err)
	}
	if got := len(starts.Runs()); got != 20 {
		return trace, fmt.Errorf("seed %d repaired runs=%d want=20", seed, got)
	}
	for i, message := range starts.Runs() {
		id := fmt.Sprintf("loop-%02d", i)
		if string(message.Data) != identity.Key("test", id) {
			return trace, fmt.Errorf("seed %d run %d data=%q want=%s", seed, i, message.Data, id)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededReconcileLoopReplay(t *testing.T) {
	if os.Getenv("SIM_RECONCILE_LOOP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededReconcileLoop(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_RECONCILE_LOOP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededReconcileLoop(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-reconcile-loop-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-reconcile-loop.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededReconcileLoop(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d reconcile loop replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("reconcile-loop-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededReconcileLoopReplay$")
		cmd.Env = append(os.Environ(), "SIM_RECONCILE_LOOP_HELPER=1", "SIM_RECONCILE_LOOP_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("reconcile loop child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded reconcile loop trace changed across processes")
	}
}
