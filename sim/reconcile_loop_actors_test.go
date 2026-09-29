package sim

import (
	"bytes"
	"context"
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

	"github.com/nats-io/nats.go"
)

type stopAfterRunSavePort struct {
	reconcile.LoopPort
	starts *StartTransport
	stop   context.CancelFunc
}

func (p stopAfterRunSavePort) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	saved, err := p.LoopPort.SaveCursor(ctx, kind, next, revision)
	if err == nil && len(p.starts.Runs()) == 10 {
		p.stop()
	}
	return saved, err
}

func runTwoReconcileLoops(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("reconcile_two_scanners"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	starts := NewStartTransport(schedule)
	loop := NewLoopTransport(schedule)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("compete-%02d", i)
		if _, err := starts.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")}); err != nil {
			return trace, err
		}
	}
	choices := make([]string, 10)
	for i := range choices {
		choices[i] = strconv.Itoa(i + 1)
	}
	chosen, err := schedule.Choose(choices)
	if err != nil {
		return trace, err
	}
	at, _ := strconv.Atoi(chosen)
	fault, err := schedule.Choose([]string{string(KVDropBeforeCommit), string(KVLoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	if err := loop.RejectCursorSaveAt(at, KVFaultKind(fault)); err != nil {
		return trace, err
	}
	actors := make([]ReconcileLoopActor, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		name := name
		actors = append(actors, ReconcileLoopActor{Name: name, Run: func(_ context.Context, port reconcile.LoopPort, scanPort reconcile.StartScanPort) error {
			scan := reconcile.NewStartScanWithPort(scanPort)
			return reconcile.RunLoopWithPort(loopCtx,
				stopAfterRunSavePort{LoopPort: port, starts: starts, stop: stopLoops},
				name, "start", 100*time.Millisecond, 1, scan.Scan)
		}})
	}
	results, err := RunReconcileLoopActors(ctx, schedule, loop, starts, actors)
	if err != nil {
		return trace, err
	}
	if results["alpha"] != nil || results["beta"] != nil {
		return trace, fmt.Errorf("seed %d scanner results=%v", seed, results)
	}
	if got := len(starts.Runs()); got != 10 {
		return trace, fmt.Errorf("seed %d retained wakeups=%d want=10", seed, got)
	}
	for i, message := range starts.Runs() {
		id := fmt.Sprintf("compete-%02d", i)
		if string(message.Data) != identity.Key("test", id) {
			return trace, fmt.Errorf("seed %d wakeup %d data=%q", seed, i, message.Data)
		}
	}
	creates := 0
	for _, event := range schedule.Trace().Transport {
		if event.Operation == "kv_create" && event.Subject == "system.start-reconciler" && event.Outcome == "ok" {
			creates++
		}
	}
	if creates < 2 {
		return trace, fmt.Errorf("seed %d scanner lease did not turn over after cursor fault: creates=%d", seed, creates)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeReconcileScannersReplay(t *testing.T) {
	if os.Getenv("SIM_TWO_RECONCILERS_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoReconcileLoops(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TWO_RECONCILERS_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runTwoReconcileLoops(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-two-reconcilers-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-two-reconcilers.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runTwoReconcileLoops(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d two-scanner replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("two-reconcilers-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeReconcileScannersReplay$")
		cmd.Env = append(os.Environ(), "SIM_TWO_RECONCILERS_HELPER=1", "SIM_TWO_RECONCILERS_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("two-scanner child %d: %v: %s", i, err, output)
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
		t.Fatal("two-scanner trace changed across processes")
	}
}
