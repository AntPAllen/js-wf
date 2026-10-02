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

type stopAfterSuspendedSavePort struct {
	reconcile.LoopPort
	model *SignalTransport
	stop  context.CancelFunc
}

func (p stopAfterSuspendedSavePort) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	saved, err := p.LoopPort.SaveCursor(ctx, kind, next, revision)
	if err == nil && len(p.model.Runs()) == 10 {
		p.stop()
	}
	return saved, err
}

func runTwoSuspendedLoops(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("suspended_two_scanners"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewSignalTransport(schedule)
	loop := NewLoopTransport(schedule)
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("compete-suspended-%02d", i)
		sequence, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
		if err != nil {
			return trace, err
		}
		mode := "due_timer"
		var signalSeq uint64
		if i%2 == 1 {
			mode = "matching_signal"
			message := &nats.Msg{Subject: "wf.sig.test." + id + ".go", Data: []byte("payload"), Header: nats.Header{}}
			message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(sequence, 10))
			signalSeq = model.CommitSignal(message)
		}
		records, err := suspendedFixture(sequence, mode, base.Add(-2*time.Second), signalSeq)
		if err != nil {
			return trace, err
		}
		model.SetJournal("test", id, records)
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
	enqueueFault, err := schedule.Choose([]string{"drop_before_commit", "lose_ack_after_commit"})
	if err != nil {
		return trace, err
	}
	if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: enqueueFault}); err != nil {
		return trace, err
	}
	actors := make([]SuspendedLoopActor, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		name := name
		actors = append(actors, SuspendedLoopActor{Name: name, Run: func(_ context.Context, port reconcile.LoopPort, scanPort reconcile.SuspendedScanPort) error {
			scan := reconcile.NewSuspendedScanWithPort(scanPort)
			scan.Now = func() time.Time { return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond) }
			return reconcile.RunLoopWithPort(loopCtx,
				stopAfterSuspendedSavePort{LoopPort: port, model: model, stop: stopLoops},
				name, "suspended", 100*time.Millisecond, 1, scan.Scan)
		}})
	}
	results, err := RunSuspendedLoopActors(ctx, schedule, loop, timeoutSuspendedPort{model}, actors)
	if err != nil {
		return trace, err
	}
	if results["alpha"] != nil || results["beta"] != nil {
		return trace, fmt.Errorf("seed %d scanner results=%v", seed, results)
	}
	if got := len(model.Runs()); got != 10 {
		return trace, fmt.Errorf("seed %d retained wakeups=%d", seed, got)
	}
	for i, message := range model.Runs() {
		id := fmt.Sprintf("compete-suspended-%02d", i)
		if string(message.Data) != identity.Key("test", id) {
			return trace, fmt.Errorf("seed %d wakeup %d data=%q", seed, i, message.Data)
		}
	}
	creates := 0
	for _, event := range schedule.Trace().Transport {
		if event.Operation == "kv_create" && event.Subject == "system.suspended-reconciler" && event.Outcome == "ok" {
			creates++
		}
	}
	if creates < 2 {
		return trace, fmt.Errorf("seed %d scanner lease did not turn over: creates=%d", seed, creates)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeSuspendedScannersReplay(t *testing.T) {
	if os.Getenv("SIM_TWO_SUSPENDED_SCANNERS_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoSuspendedLoops(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TWO_SUSPENDED_SCANNERS_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runTwoSuspendedLoops(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-two-suspended-scanners-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-two-suspended-scanners.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runTwoSuspendedLoops(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("two-suspended-scanners-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeSuspendedScannersReplay$")
		cmd.Env = append(os.Environ(), "SIM_TWO_SUSPENDED_SCANNERS_HELPER=1", "SIM_TWO_SUSPENDED_SCANNERS_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("two-suspended-scanner trace changed across processes")
	}
}
