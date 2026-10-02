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

	"github.com/nats-io/nats.go"
)

type timeoutSuspendedPort struct{ reconcile.SuspendedScanPort }

func (p timeoutSuspendedPort) EnqueueSuspended(ctx context.Context, typ, id string, sequence uint64, retryWindow int64) error {
	err := p.SuspendedScanPort.EnqueueSuspended(ctx, typ, id, sequence, retryWindow)
	if errors.Is(err, ErrTransportLost) {
		return nats.ErrTimeout
	}
	return err
}

func runSeededSuspendedLoop(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("suspended_reconcile_loop_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewSignalTransport(schedule)
	loop := NewLoopTransport(schedule)
	scan := reconcile.NewSuspendedScanWithPort(timeoutSuspendedPort{model})
	scan.Now = func() time.Time { return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond) }
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("loop-suspended-%02d", i)
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
	enqueueChoice, err := schedule.Choose([]string{"drop_before_commit", "lose_ack_after_commit"})
	if err != nil {
		return trace, err
	}
	if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: enqueueChoice}); err != nil {
		return trace, err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	loop.StopAfterWaits(10, stopFirst)
	if err := reconcile.RunLoopWithPort(firstCtx, loop, "first", "suspended", 100*time.Millisecond, 1, scan.Scan); err != nil {
		return trace, fmt.Errorf("seed %d first loop: %w", seed, err)
	}
	if loop.saves < at {
		return trace, fmt.Errorf("seed %d first scanner missed cursor fault at save %d after %d attempts", seed, at, loop.saves)
	}
	cursor, revision, err := loop.LoadCursor(ctx, "suspended")
	if err != nil || revision == 0 || cursor < 1 || cursor > 11 || len(model.Runs()) < 8 || len(model.Runs()) > 10 {
		return trace, fmt.Errorf("seed %d first cursor=%d revision=%d runs=%d err=%v", seed, cursor, revision, len(model.Runs()), err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	loop.StopAfterWaits(25, stopSecond)
	if err := reconcile.RunLoopWithPort(secondCtx, loop, "replacement", "suspended", 100*time.Millisecond, 1, scan.Scan); err != nil {
		return trace, fmt.Errorf("seed %d replacement loop: %w", seed, err)
	}
	if len(model.Runs()) != 20 {
		return trace, fmt.Errorf("seed %d final runs=%d", seed, len(model.Runs()))
	}
	for i, run := range model.Runs() {
		id := fmt.Sprintf("loop-suspended-%02d", i)
		if string(run.Data) != identity.Key("test", id) {
			return trace, fmt.Errorf("seed %d run %d data=%s", seed, i, run.Data)
		}
	}
	report, err := CheckSuspendedWakeupLiveness(model, scan.Now(), time.Second)
	if err != nil || report.Enabled != 20 || len(report.Waiting) != 0 {
		return trace, fmt.Errorf("seed %d suspended liveness: enabled=%d waiting=%v missing=%v err=%v", seed, report.Enabled, report.Waiting, report.Missing, err)
	}
	waiting, err := json.Marshal(report.Waiting)
	if err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_suspended_liveness", Sequence: uint64(report.Enabled), DataSHA256: digest(waiting), Outcome: "ok", AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSuspendedLoopReplay(t *testing.T) {
	if os.Getenv("SIM_SUSPENDED_LOOP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSuspendedLoop(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SUSPENDED_LOOP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededSuspendedLoop(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-suspended-loop-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-suspended-loop.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSuspendedLoop(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("suspended-loop-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSuspendedLoopReplay$")
		cmd.Env = append(os.Environ(), "SIM_SUSPENDED_LOOP_HELPER=1", "SIM_SUSPENDED_LOOP_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("suspended loop trace changed across processes")
	}
}
