package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSuspendedScanLeaderChild(t *testing.T) {
	if os.Getenv("WF_SUSPENDED_SCAN_LEADER_CHILD") != "1" {
		t.Skip("suspended scan leader helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_SUSPENDED_SCAN_LEADER_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcile.RunSuspendedLoop(context.Background(), js, "scan-scale-first", 100*time.Millisecond, 32); err != nil {
		t.Fatal(err)
	}
}

// WF_SUSPENDED_SCAN_SCALE=1 runs the Phase 7 million-suspended cursor proof.
// A lower WF_SUSPENDED_SCAN_COUNT is useful for diagnosing the fixture.
func TestMillionSuspendedScanCursorSurvivesLeaderKill(t *testing.T) {
	if os.Getenv("WF_SUSPENDED_SCAN_SCALE") == "" {
		t.Skip("set WF_SUSPENDED_SCAN_SCALE=1 for the million-suspended cursor proof")
	}
	count := 1000000
	if raw := os.Getenv("WF_SUSPENDED_SCAN_COUNT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1000 || parsed > count {
			t.Fatalf("invalid WF_SUSPENDED_SCAN_COUNT %q", raw)
		}
		count = parsed
	}
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	const typ = "scan-scale"
	jobs := make(chan int, 256)
	var workers sync.WaitGroup
	var completed atomic.Int64
	var firstErr error
	var failOnce sync.Once
	start := time.Now()
	for workerID := 0; workerID < 96; workerID++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			js := all[workerID%len(all)]
			store := journal.New(js)
			for index := range jobs {
				id := fmt.Sprintf("scan-%07d", index)
				if _, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`)); err != nil {
					failOnce.Do(func() { firstErr = fmt.Errorf("publish %s: %w", id, err); cancel() })
					return
				}
				var tail uint64
				for step, entry := range []journal.Entry{
					{Epoch: 0, Index: 0, Kind: journal.Started},
					{Epoch: 0, Index: 1, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"signal","name":"go"}`)},
					{Epoch: 0, Index: 2, Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"signal:go"}`)},
				} {
					var err error
					tail, err = store.Append(ctx, typ, id, entry, tail)
					if err != nil {
						failOnce.Do(func() { firstErr = fmt.Errorf("append %s step %d: %w", id, step, err); cancel() })
						return
					}
				}
				if done := completed.Add(1); done%100000 == 0 {
					t.Logf("journaled %d/%d suspended invocations in %s", done, count, time.Since(start))
				}
			}
		}(workerID)
	}
produce:
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break produce
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil || completed.Load() != int64(count) {
		t.Fatalf("suspended fixture=%d/%d first_error=%v context=%v", completed.Load(), count, firstErr, ctx.Err())
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	jrn, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	invInfo, err := inv.Info(ctx)
	if err != nil || invInfo.State.NumSubjects != uint64(count) || invInfo.State.Msgs != uint64(count) {
		t.Fatalf("invocation fixture: info=%+v err=%v", invInfo, err)
	}
	jrnInfo, err := jrn.Info(ctx)
	if err != nil || jrnInfo.State.NumSubjects != uint64(count) || jrnInfo.State.Msgs != uint64(3*count) {
		t.Fatalf("journal fixture: info=%+v err=%v", jrnInfo, err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	readCursor := func() (uint64, error) {
		entry, err := state.Get(ctx, "scan.suspended")
		if err != nil {
			return 0, err
		}
		return strconv.ParseUint(string(entry.Value()), 10, 64)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	process := exec.Command(executable, "-test.run=^TestSuspendedScanLeaderChild$")
	process.Env = append(os.Environ(), "WF_SUSPENDED_SCAN_LEADER_CHILD=1", "WF_SUSPENDED_SCAN_LEADER_URL="+cluster.Servers[0].ClientURL())
	childLog, err := os.CreateTemp(t.TempDir(), "suspended-leader-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer childLog.Close()
	process.Stdout, process.Stderr = childLog, childLog
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.Wait() }()
	reaped := false
	defer func() {
		if !reaped {
			_ = process.Process.Kill()
			<-waitDone
		}
	}()
	var before uint64
	for ctx.Err() == nil {
		select {
		case childErr := <-waitDone:
			reaped = true
			output, _ := os.ReadFile(childLog.Name())
			t.Fatalf("first scanner exited early: %v; output=%s", childErr, output)
		default:
		}
		before, err = readCursor()
		if err == nil && before >= 129 && before < uint64(count/2) {
			break
		}
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("first scanner did not advance cursor: %v", ctx.Err())
	}
	if err := process.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := <-waitDone
	reaped = true
	status, ok := process.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("scanner leader was not killed: state=%v err=%v", process.ProcessState, waitErr)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- reconcile.RunSuspendedLoop(secondCtx, all[2], "scan-scale-second", 100*time.Millisecond, 32)
	}()
	secondFinished := false
	defer func() {
		stopSecond()
		if !secondFinished {
			if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("second scanner: %v", err)
			}
		}
	}()
	last := before
	for ctx.Err() == nil {
		select {
		case secondErr := <-secondDone:
			secondFinished = true
			t.Fatalf("replacement scanner stopped before cursor advanced: %v", secondErr)
		default:
		}
		cursor, err := readCursor()
		if err != nil {
			t.Fatal(err)
		}
		if cursor < last {
			t.Fatalf("cursor moved backward across leader kill: previous=%d current=%d", last, cursor)
		}
		last = cursor
		if cursor >= before+128 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("replacement scanner did not resume after kill: before=%d last=%d err=%v", before, last, ctx.Err())
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	runInfo, err := run.Info(ctx)
	if err != nil || runInfo.State.Msgs != 0 {
		t.Fatalf("suspended scan published an unready wakeup: info=%+v err=%v", runInfo, err)
	}
	t.Logf("retained %d suspended invocations; cursor advanced %d -> %d across SIGKILL in %s", count, before, last, time.Since(start))
}
