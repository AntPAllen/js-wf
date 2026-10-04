//go:build linux

package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMatrixProcessExitObservationPreservesChildFailure(t *testing.T) {
	base := filepath.Join(t.TempDir(), "clock-worker")
	log, err := os.Create(base + ".log")
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("/bin/sh", "-c", "printf 'worker clock proof: context deadline exceeded\\n'; exit 7")
	child.Stdout, child.Stderr = log, log
	if err := child.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	log.Close()
	process := &matrixProcessWorker{cmd: child, exited: make(chan error, 1), base: base, id: "clock-worker"}
	go func() { defer close(process.exited); process.exited <- child.Wait() }()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	err = waitMatrixProcessWorkerExit(ctx, process)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 || !strings.Contains(err.Error(), "worker clock proof: context deadline exceeded") {
		t.Fatalf("child failure masked: %v", err)
	}
	// The observer consumed the Wait result. Cleanup must still return from the
	// closed channel without signaling or trying to reap the child twice.
	stopMatrixProcessWorker(process)
}

func TestMatrixProcessExitObservationRejectsUnexpectedSuccess(t *testing.T) {
	process := &matrixProcessWorker{exited: make(chan error, 1), base: filepath.Join(t.TempDir(), "worker"), id: "worker"}
	process.exited <- nil
	close(process.exited)
	err := waitMatrixProcessWorkerExit(context.Background(), process)
	if err == nil || !strings.Contains(err.Error(), "exited unexpectedly with success") {
		t.Fatalf("unexpected success accepted: %v", err)
	}
}

func TestMatrixProcessExitObservationCancellationPreservesWaitResult(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	stop()
	process := &matrixProcessWorker{exited: make(chan error, 1)}
	want := errors.New("child exit")
	process.exited <- want
	if err := waitMatrixProcessWorkerExit(ctx, process); !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup cancellation: %v", err)
	}
	if got := <-process.exited; got != want {
		t.Fatalf("cleanup lost Wait result: %v", got)
	}
}
