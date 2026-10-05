//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMatrixAuditWaitObservationStopsWithoutCapturing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	captured := false
	stop := matrixAuditWaitObservation(ctx, time.Millisecond, func(time.Time) error { captured = true; return nil })
	if err := stop(); err != nil || captured {
		t.Fatalf("completed audit sampled: %v %v", captured, err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := matrixAuditWaitObservation(ctx2, 0, func(time.Time) error { t.Error("deadline-free context sampled"); return nil })(); err != nil {
		t.Fatal(err)
	}
}

func TestMatrixAuditWaitObservationCapturesPendingParentAndTrace(t *testing.T) {
	t.Setenv("WF_TIER3_AUDIT_WAIT_STACK", "1")
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	trace := &retainedAuditTrace{}
	complete := trace.begin(ctx, "WF_STATE.WatchAll", "")
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go matrixFailureStackBlockedParent(entered, release, done)
	<-entered
	defer func() { close(release); <-done }()
	stop := startMatrixAuditWaitObservation(ctx, root, "pending", trace)
	// Observe the actual files rather than cancelling at the intended timer
	// time: a delayed scheduler must not be mistaken for completed capture.
	for {
		if _, err := os.Stat(filepath.Join(root, "pending-goroutines.json")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("pending observer did not finish")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	complete(nil, 0)
	raw, err := os.ReadFile(filepath.Join(root, "pending-trace.json"))
	var snapshot retainedAuditTraceSnapshot
	if err != nil || json.Unmarshal(raw, &snapshot) != nil || snapshot.Counts["WF_STATE.WatchAll"].Started != 1 || snapshot.Counts["WF_STATE.WatchAll"].Completed != 0 {
		t.Fatalf("pending trace=%s err=%v", raw, err)
	}
	raw, err = os.ReadFile(filepath.Join(root, "pending-goroutines.txt"))
	if err != nil || !bytes.Contains(raw, []byte("matrixFailureStackBlockedParent")) {
		t.Fatal("pending parent absent", err)
	}
	raw, err = os.ReadFile(filepath.Join(root, "pending-goroutines.json"))
	var meta struct {
		PID                int
		Observed, Deadline time.Time
		Truncated          bool
	}
	if err != nil || json.Unmarshal(raw, &meta) != nil || meta.PID != os.Getpid() || !meta.Deadline.Equal(deadline) || !meta.Observed.Before(deadline) || meta.Truncated {
		t.Fatalf("stack metadata=%+v err=%v", meta, err)
	}
}

func TestMatrixAuditWaitObservationRetainsCaptureError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	captured := make(chan struct{})
	want := errors.New("capture failed")
	stop := matrixAuditWaitObservation(ctx, time.Second, func(time.Time) error { close(captured); return want })
	<-captured
	if err := stop(); !errors.Is(err, want) {
		t.Fatal(err)
	}
}
