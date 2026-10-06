//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestMatrixParallelInvocationAuditsOrderAndBound(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const first, last = uint64(17), uint64(1016)
	var active, peak, began atomic.Int64
	var seen [1000]atomic.Int64
	barrier, tail := make(chan struct{}), make(chan struct{})
	results, err := matrixParallelInvocationAudits(ctx, first, last, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		if began.Add(1) == 32 {
			close(barrier)
		}
		select {
		case <-barrier:
		case <-ctx.Done():
			return matrixInvocationAuditResult{}, ctx.Err()
		}
		if sequence == first {
			select {
			case <-tail:
			case <-ctx.Done():
				return matrixInvocationAuditResult{}, ctx.Err()
			}
		}
		if sequence == last {
			close(tail)
		}
		seen[sequence-first].Add(1)
		return matrixInvocationAuditResult{samples: []matrixLatencySample{{ID: fmt.Sprint(sequence)}}, mixed: sequence%2 == 0}, nil
	})
	if err != nil || len(results) != 1000 || peak.Load() != 32 || active.Load() != 0 {
		t.Fatalf("results=%d peak=%d active=%d err=%v", len(results), peak.Load(), active.Load(), err)
	}
	for i, result := range results {
		sequence := first + uint64(i)
		if seen[i].Load() != 1 || len(result.samples) != 1 || result.samples[0].ID != fmt.Sprint(sequence) || result.mixed != (sequence%2 == 0) {
			t.Fatalf("changed/omitted/duplicate sequence %d: %+v visits=%d", sequence, result, seen[i].Load())
		}
	}
}

func TestMatrixParallelInvocationAuditsFailureJoinsReaders(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	fault := errors.New("corrupt invocation")
	var began, active atomic.Int64
	barrier := make(chan struct{})
	results, err := matrixParallelInvocationAudits(ctx, 1, 1000, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		active.Add(1)
		defer active.Add(-1)
		if began.Add(1) == 32 {
			close(barrier)
		}
		if sequence == 1 {
			select {
			case <-barrier:
				return matrixInvocationAuditResult{}, fault
			case <-ctx.Done():
				return matrixInvocationAuditResult{}, ctx.Err()
			}
		}
		<-ctx.Done()
		return matrixInvocationAuditResult{}, ctx.Err()
	})
	if results != nil || !errors.Is(err, fault) || began.Load() != 32 || active.Load() != 0 {
		t.Fatalf("partial result or unjoined reader: result=%v err=%v began=%d active=%d", results, err, began.Load(), active.Load())
	}
}

func TestMatrixParallelInvocationAuditsDeadlines(t *testing.T) {
	_, err := matrixParallelInvocationAudits(context.Background(), 1, 1, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		deadline, ok := ctx.Deadline()
		if left := time.Until(deadline); !ok || left > 20*time.Second || left < 19*time.Second {
			t.Fatalf("request deadline changed: %s, present=%v", left, ok)
		}
		return matrixInvocationAuditResult{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	parentDeadline, _ := ctx.Deadline()
	var active atomic.Int64
	results, err := matrixParallelInvocationAudits(ctx, 1, 1000, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(parentDeadline) {
			t.Errorf("reader extended parent deadline: %s != %s", deadline, parentDeadline)
		}
		active.Add(1)
		defer active.Add(-1)
		<-ctx.Done()
		return matrixInvocationAuditResult{}, ctx.Err()
	})
	if results != nil || !errors.Is(err, context.DeadlineExceeded) || active.Load() != 0 {
		t.Fatalf("deadline/closure: result=%v err=%v active=%d", results, err, active.Load())
	}
}

func TestMatrixParallelInvocationAuditsEmptyCancelledAndOverflow(t *testing.T) {
	read := func(context.Context, uint64) (matrixInvocationAuditResult, error) {
		t.Fatal("unexpected read")
		return matrixInvocationAuditResult{}, nil
	}
	if results, err := matrixParallelInvocationAudits(context.Background(), 1, 0, read); err != nil || results != nil {
		t.Fatalf("empty range: %v %v", results, err)
	}
	if _, err := matrixParallelInvocationAudits(context.Background(), 0, math.MaxUint64, read); err == nil {
		t.Fatal("overflow range accepted")
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if results, err := matrixParallelInvocationAudits(ctx, 1, 10, read); results != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled range: %v %v", results, err)
	}
}
