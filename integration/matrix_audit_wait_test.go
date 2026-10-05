//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The observer uses the existing attempt deadline. It neither cancels nor
// extends the audit. A pending call in the trace is evidence at capture time,
// not a claim that the call caused the subsequent timeout.
func startMatrixAuditWaitObservation(ctx context.Context, root, name string, trace *retainedAuditTrace) func() error {
	if os.Getenv("WF_TIER3_AUDIT_WAIT_STACK") != "1" {
		return func() error { return nil }
	}
	return matrixAuditWaitObservation(ctx, time.Second, func(deadline time.Time) error {
		var traceErr error
		if trace != nil {
			data, err := json.MarshalIndent(trace.snapshot(), "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, name+"-trace.json"), data, 0600)
			}
			traceErr = err
		}
		stackErr := saveMatrixParentStack(root, name+"-goroutines", "Parent SDK process sampled near a still-pending audit attempt deadline; child stacks excluded; this is not a failure verdict", deadline)
		return errors.Join(traceErr, stackErr)
	})
}

func matrixAuditWaitObservation(ctx context.Context, lead time.Duration, capture func(time.Time) error) func() error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return func() error { return nil }
	}
	stop, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		timer := time.NewTimer(max(time.Until(deadline)-lead, 0))
		defer timer.Stop()
		select {
		case <-stop:
			finished <- nil
		case <-ctx.Done():
			finished <- nil
		case <-timer.C:
			// Prefer an already completed/cancelled call to invented wait
			// evidence when the timer and cancellation become ready together.
			select {
			case <-stop:
				finished <- nil
			case <-ctx.Done():
				finished <- nil
			default:
				finished <- capture(deadline)
			}
		}
	}()
	var once sync.Once
	var err error
	return func() error {
		once.Do(func() { close(stop); err = <-finished })
		if err != nil {
			return fmt.Errorf("retained audit wait observation: %w", err)
		}
		return nil
	}
}
