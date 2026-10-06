package integrity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestStateStreamUsesWholeAuditDeadlineWhileMetadataStaysBounded(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	deadline, _ := ctx.Deadline()
	// Model a complete set requiring three seconds of delivery opportunity.
	// Reject a shorter window immediately; this needs no wall-clock sleeps.
	read := func(call context.Context) (int, error) {
		end, _ := call.Deadline()
		if time.Until(end) < 3*time.Second {
			return 0, context.DeadlineExceeded
		}
		if !end.Equal(deadline) {
			t.Fatal("state read extended or replaced the audit deadline")
		}
		return 88068, nil
	}
	if count, err := auditRead(ctx, read); count != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("control no longer models the original two-second admission failure")
	}
	if count, err := auditStateRead(ctx, read); count != 88068 || err != nil {
		t.Fatalf("whole set rejected despite available audit budget: %d %v", count, err)
	}
}

func TestStateStreamRetryClosesAttemptAndKeepsOriginalDeadline(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	deadline, _ := ctx.Deadline()
	var calls []context.Context
	count, err := auditStateRead(ctx, func(call context.Context) (int, error) {
		if len(calls) != 0 && calls[len(calls)-1].Err() == nil {
			t.Fatal("previous attempt remains active")
		}
		end, _ := call.Deadline()
		if !end.Equal(deadline) {
			t.Fatal("retry changed the audit deadline")
		}
		calls = append(calls, call)
		if len(calls) < 3 {
			return 0, nats.ErrNoResponders
		}
		return 7, nil
	})
	if count != 7 || err != nil || len(calls) != 3 || calls[2].Err() == nil {
		t.Fatalf("retry/cleanup changed: %d %v calls=%d", count, err, len(calls))
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = auditStateRead(canceled, func(context.Context) (int, error) {
		t.Fatal("canceled audit started transport")
		return 0, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation identity changed")
	}
}
