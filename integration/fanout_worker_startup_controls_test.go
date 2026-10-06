package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/worker"
)

func TestFanoutWorkerStartupRetriesOnlyTransientErrors(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, nats.ErrTimeout, nats.ErrNoResponders, &jetstream.APIError{ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}} {
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		calls := 0
		expected := &worker.Worker{}
		actual, attempts, err := retryFanoutWorkerStartup(ctx, func(context.Context) (*worker.Worker, error) {
			calls++
			if calls == 1 {
				return nil, fmt.Errorf("signal stream: %w", cause)
			}
			return expected, nil
		})
		stop()
		if err != nil || actual != expected || calls != 2 || len(attempts) != 2 || attempts[0].Error == "" || attempts[1].Error != "" || !attempts[0].Retryable || attempts[1].Retryable {
			t.Fatalf("retry cause=%v calls=%d attempts=%+v err=%v", cause, calls, attempts, err)
		}
	}
	for _, cause := range []error{errors.New("invalid worker option"), jetstream.ErrStreamNotFound, jetstream.ErrBucketNotFound, context.Canceled} {
		calls := 0
		_, attempts, err := retryFanoutWorkerStartup(context.Background(), func(context.Context) (*worker.Worker, error) { calls++; return nil, cause })
		if err != cause || calls != 1 || len(attempts) != 1 || attempts[0].Retryable {
			t.Fatalf("permanent failure retried: %v calls=%d", err, calls)
		}
	}
}

func TestFanoutWorkerStartupRetainsOriginalCaseDeadline(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	deadline, _ := ctx.Deadline()
	calls := 0
	_, attempts, err := retryFanoutWorkerStartup(ctx, func(child context.Context) (*worker.Worker, error) {
		calls++
		actual, _ := child.Deadline()
		if actual != deadline {
			t.Fatal("constructor received extended deadline")
		}
		return nil, context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || len(attempts) != 1 {
		t.Fatalf("deadline not retained: calls=%d attempts=%d err=%v", calls, len(attempts), err)
	}
	canceled, finish := context.WithCancel(context.Background())
	finish()
	_, attempts, err = retryFanoutWorkerStartup(canceled, func(context.Context) (*worker.Worker, error) {
		t.Fatal("constructor called after cancellation")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) || len(attempts) != 0 {
		t.Fatalf("pre-cancellation ignored: attempts=%d err=%v", len(attempts), err)
	}
}

func TestFanoutWorkerStartupRejectsSuccessAfterCancellation(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	actual, attempts, err := retryFanoutWorkerStartup(ctx, func(context.Context) (*worker.Worker, error) { stop(); return &worker.Worker{}, nil })
	if actual != nil || !errors.Is(err, context.Canceled) || len(attempts) != 1 || attempts[0].Error == "" {
		t.Fatalf("late constructor success accepted: worker=%v attempts=%+v err=%v", actual, attempts, err)
	}
}
