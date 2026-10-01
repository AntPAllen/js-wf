//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Both stream lookup and refresh must run within the same drain deadline.
// A successful client read of zero messages is required; monitoring is evidence
// for diagnosis only and cannot replace this check.
func waitMixedRunDrain(ctx context.Context, requestBudget, poll time.Duration, read func(context.Context) (*jetstream.StreamInfo, error)) (*jetstream.StreamInfo, error) {
	var info *jetstream.StreamInfo
	var lastErr error
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, requestBudget)
		current, err := read(attempt)
		stop()
		lastErr = err
		if err == nil {
			if current == nil {
				return info, errors.New("run metadata read returned nil info")
			}
			info = current
			if ctx.Err() == nil && info.State.Msgs == 0 {
				return info, nil
			}
		} else if !mixedRunReadTransient(err) {
			return info, err
		}
		select {
		case <-ctx.Done():
		case <-time.After(poll):
		}
	}
	return info, fmt.Errorf("run drain deadline: last read error=%v: %w", lastErr, ctx.Err())
}

func mixedRunReadTransient(err error) bool {
	if err == context.DeadlineExceeded || err == nats.ErrTimeout || err == nats.ErrNoResponders || err == jetstream.ErrNoStreamResponse {
		return true
	}
	// A joined permanent error must not be hidden by a transient cause.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !mixedRunReadTransient(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return mixedRunReadTransient(wrapped.Unwrap())
	}
	if api, ok := err.(*jetstream.APIError); ok {
		return api.ErrorCode == 10008
	}
	return false
}

func TestMixedRunDrainReadContract(t *testing.T) {
	t.Run("lost lookup reply then retained message then drain", func(t *testing.T) {
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		calls := 0
		info, err := waitMixedRunDrain(ctx, 15*time.Millisecond, time.Millisecond, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			calls++
			if calls == 1 {
				deadline, ok := attempt.Deadline()
				if !ok || time.Until(deadline) > 20*time.Millisecond {
					t.Fatal("lookup did not receive its bounded request context")
				}
				<-attempt.Done()
				return nil, attempt.Err()
			}
			if calls == 2 {
				return &jetstream.StreamInfo{State: jetstream.StreamState{Msgs: 1}}, nil
			}
			return &jetstream.StreamInfo{}, nil
		})
		if err != nil || info == nil || info.State.Msgs != 0 || calls != 3 {
			t.Fatalf("info=%+v err=%v calls=%d", info, err, calls)
		}
	})
	t.Run("permanently retained message fails", func(t *testing.T) {
		ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer stop()
		info, err := waitMixedRunDrain(ctx, time.Second, time.Millisecond, func(context.Context) (*jetstream.StreamInfo, error) {
			return &jetstream.StreamInfo{State: jetstream.StreamState{Msgs: 1}}, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || info == nil || info.State.Msgs != 1 {
			t.Fatalf("retained queue accepted: info=%+v err=%v", info, err)
		}
	})
	t.Run("missing metadata is bounded by total budget", func(t *testing.T) {
		ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer stop()
		calls := 0
		info, err := waitMixedRunDrain(ctx, 5*time.Millisecond, time.Millisecond, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			calls++
			<-attempt.Done()
			return nil, attempt.Err()
		})
		if !errors.Is(err, context.DeadlineExceeded) || info != nil || calls < 2 {
			t.Fatalf("lost metadata not bounded/retried: info=%+v err=%v calls=%d", info, err, calls)
		}
	})
	t.Run("hard error is immediate even when joined", func(t *testing.T) {
		denied := errors.New("metadata permission denied")
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		calls := 0
		_, err := waitMixedRunDrain(ctx, time.Second, time.Millisecond, func(context.Context) (*jetstream.StreamInfo, error) {
			calls++
			return nil, fmt.Errorf("lookup: %w", errors.Join(nats.ErrNoResponders, denied))
		})
		if calls != 1 || !errors.Is(err, denied) {
			t.Fatalf("permanent error retried: calls=%d err=%v", calls, err)
		}
	})
	t.Run("parent cancellation interrupts lost reply", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		_, err := waitMixedRunDrain(ctx, time.Second, time.Second, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			calls++
			cancel()
			<-attempt.Done()
			return nil, attempt.Err()
		})
		if calls != 1 || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation ignored: calls=%d err=%v", calls, err)
		}
	})
}

func TestMixedRunReadTransientErrors(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, nats.ErrTimeout, nats.ErrNoResponders, jetstream.ErrNoStreamResponse, &jetstream.APIError{ErrorCode: 10008}} {
		if !mixedRunReadTransient(fmt.Errorf("metadata read: %w", err)) {
			t.Errorf("named transient rejected: %v", err)
		}
	}
	for _, err := range []error{context.Canceled, jetstream.ErrStreamNotFound, &jetstream.APIError{ErrorCode: 10003}, errors.New("invalid stream configuration"), errors.Join(nats.ErrTimeout, jetstream.ErrStreamNotFound)} {
		if mixedRunReadTransient(fmt.Errorf("metadata read: %w", err)) {
			t.Errorf("permanent error accepted: %v", err)
		}
	}
}
