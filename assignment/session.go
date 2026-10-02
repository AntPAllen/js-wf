package assignment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/lease"
)

// SessionEvent describes a stopped member and its acknowledged replacement.
// Observers must return promptly. Worker loops are joined before stop is emitted.
type SessionEvent struct {
	At            time.Time `json:"at"`
	Worker        string    `json:"worker"`
	Phase         string    `json:"phase"`
	PreviousEpoch uint64    `json:"previous_epoch"`
	Epoch         uint64    `json:"epoch"`
	Error         string    `json:"error,omitempty"`
}

func recoverableSessionError(err error) bool {
	return errors.Is(err, lease.ErrLost) || errors.Is(err, ErrConflict) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse)
}

// RunWithWorkers supervises this registration and its partition loops. A failed
// controller pass cancels and joins all loops from that session, resolves only
// its own retained leases, then registers a fresh epoch before restarting loops.
// runWorkers must return only after every worker loop it started has stopped.
// Initial registration errors remain the caller's responsibility. The receiver
// is consumed by this method; do not call Step, Run or Close concurrently.
func (c *Controller) RunWithWorkers(ctx context.Context, runWorkers func(context.Context) error, observe func(SessionEvent)) error {
	if runWorkers == nil {
		return fmt.Errorf("nil membership worker runner")
	}
	current := c
	defer func() { current.Close() }()
	emit := func(phase string, previous uint64, err error) {
		if observe == nil {
			return
		}
		event := SessionEvent{At: time.Now().UTC(), Worker: c.id, Phase: phase, PreviousEpoch: previous, Epoch: current.registration.Epoch()}
		if err != nil {
			event.Error = err.Error()
		}
		observe(event)
	}
	for ctx.Err() == nil {
		session, stop := context.WithCancel(ctx)
		type result struct {
			controller bool
			err        error
		}
		results := make(chan result, 2)
		go func() { results <- result{true, current.Run(session)} }()
		go func() { results <- result{false, runWorkers(session)} }()
		first := <-results
		stop()
		second := <-results
		// At this boundary both controller and partition loops are joined.
		controllerResult, workerResult := first, second
		if !first.controller {
			controllerResult, workerResult = second, first
		}
		if ctx.Err() != nil {
			return nil
		}
		if !first.controller {
			if workerResult.err == nil {
				return fmt.Errorf("membership worker runner stopped unexpectedly")
			}
			return workerResult.err
		}
		cause := controllerResult.err
		if cause == nil {
			return fmt.Errorf("membership controller stopped unexpectedly")
		}
		if !recoverableSessionError(cause) {
			return cause
		}
		previous := current.registration.Epoch()
		emit("stopped", previous, cause)
		// Cleanup is safe only after the workers above have joined. It rereads and
		// revision-deletes this exact worker/epoch; it cannot delete a successor.
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		if current.coordinator != nil {
			_ = current.coordinator.Cleanup(cleanup)
		}
		_ = current.registration.Cleanup(cleanup)
		done()
		for ctx.Err() == nil {
			attempt, done := context.WithTimeout(ctx, 8*time.Second)
			next, err := c.membership.Controller(attempt, c.id, c.assignments)
			done()
			if err == nil {
				current = next
				emit("rejoined", previous, nil)
				break
			}
			if !errors.Is(err, lease.ErrHeld) && !recoverableSessionError(err) {
				return fmt.Errorf("membership rejoin: %w", err)
			}
			emit("rejoin_wait", previous, err)
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
	return nil
}
