package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"
)

var ErrTimerClock = errors.New("timer repair clock unavailable")

// TimerDomainClock returns a conservative lower bound in the recorded domain.
// It must authenticate clock sources and honor context cancellation. Missing,
// unknown or failed domains must return an error, never worker wall time.
type TimerDomainClock func(context.Context, string) (time.Time, error)

func timerDeadlineDue(ctx context.Context, clock TimerDomainClock, legacy func() (time.Time, error), domain string, at time.Time, grace time.Duration) (bool, error) {
	if domain == "" {
		if at.IsZero() {
			return true, nil
		}
		now, err := legacy()
		return err == nil && !now.Before(at.Add(grace)), err
	}
	if identity.ValidateToken(domain) != nil || clock == nil || at.IsZero() {
		return false, fmt.Errorf("%w: unsupported domain %q or deadline", ErrTimerClock, domain)
	}
	bound, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bound.Err(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrTimerClock, err)
	}
	lower, err := clock(bound, domain)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrTimerClock, err)
	}
	if err := bound.Err(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrTimerClock, err)
	}
	if lower.IsZero() {
		return false, fmt.Errorf("%w: empty domain lower bound", ErrTimerClock)
	}
	return !lower.Before(at.Add(grace)), nil
}
