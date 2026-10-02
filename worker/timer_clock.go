package worker

import (
	"context"
	"fmt"
	"time"

	"js-wf/identity"
)

// WithTimerClock opts all new positive timers into a common durable domain.
// The provider must return authenticated conservative bounds and honor context
// cancellation. Domain-aware repair loops must be active; native schedules are
// hints that can become early or late when the scheduling leader clock changes.
// Upgrade every reader/repairer before enabling tagged writers.
func WithTimerClock(domain string, bounds func(context.Context) (time.Time, time.Time, error)) Option {
	return func(w *Worker) error {
		if err := identity.ValidateToken(domain); err != nil {
			return err
		}
		if bounds == nil {
			return fmt.Errorf("timer clock bounds unavailable")
		}
		w.timerClockDomain = domain
		w.timerClockBounds = bounds
		return nil
	}
}

func (w *Worker) domainTimerBounds(ctx context.Context) (time.Time, time.Time, error) {
	if w.timerClockBounds == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("timer domain clock unavailable")
	}
	bound, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bound.Err(); err != nil {
		return time.Time{}, time.Time{}, err
	}
	lower, upper, err := w.timerClockBounds(bound)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if err := bound.Err(); err != nil {
		return time.Time{}, time.Time{}, err
	}
	if lower.IsZero() || upper.IsZero() || lower.After(upper) {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid timer domain interval")
	}
	return lower.UTC(), upper.UTC(), nil
}

func (w *Worker) scheduleDomainTimer(ctx context.Context, typ, id string, invSeq, step uint64, fireAt time.Time, domain string) error {
	if domain == "" || domain != w.timerClockDomain {
		return fmt.Errorf("unknown timer clock domain %q", domain)
	}
	deadline := TimerDeadline{FireAt: fireAt, ClockDomain: domain}
	if w.nativeSchedules {
		// This is deliberately not proof of due: stream leadership can change
		// between this lookup and publication, or while a schedule is retained.
		physical, err := w.serverNow(ctx)
		if err != nil {
			return err
		}
		lower, _, err := w.domainTimerBounds(ctx)
		if err != nil {
			return err
		}
		remaining := fireAt.Sub(lower)
		if remaining < 0 {
			remaining = 0
		}
		deadline.ScheduleAt = physical.Add(remaining)
	}
	port := w.timerSchedulePort
	if port == nil && w.js != nil {
		port = NewTimerSchedulePort(w.js)
	}
	if port == nil {
		return fmt.Errorf("timer transport unavailable")
	}
	published, err := ScheduleTimerDeadlineWithPort(ctx, port, w.nativeSchedules, typ, id, invSeq, step, deadline)
	if published {
		w.metrics.timersScheduled.Add(1)
	}
	return err
}
