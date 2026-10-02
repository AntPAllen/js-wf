package wf

import (
	"context"
	"fmt"
	"time"

	"js-wf/identity"
)

// TimerClockSupport supplies a common durable deadline domain. Bounds must
// conservatively enclose current time in that domain without worker wall time.
// Schedule must translate that deadline for its transport and retain the domain
// in repair records. A delivery timestamp is never a due proof in this domain.
type TimerClockSupport struct {
	Domain   string
	Bounds   func(context.Context) (lower, upper time.Time, err error)
	Schedule func(context.Context, uint64, time.Time, string) error
}

// SetTimerClockSupport selects the domain for new positive timers. Legacy
// requests without a domain retain SetTimerSupport semantics. Completed journal
// decisions replay without consulting either clock. Pending domain-tagged timers
// require matching support; they never fall back to the delivery timestamp.
// Enable tagged writers only after all workers and repairers support the domain;
// older binaries ignore the additional JSON field and cannot safely read it.
func (c *Context) SetTimerClockSupport(support TimerClockSupport) error {
	if err := identity.ValidateToken(support.Domain); err != nil {
		return err
	}
	if support.Bounds == nil {
		return fmt.Errorf("%w: no clock bounds", ErrTimerSchedule)
	}
	c.timerClock = &support
	return nil
}

func (c *Context) timerOrigin() (time.Time, string, error) {
	if c.timerClock != nil {
		_, upper, err := c.clockBounds(c.timerClock.Domain)
		return upper, c.timerClock.Domain, err
	}
	if c.timerNow == nil {
		return time.Time{}, "", fmt.Errorf("no server clock")
	}
	now, err := c.timerNow(c.base)
	if err == nil && now.IsZero() {
		err = fmt.Errorf("empty server clock")
	}
	return now, "", err
}

func (c *Context) clockBounds(domain string) (time.Time, time.Time, error) {
	if c.timerClock == nil || c.timerClock.Domain != domain {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: unavailable timer clock domain %q", ErrTimerSchedule, domain)
	}
	lower, upper, err := c.timerClock.Bounds(c.base)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: %w", ErrTimerSchedule, err)
	}
	if lower.IsZero() || upper.IsZero() || lower.After(upper) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: invalid clock interval", ErrTimerSchedule)
	}
	return lower.UTC(), upper.UTC(), nil
}

func (c *Context) timerReady(domain string, fireAt time.Time, fresh bool) (bool, time.Time, error) {
	if fireAt.IsZero() {
		return true, time.Time{}, nil
	}
	if fresh {
		return false, time.Time{}, nil
	}
	if domain != "" {
		lower, _, err := c.clockBounds(domain)
		return err == nil && !lower.Before(fireAt), lower, err
	}
	return !c.wakeupAt.IsZero() && !c.wakeupAt.Before(fireAt), c.wakeupAt, nil
}

func (c *Context) scheduleInDomain(step uint64, fireAt time.Time, domain string) error {
	if domain != "" {
		if c.timerClock == nil || c.timerClock.Domain != domain || c.timerClock.Schedule == nil {
			return fmt.Errorf("%w: no scheduler for clock domain %q", ErrTimerSchedule, domain)
		}
		return c.timerClock.Schedule(c.base, step, fireAt, domain)
	}
	if c.scheduleTimer == nil {
		return fmt.Errorf("%w: no scheduler", ErrTimerSchedule)
	}
	return c.scheduleTimer(c.base, step, fireAt)
}
