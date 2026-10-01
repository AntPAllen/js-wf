package wf

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"
)

var ErrTimerCancelled = errors.New("timer was cancelled")
var ErrTimerFired = errors.New("timer has already fired")

// TimerHandle is bound to one workflow replay. Create it again on every replay
// by calling Context.Timer at the same point in the handler.
type TimerHandle struct {
	c                 *Context
	name              string
	step              uint64
	fireAt            time.Time
	cancelled         bool
	fired             bool
	createdInDelivery bool // The current wakeup cannot fire a newly created timer.
}

// Timer journals a durable timer and returns an awaitable handle. Creation
// does not suspend the workflow; other steps can run before Await or Cancel.
func (c *Context) Timer(name string, d time.Duration) (*TimerHandle, error) {
	if err := identity.ValidateToken(name); err != nil {
		return nil, err
	}
	step := c.stepPosition()
	var req request
	fresh := c.position >= len(c.entries)
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested || json.Unmarshal(recorded.Payload, &req) != nil {
			return nil, ErrCorruptJournal
		}
		if req.Kind != "timer_start" || req.Name != name || req.DurationNanos != int64(d) || d > 0 && req.FireAt.IsZero() {
			return nil, &NonDeterministicError{Index: recorded.Index, RecordedName: req.Name, RequestedName: name}
		}
	} else {
		req = request{Kind: "timer_start", Name: name, DurationNanos: int64(d)}
		if d > 0 {
			if c.timerNow == nil {
				return nil, fmt.Errorf("%w: no server clock", ErrTimerSchedule)
			}
			now, err := c.timerNow(c.base)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrTimerSchedule, err)
			}
			req.FireAt = now.Add(d).UTC()
		}
		payload, _ := json.Marshal(req)
		if err := c.next(StepRequested, payload); err != nil {
			return nil, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		if c.entries[c.position].Kind != StepCompleted {
			return nil, ErrCorruptJournal
		}
	} else {
		if d > 0 && (fresh || c.wakeupAt.IsZero() || c.wakeupAt.Before(req.FireAt)) {
			if c.scheduleTimer == nil {
				return nil, fmt.Errorf("%w: no scheduler", ErrTimerSchedule)
			}
			if err := c.scheduleTimer(c.base, step, req.FireAt); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrTimerSchedule, err)
			}
		}
		if err := c.next(StepCompleted, json.RawMessage(`{}`)); err != nil {
			return nil, err
		}
	}
	c.position++
	handle := &TimerHandle{c: c, name: name, step: step, fireAt: req.FireAt, createdInDelivery: fresh}
	if c.timerHandles == nil {
		c.timerHandles = make(map[uint64]*TimerHandle)
	}
	c.timerHandles[step] = handle
	return handle, nil
}

func (t *TimerHandle) action(kind string) error {
	c := t.c
	want := request{Kind: kind, Name: t.name, TimerStep: t.step}
	if kind == "timer_await" {
		want.FireAt = t.fireAt
	}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		var got request
		if recorded.Kind != StepRequested || json.Unmarshal(recorded.Payload, &got) != nil {
			return ErrCorruptJournal
		}
		if got.Kind != kind || got.Name != t.name || got.TimerStep != t.step || !got.FireAt.Equal(want.FireAt) {
			return &NonDeterministicError{Index: recorded.Index, RecordedName: got.Name, RequestedName: t.name}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return err
		}
	}
	c.position++
	return nil
}

// Await suspends until a run message with a server timestamp at or after the
// recorded fire time arrives. Several due handles can complete in one run.
func (t *TimerHandle) Await() error {
	if t.cancelled {
		return ErrTimerCancelled
	}
	if t.fired {
		return nil
	}
	if err := t.action("timer_await"); err != nil {
		return err
	}
	c := t.c
	if c.position < len(c.entries) {
		if c.entries[c.position].Kind != StepCompleted {
			return ErrCorruptJournal
		}
	} else {
		if !t.fireAt.IsZero() && (t.createdInDelivery || c.wakeupAt.IsZero() || c.wakeupAt.Before(t.fireAt)) {
			c.waitingOn = "timer:" + t.name
			return ErrSuspended
		}
		if err := c.next(StepCompleted, json.RawMessage(`{}`)); err != nil {
			return err
		}
		if !t.fireAt.IsZero() && c.timerFired != nil {
			c.timerFired(t.fireAt, c.wakeupAt)
		}
	}
	c.position++
	t.fired = true
	return nil
}

// Cancel records a deterministic cancellation. The server may still publish
// the scheduled wakeup; a completed invocation treats it as a no-op.
func (t *TimerHandle) Cancel() error {
	if t.cancelled {
		return nil
	}
	if t.fired {
		return ErrTimerFired
	}
	if err := t.action("timer_cancel"); err != nil {
		return err
	}
	c := t.c
	if c.position < len(c.entries) {
		var completion struct {
			Cancelled bool `json:"cancelled"`
		}
		if c.entries[c.position].Kind != StepCompleted || json.Unmarshal(c.entries[c.position].Payload, &completion) != nil || !completion.Cancelled {
			return ErrCorruptJournal
		}
	} else {
		if err := c.next(StepCompleted, json.RawMessage(`{"cancelled":true}`)); err != nil {
			return err
		}
	}
	c.position++
	t.cancelled = true
	return nil
}
