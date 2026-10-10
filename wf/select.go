package wf

import (
	"encoding/json"

	"js-wf/identity"
	"js-wf/internal/stepwire"
)

type Selection string

const (
	TimerSelected  Selection = "timer"
	SignalSelected Selection = "signal"
)

// SelectSignal waits for either this durable timer or the named signal. A
// buffered signal wins when both are ready. The chosen branch and signal
// sequence are journaled, so replay makes the same choice after a crash.
func (t *TimerHandle) SelectSignal(name string) (Selection, []byte, error) {
	if err := identity.ValidateToken(name); err != nil {
		return "", nil, err
	}
	if t.cancelled {
		return "", nil, ErrTimerCancelled
	}
	if t.fired {
		return "", nil, ErrTimerFired
	}
	c := t.c
	want := request{Kind: "timer_signal_select", Name: name, TimerName: t.name, TimerStep: t.step, FireAt: t.fireAt, ClockDomain: t.clockDomain}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		var got request
		if recorded.Kind != StepRequested || stepwire.Decode(recorded.Payload, &got) != nil {
			return "", nil, ErrCorruptJournal
		}
		if got.Kind != want.Kind || got.Name != name || got.TimerName != t.name || got.TimerStep != t.step || !got.FireAt.Equal(t.fireAt) || got.ClockDomain != want.ClockDomain {
			return "", nil, &NonDeterministicError{Index: recorded.Index, RecordedName: got.Name, RequestedName: name}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return "", nil, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepCompleted {
			return "", nil, ErrCorruptJournal
		}
		var done completion
		if stepwire.DecodeCompletion(recorded.Payload, &done) != nil {
			return "", nil, ErrCorruptJournal
		}
		c.position++
		switch Selection(done.Selected) {
		case TimerSelected:
			if done.SignalSeq != 0 {
				return "", nil, ErrCorruptJournal
			}
			t.fired = true
			return TimerSelected, nil, nil
		case SignalSelected:
			if done.SignalSeq == 0 || c.usedSignals[done.SignalSeq] {
				return "", nil, ErrCorruptJournal
			}
			for _, sig := range c.signals {
				if sig.Sequence == done.SignalSeq && sig.Name == name {
					c.usedSignals[done.SignalSeq] = true
					return SignalSelected, sig.Payload, nil
				}
			}
			return "", nil, ErrCorruptJournal
		default:
			return "", nil, ErrCorruptJournal
		}
	}
	for _, sig := range c.signals {
		if sig.Name != name || c.usedSignals[sig.Sequence] {
			continue
		}
		payload, _ := json.Marshal(completion{Selected: string(SignalSelected), SignalSeq: sig.Sequence})
		if err := c.next(StepCompleted, payload); err != nil {
			return "", nil, err
		}
		c.position++
		c.usedSignals[sig.Sequence] = true
		return SignalSelected, sig.Payload, nil
	}
	ready, observed, err := c.timerReady(t.clockDomain, t.fireAt, t.createdInDelivery)
	if err != nil {
		return "", nil, err
	}
	if ready {
		payload, _ := json.Marshal(completion{Selected: string(TimerSelected)})
		if err := c.next(StepCompleted, payload); err != nil {
			return "", nil, err
		}
		c.position++
		t.fired = true
		if !t.fireAt.IsZero() && c.timerFired != nil {
			c.timerFired(t.fireAt, observed)
		}
		return TimerSelected, nil, nil
	}
	c.waitingOn = "select:" + t.name + ":" + name
	return "", nil, ErrSuspended
}
