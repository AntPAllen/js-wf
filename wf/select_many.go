package wf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"js-wf/identity"
)

// Awaitable is a durable timer, named signal, or child promise usable in Select.
// Handles must be reconstructed at the same positions on every workflow replay.
type Awaitable interface {
	selectionCase(*Context) (selectCase, error)
}

// SignalAwaitable selects the next unconsumed signal with this name.
type SignalAwaitable string

type selectCase struct {
	Kind      string    `json:"kind"`
	Name      string    `json:"name"`
	TimerStep uint64    `json:"timer_step,omitempty"`
	FireAt    time.Time `json:"fire_at,omitempty"`
	ChildType string    `json:"child_type,omitempty"`
	ChildID   string    `json:"child_id,omitempty"`
}

func (s SignalAwaitable) selectionCase(*Context) (selectCase, error) {
	name := string(s)
	if err := identity.ValidateToken(name); err != nil {
		return selectCase{}, err
	}
	return selectCase{Kind: "signal", Name: name}, nil
}
func (p Promise) selectionCase(*Context) (selectCase, error) {
	if p.ChildType != "" || p.ChildID != "" {
		if err := identity.Validate(p.ChildType, p.ChildID); err != nil {
			return selectCase{}, err
		}
	}
	if err := identity.ValidateToken(p.SignalName); err != nil {
		return selectCase{}, err
	}
	return selectCase{Kind: "promise", Name: p.SignalName, ChildType: p.ChildType, ChildID: p.ChildID}, nil
}
func (t *TimerHandle) selectionCase(c *Context) (selectCase, error) {
	if t == nil || t.c != c {
		return selectCase{}, fmt.Errorf("timer belongs to another workflow replay")
	}
	if t.cancelled {
		return selectCase{}, ErrTimerCancelled
	}
	if t.fired {
		return selectCase{}, ErrTimerFired
	}
	return selectCase{Kind: "timer", Name: t.name, TimerStep: t.step, FireAt: t.fireAt}, nil
}

type selectRequest struct {
	Kind  string       `json:"kind"`
	Cases []selectCase `json:"cases"`
}
type selectCompletion struct {
	CaseIndex *int   `json:"case_index"`
	SignalSeq uint64 `json:"signal_seq,omitempty"`
}

// Select waits for any combination of SDK awaitables. The first ready case in
// argument order wins; its index and signal sequence are journaled for replay.
// Timer results are nil, signal results are payload bytes, and promise results
// are the decoded child result (or child error). Losers remain available.
// No ready case returns index -1 and ErrSuspended. Propagate that suspension.
func Select(c *Context, awaitables ...Awaitable) (int, []byte, error) {
	if len(awaitables) == 0 {
		return -1, nil, fmt.Errorf("select requires at least one awaitable")
	}
	want := selectRequest{Kind: "select_many", Cases: make([]selectCase, len(awaitables))}
	for i, a := range awaitables {
		if a == nil {
			return -1, nil, fmt.Errorf("nil select awaitable")
		}
		if p, ok := a.(*Promise); ok && p == nil {
			return -1, nil, fmt.Errorf("nil select promise")
		}
		descriptor, err := a.selectionCase(c)
		if err != nil {
			return -1, nil, err
		}
		want.Cases[i] = descriptor
	}
	payload, _ := json.Marshal(want)
	if c.position < len(c.entries) {
		record := c.entries[c.position]
		var got selectRequest
		if record.Kind != StepRequested || json.Unmarshal(record.Payload, &got) != nil {
			return -1, nil, ErrCorruptJournal
		}
		encoded, _ := json.Marshal(got)
		if !bytes.Equal(payload, encoded) {
			return -1, nil, &NonDeterministicError{Index: record.Index, RecordedName: "select_many", RequestedName: "select_many"}
		}
	} else if err := c.next(StepRequested, payload); err != nil {
		return -1, nil, err
	}
	c.position++
	if c.position < len(c.entries) {
		record := c.entries[c.position]
		var done selectCompletion
		if record.Kind != StepCompleted || json.Unmarshal(record.Payload, &done) != nil || done.CaseIndex == nil || *done.CaseIndex < 0 || *done.CaseIndex >= len(awaitables) {
			return -1, nil, ErrCorruptJournal
		}
		c.position++
		return completeSelection(c, awaitables, want.Cases, *done.CaseIndex, done.SignalSeq, false)
	}
	for i, descriptor := range want.Cases {
		var sequence uint64
		ready := false
		switch descriptor.Kind {
		case "timer":
			ready = descriptor.FireAt.IsZero() || !c.wakeupAt.IsZero() && !c.wakeupAt.Before(descriptor.FireAt)
		case "promise":
			ready = c.promiseResults[descriptor.Name] != nil
			if ready {
				break
			}
			fallthrough
		case "signal":
			for _, signal := range c.signals {
				if signal.Name == descriptor.Name && !c.usedSignals[signal.Sequence] {
					if signal.Sequence == 0 {
						return -1, nil, ErrCorruptJournal
					}
					sequence = signal.Sequence
					ready = true
					break
				}
			}
		}
		if !ready {
			continue
		}
		done, _ := json.Marshal(selectCompletion{CaseIndex: &i, SignalSeq: sequence})
		if err := c.next(StepCompleted, done); err != nil {
			return -1, nil, err
		}
		c.position++
		return completeSelection(c, awaitables, want.Cases, i, sequence, true)
	}
	c.waitingOn = "select_many"
	return -1, nil, ErrSuspended
}

func completeSelection(c *Context, awaitables []Awaitable, cases []selectCase, index int, sequence uint64, fresh bool) (int, []byte, error) {
	descriptor := cases[index]
	if descriptor.Kind == "timer" {
		if sequence != 0 {
			return index, nil, ErrCorruptJournal
		}
		timer := awaitables[index].(*TimerHandle)
		timer.fired = true
		if fresh && !timer.fireAt.IsZero() && c.timerFired != nil {
			c.timerFired(timer.fireAt, c.wakeupAt)
		}
		return index, nil, nil
	}
	var data []byte
	if sequence == 0 {
		if descriptor.Kind != "promise" || c.promiseResults[descriptor.Name] == nil {
			return index, nil, ErrCorruptJournal
		}
	} else {
		if c.usedSignals[sequence] {
			return index, nil, ErrCorruptJournal
		}
		found := false
		for _, signal := range c.signals {
			if signal.Sequence == sequence && signal.Name == descriptor.Name {
				data = signal.Payload
				found = true
				break
			}
		}
		if !found {
			return index, nil, ErrCorruptJournal
		}
		c.usedSignals[sequence] = true
	}
	if descriptor.Kind == "signal" {
		return index, bytes.Clone(data), nil
	}
	if descriptor.Kind != "promise" {
		return index, nil, ErrCorruptJournal
	}
	if sequence != 0 {
		if c.promiseResults == nil {
			c.promiseResults = make(map[string]*promiseResult)
		}
		c.promiseResults[descriptor.Name] = &promiseResult{payload: data}
	}
	result, err := AwaitPromise(c, Promise{SignalName: descriptor.Name})
	return index, result, err
}
