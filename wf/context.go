// Package wf implements the journal-and-replay step protocol.
package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/identity"
)

var ErrNonDeterministic = errors.New("workflow step differs from recorded journal")
var ErrCorruptJournal = errors.New("invalid step protocol in journal")
var ErrSuspended = errors.New("workflow suspended awaiting an external event")
var ErrTimerSchedule = errors.New("timer schedule was not confirmed")
var ErrChildStart = errors.New("child start was not confirmed")
var ErrStepResultNotSerializable = errors.New("step result is not serializable")

type stepResultSerializationError struct{ message string }

func (e *stepResultSerializationError) Error() string { return e.message }
func (e *stepResultSerializationError) Unwrap() error { return ErrStepResultNotSerializable }

type NonDeterministicError struct {
	Index         uint64
	RecordedName  string
	RequestedName string
}

func (e *NonDeterministicError) Error() string {
	return fmt.Sprintf("%v at index %d: recorded %q, requested %q", ErrNonDeterministic, e.Index, e.RecordedName, e.RequestedName)
}
func (e *NonDeterministicError) Unwrap() error { return ErrNonDeterministic }

type Kind string

const (
	StepRequested Kind = "StepRequested"
	StepCompleted Kind = "StepCompleted"
)

type Entry struct {
	Index   uint64
	Kind    Kind
	Payload json.RawMessage
}

type Appender func(ctx context.Context, kind Kind, payload json.RawMessage) error

type Context struct {
	base                      context.Context
	entries                   []Entry
	position                  int
	stepOffset                uint64
	append                    Appender
	replay                    bool
	signals                   []Signal
	usedSignals               map[uint64]bool
	waitingOn                 string
	wakeupAt                  time.Time
	timerNow                  func(context.Context) (time.Time, error)
	scheduleTimer             func(context.Context, uint64, time.Time) error
	timerClock                *TimerClockSupport
	timerFired                func(time.Time, time.Time)
	parentType                string
	parentID                  string
	parentInvSeq              uint64
	startChild                func(context.Context, string, string, []byte, string) error
	childResultValidator      func(context.Context, Signal) error
	storeResult               func(context.Context, []byte) (string, error)
	loadResult                func(context.Context, string) ([]byte, error)
	promiseResults            map[string]*promiseResult
	promiseResultBytes        int
	state                     map[string]json.RawMessage
	timerHandles              map[uint64]*TimerHandle
	checkpointDrainedCursor   uint64
	checkpointSignalCursor    *uint64
	checkpointCancelledTimers map[uint64]bool
	continuationStages        func(string) bool
	continuationAnchor        func(uint64, bool) (ContinuationAnchor, error)
	continuation              *ContinuationCheckpoint
	continuationError         error
}

type Signal struct {
	Sequence uint64
	Name     string
	Payload  []byte
}

// NewContext takes journal step entries in order; the Started entry is omitted.
// The appender must durably CAS each new entry before returning success.
func NewContext(base context.Context, entries []Entry, appendFn Appender, signals ...Signal) *Context {
	return &Context{base: base, entries: entries, append: appendFn, signals: signals, usedSignals: map[uint64]bool{}, state: map[string]json.RawMessage{}}
}

type request struct {
	Kind          string    `json:"kind,omitempty"`
	Name          string    `json:"name"`
	InputHash     string    `json:"input_hash"`
	DurationNanos int64     `json:"duration_nanos,omitempty"`
	FireAt        time.Time `json:"fire_at,omitempty"`
	ClockDomain   string    `json:"clock_domain,omitempty"`
	TimerStep     uint64    `json:"timer_step,omitempty"`
	TimerName     string    `json:"timer_name,omitempty"`
	ChildType     string    `json:"child_type,omitempty"`
	ChildID       string    `json:"child_id,omitempty"`
}
type completion struct {
	Result     json.RawMessage `json:"result,omitempty"`
	ResultRef  string          `json:"result_ref,omitempty"`
	ResultHash string          `json:"result_hash,omitempty"`
	Error      string          `json:"error,omitempty"`
	ErrorKind  string          `json:"error_kind,omitempty"`
	SignalSeq  uint64          `json:"signal_seq,omitempty"`
	Selected   string          `json:"selected,omitempty"`
}

const MaxInlineResult = 900 * 1024

// SetResultStore lets large step results live in Object Store while the journal
// retains a content hash and object name.
func (c *Context) SetResultStore(store func(context.Context, []byte) (string, error), load func(context.Context, string) ([]byte, error)) {
	c.storeResult = store
	c.loadResult = load
}

// stepPosition is the absolute SDK entry position. position remains a cursor
// into the retained suffix; a continuation checkpoint can omit earlier entries
// without changing timer, child or external deduplication identities.
func (c *Context) stepPosition() uint64 { return c.stepOffset + uint64(c.position) }

func (c *Context) next(kind Kind, payload json.RawMessage) error {
	if c.continuationError != nil {
		return c.continuationError
	}
	if c.waitingOn != "" {
		return ErrSuspended
	}
	if c.append == nil {
		return fmt.Errorf("%w: replay reached journal tail", ErrNonDeterministic)
	}
	if err := c.append(c.base, kind, payload); err != nil {
		return err
	}
	c.entries = append(c.entries, Entry{Index: c.stepOffset + uint64(len(c.entries)) + 1, Kind: kind, Payload: payload})
	return nil
}

// Run records a request before the effect and its outcome afterward. Effects
// must tolerate retries if a worker dies after execution but before completion.
func Run[T any](c *Context, name string, input any, fn func(context.Context) (T, error)) (T, error) {
	return runWithKind(c, "run", name, input, fn)
}

func runWithKind[T any](c *Context, kind, name string, input any, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	inputBytes, err := json.Marshal(input)
	if err != nil {
		return zero, fmt.Errorf("step input: %w", err)
	}
	hash := sha256.Sum256(inputBytes)
	want := request{Kind: kind, Name: name, InputHash: hex.EncodeToString(hash[:])}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested {
			return zero, ErrCorruptJournal
		}
		var got request
		if err := json.Unmarshal(recorded.Payload, &got); err != nil {
			return zero, ErrCorruptJournal
		}
		if (got.Kind != kind && !(kind == "run" && got.Kind == "")) || got.Name != name || got.InputHash != want.InputHash {
			return zero, &NonDeterministicError{Index: recorded.Index, RecordedName: got.Name, RequestedName: name}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return zero, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepCompleted {
			return zero, ErrCorruptJournal
		}
		var done completion
		if err := json.Unmarshal(recorded.Payload, &done); err != nil {
			return zero, ErrCorruptJournal
		}
		c.position++
		if done.ErrorKind != "" && (done.ErrorKind != "result_not_serializable" || done.Error == "" || len(done.Result) != 0 || done.ResultRef != "") {
			return zero, ErrCorruptJournal
		}
		if done.Error != "" {
			if done.ErrorKind == "result_not_serializable" {
				return zero, &stepResultSerializationError{message: done.Error}
			}
			return zero, errors.New(done.Error)
		}
		if done.ResultRef != "" {
			if c.loadResult == nil || done.ResultHash == "" || len(done.Result) != 0 {
				return zero, ErrCorruptJournal
			}
			data, err := c.loadResult(c.base, done.ResultRef)
			if err != nil {
				return zero, err
			}
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) != done.ResultHash {
				return zero, ErrCorruptJournal
			}
			done.Result = data
		}
		var value T
		if err := json.Unmarshal(done.Result, &value); err != nil {
			return zero, ErrCorruptJournal
		}
		return value, nil
	}
	if c.replay {
		return zero, ErrReplayPendingStep
	}
	if err := c.base.Err(); err != nil {
		return zero, err
	}
	value, effectErr := runEffect(c.base, fn)
	if err := c.base.Err(); err != nil {
		return zero, err
	}
	done := completion{}
	var serializationErr error
	if effectErr != nil {
		done.Error = effectErr.Error()
	} else {
		result, err := json.Marshal(value)
		if err != nil {
			serializationErr = &stepResultSerializationError{message: fmt.Sprintf("%s: %v", ErrStepResultNotSerializable, err)}
			done.Error = serializationErr.Error()
			done.ErrorKind = "result_not_serializable"
		} else if len(result) > MaxInlineResult {
			if c.storeResult == nil {
				return zero, fmt.Errorf("step result exceeds inline limit without Object Store")
			}
			ref, err := c.storeResult(c.base, result)
			if err != nil {
				return zero, err
			}
			if ref == "" {
				return zero, fmt.Errorf("empty step result object key")
			}
			digest := sha256.Sum256(result)
			done.ResultRef = ref
			done.ResultHash = hex.EncodeToString(digest[:])
		} else {
			done.Result = result
		}
	}
	payload, _ := json.Marshal(done)
	if err := c.next(StepCompleted, payload); err != nil {
		return zero, err
	}
	c.position++
	if serializationErr != nil {
		return zero, serializationErr
	}
	return value, effectErr
}

// runEffect keeps a stalled or Goexit-terminated effect from trapping the
// worker's journal goroutine. A function that ignores cancellation may keep
// running; its abandoned result cannot be appended by this invocation.
func runEffect[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		returned := false
		defer func() {
			if p := recover(); p != nil {
				done <- result{err: fmt.Errorf("step panic: %v", p)}
			} else if !returned {
				done <- result{err: fmt.Errorf("step exited via runtime.Goexit")}
			}
		}()
		value, err := fn(ctx)
		returned = true
		done <- result{value: value, err: err}
	}()
	select {
	case outcome := <-done:
		return outcome.value, outcome.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (c *Context) CheckComplete() error {
	if c.continuationError != nil {
		return c.continuationError
	}
	if c.continuation != nil {
		return ErrContinuation
	}
	if c.position != len(c.entries) {
		return fmt.Errorf("%w: %d unplayed entries", ErrNonDeterministic, len(c.entries)-c.position)
	}
	return nil
}

func (c *Context) Context() context.Context { return c.base }

func (c *Context) WaitingOn() string { return c.waitingOn }

// SetTimerSupport supplies the server timestamp of the current wakeup and a
// durable schedule publisher. The worker sets this before running user code.
func (c *Context) SetTimerSupport(wakeupAt time.Time, now func(context.Context) (time.Time, error), schedule func(context.Context, uint64, time.Time) error) {
	c.wakeupAt = wakeupAt
	c.timerNow = now
	c.scheduleTimer = schedule
}

// SetTimerObserver reports a timer only when its completion is newly journaled.
func (c *Context) SetTimerObserver(observer func(fireAt, wakeupAt time.Time)) {
	c.timerFired = observer
}

func (c *Context) SetChildSupport(parentType, parentID string, parentInvSeq uint64, start func(context.Context, string, string, []byte, string) error) {
	c.parentType = parentType
	c.parentID = parentID
	c.parentInvSeq = parentInvSeq
	c.startChild = start
}

// SetChildResultValidator lets the runtime require provenance before a child
// completion is accepted. It validates the exact selected signal on new and
// replayed calls. Nil preserves the ordinary signal-backed child behavior.
func (c *Context) SetChildResultValidator(validate func(context.Context, Signal) error) {
	c.childResultValidator = validate
}
func (c *Context) validateChildResult(signal Signal) error {
	if c.childResultValidator != nil {
		return c.childResultValidator(c.base, signal)
	}
	return nil
}

func (c *Context) childID(childType string, step uint64) string {
	material := c.parentType + ":" + c.parentID + ":" + strconv.FormatUint(c.parentInvSeq, 10) + ":" + childType + ":" + strconv.FormatUint(step, 10)
	hash := sha256.Sum256([]byte(material))
	return "c-" + hex.EncodeToString(hash[:16])
}

// Call starts a child with an ID derived from the parent and the logical step
// position. It suspends until the child's terminal result arrives as a signal.
func Call(c *Context, childType string, input []byte) ([]byte, error) {
	if err := identity.ValidateToken(childType); err != nil {
		return nil, err
	}
	if c.parentType == "" || c.parentID == "" || c.parentInvSeq == 0 || c.startChild == nil {
		return nil, fmt.Errorf("child support is not configured")
	}
	step := c.stepPosition()
	childID := c.childID(childType, step)
	signalName := "child_" + strconv.FormatUint(step, 10)
	inputDigest := sha256.Sum256(input)
	want := request{Kind: "call", Name: signalName, InputHash: hex.EncodeToString(inputDigest[:]), ChildType: childType, ChildID: childID}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested {
			return nil, ErrCorruptJournal
		}
		var got request
		if err := json.Unmarshal(recorded.Payload, &got); err != nil {
			return nil, ErrCorruptJournal
		}
		if got.Kind != "call" || got.Name != want.Name || got.InputHash != want.InputHash || got.ChildType != childType || got.ChildID != childID {
			return nil, &NonDeterministicError{Index: recorded.Index, RecordedName: got.ChildType, RequestedName: childType}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return nil, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepCompleted {
			return nil, ErrCorruptJournal
		}
		var done completion
		if err := json.Unmarshal(recorded.Payload, &done); err != nil || done.SignalSeq == 0 {
			return nil, ErrCorruptJournal
		}
		if c.usedSignals[done.SignalSeq] {
			return nil, ErrCorruptJournal
		}
		c.usedSignals[done.SignalSeq] = true
		c.position++
		for _, sig := range c.signals {
			if sig.Sequence == done.SignalSeq && sig.Name == signalName {
				if err := c.validateChildResult(sig); err != nil {
					return nil, err
				}
				var out Outcome
				if err := json.Unmarshal(sig.Payload, &out); err != nil {
					return nil, ErrCorruptJournal
				}
				if out.Error != "" {
					return nil, errors.New(out.Error)
				}
				return out.ResultBytes(c.base, c.loadResult)
			}
		}
		return nil, ErrCorruptJournal
	}
	for _, sig := range c.signals {
		if sig.Name != signalName || c.usedSignals[sig.Sequence] {
			continue
		}
		if err := c.validateChildResult(sig); err != nil {
			return nil, err
		}
		var out Outcome
		if err := json.Unmarshal(sig.Payload, &out); err != nil {
			return nil, ErrCorruptJournal
		}
		payload, _ := json.Marshal(completion{SignalSeq: sig.Sequence})
		if err := c.next(StepCompleted, payload); err != nil {
			return nil, err
		}
		c.usedSignals[sig.Sequence] = true
		c.position++
		if out.Error != "" {
			return nil, errors.New(out.Error)
		}
		return out.ResultBytes(c.base, c.loadResult)
	}
	if err := c.startChild(c.base, childType, childID, input, signalName); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrChildStart, err)
	}
	c.waitingOn = "child:" + childID
	return nil, ErrSuspended
}

type Promise struct {
	ChildType  string
	ChildID    string
	SignalName string
}

// CallAsync durably starts a child and returns an awaitable promise. The
// journal completion means StartChild was acknowledged or already existed.
func CallAsync(c *Context, childType string, input []byte) (Promise, error) {
	var empty Promise
	if err := identity.ValidateToken(childType); err != nil {
		return empty, err
	}
	if c.parentType == "" || c.parentID == "" || c.parentInvSeq == 0 || c.startChild == nil {
		return empty, fmt.Errorf("child support is not configured")
	}
	step := c.stepPosition()
	p := Promise{ChildType: childType, ChildID: c.childID(childType, step), SignalName: "child_" + strconv.FormatUint(step, 10)}
	inputDigest := sha256.Sum256(input)
	want := request{Kind: "call_async", Name: p.SignalName, InputHash: hex.EncodeToString(inputDigest[:]), ChildType: childType, ChildID: p.ChildID}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested {
			return empty, ErrCorruptJournal
		}
		var got request
		if err := json.Unmarshal(recorded.Payload, &got); err != nil {
			return empty, ErrCorruptJournal
		}
		if got.Kind != "call_async" || got.Name != want.Name || got.InputHash != want.InputHash || got.ChildType != childType || got.ChildID != p.ChildID {
			return empty, &NonDeterministicError{Index: recorded.Index, RecordedName: got.ChildType, RequestedName: childType}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return empty, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		if c.entries[c.position].Kind != StepCompleted {
			return empty, ErrCorruptJournal
		}
		c.position++
		return p, nil
	}
	if err := c.startChild(c.base, childType, p.ChildID, input, p.SignalName); err != nil {
		return empty, fmt.Errorf("%w: %v", ErrChildStart, err)
	}
	if err := c.next(StepCompleted, json.RawMessage(`{}`)); err != nil {
		return empty, err
	}
	c.position++
	return p, nil
}

const maxCachedPromiseResultBytes = 16 << 20

type promiseResult struct {
	payload []byte
	cached  bool
	result  []byte
}

// AwaitPromise consumes the child's result signal once per replay. Repeated
// awaits resolve the same immutable outcome without adding steps or waiting for
// a second signal. Returned slices are detached. Result caching is bounded to
// sixteen MiB per context; uncached object results are reread and hash-verified.
// Transient object reads can be retried without consuming the signal again.
func AwaitPromise(c *Context, p Promise) ([]byte, error) {
	if err := identity.ValidateToken(p.SignalName); err != nil {
		return nil, err
	}
	if c.promiseResults == nil {
		c.promiseResults = make(map[string]*promiseResult)
	}
	result := c.promiseResults[p.SignalName]
	if result == nil {
		data, err := awaitSignal(c, p.SignalName, true)
		if err != nil {
			return nil, err
		}
		result = &promiseResult{payload: data}
		c.promiseResults[p.SignalName] = result
	}
	if result.cached {
		return bytes.Clone(result.result), nil
	}
	var out Outcome
	if json.Unmarshal(result.payload, &out) != nil {
		return nil, ErrCorruptJournal
	}
	if out.Error != "" {
		return nil, errors.New(out.Error)
	}
	data, err := out.ResultBytes(c.base, c.loadResult)
	if err != nil {
		return nil, err
	}
	if len(data) <= maxCachedPromiseResultBytes-c.promiseResultBytes {
		result.result = bytes.Clone(data)
		result.cached = true
		c.promiseResultBytes += len(data)
	}
	return bytes.Clone(data), nil
}

// Version records a change decision once. New invocations choose max; replay
// returns the recorded value so a deployment can keep an old branch alive.
func Version(c *Context, changeID string, min, max int) (int, error) {
	if changeID == "" || min > max {
		return 0, fmt.Errorf("invalid version range or change ID")
	}
	want := request{Kind: "version", Name: changeID}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested {
			return 0, ErrCorruptJournal
		}
		var got request
		if err := json.Unmarshal(recorded.Payload, &got); err != nil {
			return 0, ErrCorruptJournal
		}
		if got.Kind != "version" || got.Name != changeID {
			return 0, &NonDeterministicError{Index: recorded.Index, RecordedName: got.Name, RequestedName: changeID}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return 0, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepCompleted {
			return 0, ErrCorruptJournal
		}
		var done completion
		if err := json.Unmarshal(recorded.Payload, &done); err != nil {
			return 0, ErrCorruptJournal
		}
		var v int
		if err := json.Unmarshal(done.Result, &v); err != nil {
			return 0, ErrCorruptJournal
		}
		if v < min || v > max {
			return 0, &NonDeterministicError{Index: recorded.Index, RecordedName: fmt.Sprint(v), RequestedName: fmt.Sprintf("%d..%d", min, max)}
		}
		c.position++
		return v, nil
	}
	value, _ := json.Marshal(max)
	payload, _ := json.Marshal(completion{Result: value})
	if err := c.next(StepCompleted, payload); err != nil {
		return 0, err
	}
	c.position++
	return max, nil
}

// Sleep journals a timer request before scheduling its wakeup. A suspended
// invocation resumes on a later run message and completes when its configured
// clock proves the recorded fire time due. Legacy timers use the
// delivery timestamp; tagged timers use their clock domain's lower bound.
func Sleep(c *Context, name string, d time.Duration) error {
	if name == "" {
		return fmt.Errorf("empty timer name")
	}
	step := c.stepPosition()
	var req request
	// This delivery's wakeup predates a fresh request. Different leader clocks
	// can make that old timestamp exceed the new deadline without any wait.
	fresh := c.position >= len(c.entries)
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested {
			return ErrCorruptJournal
		}
		if err := json.Unmarshal(recorded.Payload, &req); err != nil {
			return ErrCorruptJournal
		}
		if req.Kind != "timer" || req.Name != name || req.DurationNanos != int64(d) {
			return &NonDeterministicError{Index: recorded.Index, RecordedName: req.Name, RequestedName: name}
		}
	} else {
		req = request{Kind: "timer", Name: name, DurationNanos: int64(d)}
		if d > 0 {
			serverNow, domain, err := c.timerOrigin()
			if err != nil {
				return fmt.Errorf("%w: %w", ErrTimerSchedule, err)
			}
			req.FireAt = serverNow.Add(d).UTC()
			req.ClockDomain = domain
		}
		payload, _ := json.Marshal(req)
		if err := c.next(StepRequested, payload); err != nil {
			return err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		if c.entries[c.position].Kind != StepCompleted {
			return ErrCorruptJournal
		}
		c.position++
		return nil
	}
	ready, observed, err := c.timerReady(req.ClockDomain, req.FireAt, fresh)
	if err != nil {
		return err
	}
	if d <= 0 || ready {
		if err := c.next(StepCompleted, json.RawMessage(`{}`)); err != nil {
			return err
		}
		if d > 0 && c.timerFired != nil {
			c.timerFired(req.FireAt, observed)
		}
		c.position++
		return nil
	}
	if err := c.scheduleInDomain(step, req.FireAt, req.ClockDomain); err != nil {
		return fmt.Errorf("%w: %w", ErrTimerSchedule, err)
	}
	c.waitingOn = "timer:" + name
	return ErrSuspended
}

// AwaitSignal returns the oldest unconsumed signal with this name. If none is
// available, it records the request and asks the worker to suspend. Arrivals
// are journaled separately by the worker before user code is replayed.
func AwaitSignal(c *Context, name string) ([]byte, error) {
	return awaitSignal(c, name, false)
}
func awaitSignal(c *Context, name string, child bool) ([]byte, error) {
	if err := identity.ValidateToken(name); err != nil {
		return nil, err
	}
	want := request{Kind: "signal", Name: name}
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepRequested {
			return nil, ErrCorruptJournal
		}
		var got request
		if err := json.Unmarshal(recorded.Payload, &got); err != nil {
			return nil, ErrCorruptJournal
		}
		if got.Kind != "signal" || got.Name != name {
			return nil, &NonDeterministicError{Index: recorded.Index, RecordedName: got.Name, RequestedName: name}
		}
	} else {
		payload, _ := json.Marshal(want)
		if err := c.next(StepRequested, payload); err != nil {
			return nil, err
		}
	}
	c.position++
	if c.position < len(c.entries) {
		recorded := c.entries[c.position]
		if recorded.Kind != StepCompleted {
			return nil, ErrCorruptJournal
		}
		var done completion
		if err := json.Unmarshal(recorded.Payload, &done); err != nil || done.SignalSeq == 0 {
			return nil, ErrCorruptJournal
		}
		if c.usedSignals[done.SignalSeq] {
			return nil, ErrCorruptJournal
		}
		c.usedSignals[done.SignalSeq] = true
		c.position++
		for _, sig := range c.signals {
			if sig.Sequence == done.SignalSeq && sig.Name == name {
				if child {
					if err := c.validateChildResult(sig); err != nil {
						return nil, err
					}
				}
				return sig.Payload, nil
			}
		}
		return nil, ErrCorruptJournal
	}
	for _, sig := range c.signals {
		if sig.Name != name || c.usedSignals[sig.Sequence] {
			continue
		}
		if child {
			if err := c.validateChildResult(sig); err != nil {
				return nil, err
			}
		}
		payload, _ := json.Marshal(completion{SignalSeq: sig.Sequence})
		if err := c.next(StepCompleted, payload); err != nil {
			return nil, err
		}
		c.usedSignals[sig.Sequence] = true
		c.position++
		return sig.Payload, nil
	}
	c.waitingOn = "signal:" + name
	return nil, ErrSuspended
}
