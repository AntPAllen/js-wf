package wf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"

	"js-wf/internal/checkpoint"
)

var ErrCheckpointBoundary = errors.New("workflow is not at a checkpoint boundary")
var ErrInvalidCheckpoint = checkpoint.ErrInvalid

// CheckpointLocation binds stored bytes to the manifest's invocation and
// completion anchor. The worker must separately verify the actual journal
// anchor and stage registration before invoking any continuation.
type CheckpointLocation struct {
	Type   string
	ID     string
	InvSeq uint64
	Index  uint64
	Epoch  uint64
	Hash   string
}

// CheckpointInfo carries dispatch and runtime facts omitted from suffix replay.
// Frame storage, manifest CAS and continuation dispatch remain worker work.
type CheckpointInfo struct {
	Stage           string
	Data            json.RawMessage
	StepPosition    uint64
	PanicAttempts   uint64
	CancelledTimers []uint64
}

// CaptureCheckpoint serializes a frame for a prospective checkpoint pair at
// index/epoch. It never appends or mutates the context. The frame's SDK position
// includes the two entries that the worker must durably publish before use.
// A successful capture alone is not a committed checkpoint.
func (c *Context) CaptureCheckpoint(stage string, data any, index, epoch, panicAttempts uint64) ([]byte, string, error) {
	if c.waitingOn != "" || c.position < 0 || c.position > len(c.entries) || c.position%2 != 0 || c.stepOffset > math.MaxUint64-uint64(c.position)-2 {
		return nil, "", ErrCheckpointBoundary
	}
	cancelled := make(map[uint64]bool, len(c.checkpointCancelledTimers))
	for step, yes := range c.checkpointCancelledTimers {
		if yes {
			cancelled[step] = true
		}
	}
	for step, handle := range c.timerHandles {
		if !handle.cancelled && !handle.fired {
			return nil, "", fmt.Errorf("%w: timer %q is still live", ErrCheckpointBoundary, handle.name)
		}
		if handle.cancelled {
			cancelled[step] = true
		}
	}
	// Context and handles otherwise encode as empty objects. Reject them even
	// when nested in user locals instead of losing the execution capability.
	if err := checkContinuationData(reflect.ValueOf(data), make(map[dataVisit]bool)); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, "", fmt.Errorf("%w: continuation data: %v", ErrInvalidCheckpoint, err)
	}
	frame := checkpoint.Frame{
		Version:  checkpoint.Version,
		Identity: checkpoint.Identity{Type: c.parentType, ID: c.parentID, InvSeq: c.parentInvSeq},
		Stage:    stage, Data: raw, Anchor: checkpoint.Anchor{Index: index, Epoch: epoch},
		StepPosition: c.stepPosition() + 2, PanicAttempts: panicAttempts,
		State:           make(map[string]json.RawMessage, len(c.state)),
		PromiseOutcomes: make(map[string]json.RawMessage, len(c.promiseResults)),
	}
	for key, value := range c.state {
		frame.State[key] = bytes.Clone(value)
	}
	for name, result := range c.promiseResults {
		if result == nil {
			return nil, "", fmt.Errorf("%w: missing promise outcome", ErrInvalidCheckpoint)
		}
		frame.PromiseOutcomes[name] = bytes.Clone(result.payload)
	}
	for seq, used := range c.usedSignals {
		if used {
			frame.ConsumedSignals = append(frame.ConsumedSignals, seq)
		}
	}
	for step := range cancelled {
		frame.CancelledTimers = append(frame.CancelledTimers, step)
	}
	sort.Slice(frame.ConsumedSignals, func(i, j int) bool { return frame.ConsumedSignals[i] < frame.ConsumedSignals[j] })
	sort.Slice(frame.CancelledTimers, func(i, j int) bool { return frame.CancelledTimers[i] < frame.CancelledTimers[j] })
	return checkpoint.Encode(frame)
}

// NewCheckpointContext verifies and restores frame bytes without replaying any
// prefix. entries must contain only SDK entries strictly after the confirmed
// anchor. This checks SDK suffix structure; the journal reader remains
// responsible for contiguous logical journal indices and terminal fencing.
// Child/timer/blob services must be configured by the worker before execution.
func NewCheckpointContext(base context.Context, entries []Entry, appendFn Appender, raw []byte, location CheckpointLocation, signals ...Signal) (*Context, CheckpointInfo, error) {
	frame, err := checkpoint.Decode(raw, location.Hash, checkpoint.Identity{Type: location.Type, ID: location.ID, InvSeq: location.InvSeq}, checkpoint.Anchor{Index: location.Index, Epoch: location.Epoch})
	if err != nil {
		return nil, CheckpointInfo{}, err
	}
	if uint64(len(entries)) > math.MaxUint64-frame.StepPosition {
		return nil, CheckpointInfo{}, fmt.Errorf("%w: SDK position overflow", ErrInvalidCheckpoint)
	}
	previous := location.Index
	detached := make([]Entry, len(entries))
	for i, entry := range entries {
		expected := StepRequested
		if i%2 != 0 {
			expected = StepCompleted
		}
		if entry.Index <= previous || entry.Kind != expected || !json.Valid(entry.Payload) {
			return nil, CheckpointInfo{}, fmt.Errorf("%w: invalid SDK suffix", ErrInvalidCheckpoint)
		}
		previous = entry.Index
		detached[i] = entry
		detached[i].Payload = bytes.Clone(entry.Payload)
	}
	c := NewContext(base, detached, appendFn, signals...)
	c.stepOffset = frame.StepPosition
	c.parentType = location.Type
	c.parentID = location.ID
	c.parentInvSeq = location.InvSeq
	c.state = frame.State
	c.promiseResults = make(map[string]*promiseResult, len(frame.PromiseOutcomes))
	for name, payload := range frame.PromiseOutcomes {
		c.promiseResults[name] = &promiseResult{payload: payload}
	}
	for _, seq := range frame.ConsumedSignals {
		c.usedSignals[seq] = true
	}
	c.checkpointCancelledTimers = make(map[uint64]bool, len(frame.CancelledTimers))
	for _, step := range frame.CancelledTimers {
		c.checkpointCancelledTimers[step] = true
	}
	return c, CheckpointInfo{Stage: frame.Stage, Data: frame.Data, StepPosition: frame.StepPosition, PanicAttempts: frame.PanicAttempts, CancelledTimers: append([]uint64(nil), frame.CancelledTimers...)}, nil
}

type dataVisit struct {
	typ     reflect.Type
	pointer uintptr
	length  int
}

var contextInterface = reflect.TypeFor[context.Context]()
var workflowContextType = reflect.TypeFor[Context]()
var timerHandleType = reflect.TypeFor[TimerHandle]()

func checkContinuationData(v reflect.Value, seen map[dataVisit]bool) error {
	if !v.IsValid() {
		return nil
	}
	typ := v.Type()
	if typ.Implements(contextInterface) || typ == workflowContextType || typ == timerHandleType || typ.Kind() == reflect.Pointer && (typ.Elem() == workflowContextType || typ.Elem() == timerHandleType) {
		return fmt.Errorf("%w: continuation data contains runtime context or timer handle", ErrInvalidCheckpoint)
	}
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			return checkContinuationData(v.Elem(), seen)
		}
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return nil
		}
		visit := dataVisit{typ: typ, pointer: v.Pointer()}
		if v.Kind() == reflect.Slice {
			visit.length = v.Len()
		}
		if seen[visit] {
			return nil
		}
		seen[visit] = true
		if v.Kind() == reflect.Pointer {
			return checkContinuationData(v.Elem(), seen)
		}
		if v.Kind() == reflect.Map {
			iter := v.MapRange()
			for iter.Next() {
				if err := checkContinuationData(iter.Key(), seen); err != nil {
					return err
				}
				if err := checkContinuationData(iter.Value(), seen); err != nil {
					return err
				}
			}
		} else {
			for i := 0; i < v.Len(); i++ {
				if err := checkContinuationData(v.Index(i), seen); err != nil {
					return err
				}
			}
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if err := checkContinuationData(v.Index(i), seen); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := typ.Field(i)
			if field.PkgPath != "" || field.Tag.Get("json") == "-" {
				continue
			}
			if err := checkContinuationData(v.Field(i), seen); err != nil {
				return err
			}
		}
	}
	return nil
}
