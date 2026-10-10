package wf

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"js-wf/identity"
	"js-wf/internal/stepwire"
)

var ErrContinuation = errors.New("workflow ends delivery at a continuation checkpoint")
var ErrContinuationUnsupported = errors.New("continuation support is not configured")
var ErrUnknownContinuation = errors.New("continuation stage is not registered")
var ErrCheckpointStore = errors.New("checkpoint storage or publication was not confirmed")

// ContinuationAnchor comes from the worker's journal/lease facts. For an
// existing completion the epoch and panic count belong to that recorded anchor,
// not the replacement worker's lease or a later handler attempt. SignalCursor
// is the highest SignalConsumed stream sequence through that anchor; later
// drained signals must not alter a recorded frame.
type ContinuationAnchor struct {
	Index, Epoch, PanicAttempts uint64
	SignalCursor                uint64
}

// ContinuationCheckpoint describes the completed pair the worker must publish
// into a runtime manifest before suspending and handing off. It is not proof
// that a manifest, purge or wakeup has been committed.
type ContinuationCheckpoint struct {
	ContinuationAnchor
	Stage        string
	Object       string
	SHA256       string
	StepPosition uint64
}

// SetContinuationSupport supplies stage registration and actual journal facts.
// completedIndex is nonzero on replay of a completion; otherwise the callback
// supplies the prospective completion index/epoch. requestRecorded distinguishes
// a pending recorded request from a pair whose request still needs publication.
func (c *Context) SetContinuationSupport(registered func(string) bool, anchor func(completedIndex uint64, requestRecorded bool) (ContinuationAnchor, error)) {
	c.continuationStages = registered
	c.continuationAnchor = anchor
}

// Continuation returns detached metadata for the worker, after a verified
// request/completion pair. No continuation is returned after an unknown write.
func (c *Context) Continuation() (ContinuationCheckpoint, bool) {
	if c.continuation == nil {
		return ContinuationCheckpoint{}, false
	}
	return *c.continuation, true
}

// Continue checkpoints locals and materialized SDK facts, then ends this
// delivery. Always return its error immediately from the handler. A worker
// must wire stage dispatch/publication before this operation is supported.
func Continue(c *Context, stage string, data any) error {
	if c.continuationError != nil {
		return c.continuationError
	}
	if c.continuation != nil {
		return ErrContinuation
	}
	if c.continuationStages == nil || c.continuationAnchor == nil {
		return ErrContinuationUnsupported
	}
	if err := identity.ValidateToken(stage); err != nil {
		return err
	}
	if !c.continuationStages(stage) {
		return fmt.Errorf("%w: %s", ErrUnknownContinuation, stage)
	}
	if c.waitingOn != "" || c.position%2 != 0 {
		return ErrCheckpointBoundary
	}
	if err := checkContinuationData(reflect.ValueOf(data), make(map[dataVisit]bool)); err != nil {
		return err
	}
	// Marshal user code exactly once: custom marshalers must not declare one
	// value and capture another when the frame is constructed.
	locals, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("%w: continuation locals: %v", ErrInvalidCheckpoint, err)
	}
	input := sha256.Sum256(locals)
	want := request{Kind: "checkpoint", Name: stage, InputHash: hex.EncodeToString(input[:])}
	recorded := c.position < len(c.entries)
	completedIndex := uint64(0)
	var done completion
	if recorded {
		entry := c.entries[c.position]
		var got request
		if entry.Kind != StepRequested || stepwire.Decode(entry.Payload, &got) != nil {
			return ErrCorruptJournal
		}
		if got.Kind != want.Kind || got.Name != stage || got.InputHash != want.InputHash {
			return &NonDeterministicError{Index: entry.Index, RecordedName: got.Name, RequestedName: stage}
		}
		if c.position+1 < len(c.entries) {
			complete := c.entries[c.position+1]
			if complete.Kind != StepCompleted || json.Unmarshal(complete.Payload, &done) != nil {
				return ErrCorruptJournal
			}
			completedIndex = complete.Index
			if completedIndex == 0 || done.Error != "" || done.ErrorKind != "" || done.SignalSeq != 0 || done.Selected != "" || len(done.Result) != 0 || done.ResultHash == "" || done.ResultRef != "step-result-"+done.ResultHash {
				return ErrCorruptJournal
			}
		}
	}
	if completedIndex == 0 && c.replay {
		return ErrReplayPendingStep
	}
	anchor, err := c.continuationAnchor(completedIndex, recorded)
	if err != nil {
		return err
	}
	if completedIndex != 0 && anchor.Index != completedIndex {
		return ErrInvalidCheckpoint
	}
	c.checkpointSignalCursor = &anchor.SignalCursor
	frame, hash, err := c.CaptureCheckpoint(stage, json.RawMessage(locals), anchor.Index, anchor.Epoch, anchor.PanicAttempts)
	if err != nil {
		return err
	}
	if c.loadResult == nil || completedIndex == 0 && c.storeResult == nil {
		return ErrContinuationUnsupported
	}
	if completedIndex != 0 {
		if done.ResultHash != hash {
			return fmt.Errorf("%w: checkpoint state differs at index %d", ErrNonDeterministic, completedIndex)
		}
		if err := c.verifyContinuationObject(done.ResultRef, frame); err != nil {
			return c.blockContinuation(err)
		}
		c.position += 2
	} else {
		if !recorded {
			payload, _ := json.Marshal(want)
			if err := c.next(StepRequested, payload); err != nil {
				return c.blockContinuation(err)
			}
		}
		c.position++
		ref, err := c.storeResult(c.base, bytes.Clone(frame))
		if err != nil {
			return c.blockContinuation(fmt.Errorf("%w: %w", ErrCheckpointStore, err))
		}
		if ref != "step-result-"+hash {
			return c.blockContinuation(ErrInvalidCheckpoint)
		}
		if err := c.verifyContinuationObject(ref, frame); err != nil {
			return c.blockContinuation(err)
		}
		payload, _ := json.Marshal(completion{ResultRef: ref, ResultHash: hash})
		if err := c.next(StepCompleted, payload); err != nil {
			return c.blockContinuation(err)
		}
		c.position++
		done = completion{ResultRef: ref, ResultHash: hash}
	}
	c.continuation = &ContinuationCheckpoint{ContinuationAnchor: anchor, Stage: stage, Object: done.ResultRef, SHA256: done.ResultHash, StepPosition: c.stepPosition()}
	c.waitingOn = "continuation:" + stage
	return ErrContinuation
}

func (c *Context) verifyContinuationObject(name string, expected []byte) error {
	actual, err := c.loadResult(c.base, name)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCheckpointStore, err)
	}
	if !bytes.Equal(actual, expected) {
		return ErrInvalidCheckpoint
	}
	return nil
}

func (c *Context) blockContinuation(err error) error { c.continuationError = err; return err }

// ContinuationFailure returns an unconfirmed publication error even if a handler ignored it.
func (c *Context) ContinuationFailure() error { return c.continuationError }
