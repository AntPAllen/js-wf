package client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
)

func (c *Client) startCanonical(ctx context.Context, r journal.GraphStartRequest, input []byte) (Handle, error) {
	h := Handle{Type: r.Type, ID: r.ID}
	var state journal.GraphStartState
	var err error
	for attempt := 0; attempt < 16; attempt++ {
		state, err = c.graphJournal.ReserveStart(ctx, r, input)
		h.InvSeq = state.Invocation
		if errors.Is(err, journal.ErrStartMismatch) {
			return h, ErrInputMismatch
		}
		if err == nil {
			break
		}
		if !errors.Is(err, journal.ErrStale) || ctx.Err() != nil {
			return h, fmt.Errorf("%w: reserve: %w", ErrStartUnknown, err)
		}
		status, e := c.graphJournal.InspectRetirement(ctx, r.Type, r.ID)
		if e != nil {
			return h, fmt.Errorf("%w: %w", ErrStartUnknown, e)
		}
		if status.Purging && !status.Retired {
			return h, ErrPurged
		}
		if e = c.startOperations().Wait(ctx, 10*time.Millisecond); e != nil {
			return h, fmt.Errorf("%w: %w", ErrStartUnknown, e)
		}
	}
	if err != nil {
		return h, fmt.Errorf("%w: reserve: %w", ErrStartUnknown, err)
	}
	return c.finishCanonicalStart(ctx, state)
}

// finishCanonicalStart never reserves a replacement; recovery must remain bound
// to the captured durable token even if retirement races a later observation.
func (c *Client) finishCanonicalStart(ctx context.Context, state journal.GraphStartState) (Handle, error) {
	r := state.Start.Request
	h := Handle{Type: r.Type, ID: r.ID, InvSeq: state.Invocation}
	validated, _, err := c.graphJournal.ReadStart(ctx, r.Type, r.ID)
	if err != nil {
		return h, fmt.Errorf("%w: input: %w", ErrStartUnknown, err)
	}
	if validated.Start != state.Start {
		return h, ErrStaleGeneration
	}
	state = validated
	h.InvSeq = state.Invocation
	already := !state.Pending
	if already {
		existing, e := c.startOperations().LastInvocation(ctx, identity.InvocationSubject(r.Type, r.ID))
		if e != nil || !state.Start.MatchesInvocation(existing) || existing.Sequence != state.Invocation {
			return h, fmt.Errorf("%w: source pointer: %v", ErrStartUnknown, e)
		}
	} else {
		m := &nats.Msg{Subject: identity.InvocationSubject(r.Type, r.ID), Data: state.Start.PointerBytes(), Header: nats.Header{}}
		m.Header.Set(inputHashHeader, state.Start.InputSHA256)
		m.Header.Set(journal.GraphStartTokenHeader, state.Start.Token)
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, "0")
		if r.ParentType != "" {
			m.Header.Set(ParentTypeHeader, r.ParentType)
			m.Header.Set(ParentIDHeader, r.ParentID)
			m.Header.Set(ParentInvSeqHeader, strconv.FormatUint(r.ParentInvocation, 10))
			m.Header.Set(ParentSignalHeader, r.SignalName)
		}
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		sequence, publishErr := c.startOperations().PublishInvocation(attempt, m)
		stop()
		if publishErr != nil {
			existing, getErr := c.readStartAfterPublishError(ctx, m.Subject)
			if getErr != nil || !state.Start.MatchesInvocation(existing) {
				return h, fmt.Errorf("%w: publish: %v; pointer read: %v", ErrStartUnknown, publishErr, getErr)
			}
			sequence = existing.Sequence
			already = true
		}
		if sequence == 0 {
			return h, ErrStartUnknown
		}
		h.InvSeq = sequence
		for attempt := 0; attempt < 16; attempt++ {
			err = c.graphJournal.BindStart(ctx, r.Type, r.ID, state.Start.Token, sequence)
			if err == nil {
				break
			}
			if !errors.Is(err, journal.ErrStale) || ctx.Err() != nil {
				return h, fmt.Errorf("%w: bind: %w", ErrStartUnknown, err)
			}
			if e := c.startOperations().Wait(ctx, 10*time.Millisecond); e != nil {
				return h, fmt.Errorf("%w: %w", ErrStartUnknown, e)
			}
		}
		if err != nil {
			return h, fmt.Errorf("%w: bind: %w", ErrStartUnknown, err)
		}
	}
	if err = c.Enqueue(ctx, r.Type, r.ID, "start:"+identity.Key(r.Type, r.ID)+":"+strconv.FormatUint(h.InvSeq, 10)); err != nil {
		return h, fmt.Errorf("%w: %w", ErrEnqueueUnknown, err)
	}
	if already {
		return h, ErrAlreadyStarted
	}
	return h, nil
}

// RecoverStart resumes a canonical pending source write/binding or repairs its
// missing enqueue from graph-owned input. No caller input or process-local
// prepared object is needed. This is an explicit recovery operation; catalog
// scanning and deployed repair-loop integration remain separate requirements.
func (c *Client) RecoverStart(ctx context.Context, typ, id string) (Handle, error) {
	return c.RecoverStartAttempt(ctx, typ, id, "")
}

// RecoverStartAttempt resumes only the captured durable token when token is
// nonempty. It never reserves a replacement generation after retirement.
func (c *Client) RecoverStartAttempt(ctx context.Context, typ, id, token string) (Handle, error) {
	h := Handle{Type: typ, ID: id}
	if c.graphJournal == nil || !c.graphJournal.CanonicalStarts() {
		return h, fmt.Errorf("canonical starts are not configured")
	}
	state, _, err := c.graphJournal.ReadStart(ctx, typ, id)
	if err != nil {
		return h, err
	}
	if state.Start.Token == "" {
		return h, ErrNotFound
	}
	if token != "" && state.Start.Token != token {
		return h, ErrStaleGeneration
	}
	h, err = c.finishCanonicalStart(ctx, state)
	if errors.Is(err, ErrAlreadyStarted) {
		err = nil
	}
	return h, err
}

// RepairBoundStartAttempt repairs delivery of an already-bound generation.
// It does not open input: repeated reader acquisition would advance the shared
// logical head and can starve a worker's prepared Started append. Execution
// still requires the worker's exact binding and owned-input validation.
func (c *Client) RepairBoundStartAttempt(ctx context.Context, typ, id, token string, invocation uint64) (Handle, error) {
	// An acknowledged wakeup can disappear before execution starts. Reusing
	// its message ID would suppress a fresh repair inside the dedup window.
	// Captured binding/source validation and worker fencing make duplicates safe.
	return c.repairBoundGraphAttempt(ctx, typ, id, token, invocation, "", false)
}

// RepairTerminalProjectionAttempt repairs a captured terminal generation's
// delivery without opening input or changing its graph head. No deduplication
// key is used: an earlier repaired wakeup may already have been acknowledged
// before a projection is lost again. The worker verifies canonical ownership.
func (c *Client) RepairTerminalProjectionAttempt(ctx context.Context, typ, id, token string, invocation uint64) (bool, error) {
	_, err := c.repairBoundGraphAttempt(ctx, typ, id, token, invocation, "", true)
	return err == nil, err
}

func (c *Client) repairBoundGraphAttempt(ctx context.Context, typ, id, token string, invocation uint64, messageID string, terminal bool) (Handle, error) {
	return c.repairBoundGraphGuard(ctx, typ, id, token, invocation, messageID, terminal, nil)
}

// RepairContinuationAttempt dispatches only the captured canonical checkpoint
// generation after checking the native source and observing eligibility again.
// It does not open a frame or authorize execution; workers own those checks.
func (c *Client) RepairContinuationAttempt(ctx context.Context, typ, id, token string, invocation, checkpointSequence uint64) (bool, error) {
	if c.graphJournal == nil || !c.graphJournal.CheckpointIndex() || checkpointSequence == 0 {
		return false, journal.ErrGap
	}
	_, err := c.repairBoundGraphGuard(ctx, typ, id, token, invocation, "", false, func(status *journal.GraphStartStatus) bool {
		return status.ContinuationReady() && status.Checkpoint.Sequence == checkpointSequence
	})
	return err == nil, err
}

func (c *Client) repairBoundGraphGuard(ctx context.Context, typ, id, token string, invocation uint64, messageID string, terminal bool, guard func(*journal.GraphStartStatus) bool) (Handle, error) {

	h := Handle{Type: typ, ID: id, InvSeq: invocation}
	if c.graphJournal == nil || !c.graphJournal.CanonicalStarts() || token == "" || invocation == 0 {
		return h, journal.ErrGap
	}
	check := func() (*journal.GraphStartStatus, error) {
		status, err := c.graphJournal.InspectStart(ctx, typ, id)
		if err != nil {
			return nil, err
		}
		if status == nil || status.Retired || status.Purging || status.State.Pending || status.State.Invocation != invocation || status.State.Start.Token != token {
			return nil, journal.ErrStale
		}
		if guard != nil && !guard(status) {
			return nil, journal.ErrStale
		}
		if terminal && status.Kind != journal.Completed && status.Kind != journal.Failed {
			return nil, journal.ErrStale
		}
		return status, nil
	}
	status, err := check()
	if err != nil {
		return h, err
	}
	source, err := c.startOperations().LastInvocation(ctx, identity.InvocationSubject(typ, id))
	if err != nil || !status.State.Start.MatchesInvocation(source) || source.Sequence != invocation {
		return h, fmt.Errorf("%w: source pointer: %v", ErrStartUnknown, err)
	}
	if _, err = check(); err != nil {
		return h, err
	}
	if err = c.Enqueue(ctx, typ, id, messageID); err != nil {
		return h, fmt.Errorf("%w: %w", ErrEnqueueUnknown, err)
	}
	return h, nil
}
