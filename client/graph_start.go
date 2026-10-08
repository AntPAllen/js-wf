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
	h := Handle{Type: typ, ID: id}
	if c.graphJournal == nil || !c.graphJournal.CanonicalStarts() {
		return h, fmt.Errorf("canonical starts are not configured")
	}
	state, input, err := c.graphJournal.ReadStart(ctx, typ, id)
	if err != nil {
		return h, err
	}
	if state.Start.Token == "" {
		return h, ErrNotFound
	}
	h, err = c.startCanonical(ctx, state.Start.Request, input)
	if errors.Is(err, ErrAlreadyStarted) {
		err = nil
	}
	return h, err
}
