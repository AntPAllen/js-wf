package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

func canonicalSignalError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, journal.ErrSignalMismatch):
		return fmt.Errorf("%w: %w", ErrSignalMismatch, err)
	case errors.Is(err, journal.ErrSignalNotRunning):
		return fmt.Errorf("%w: %w", ErrNotRunning, err)
	case errors.Is(err, journal.ErrStale):
		return fmt.Errorf("%w: %w", ErrStaleGeneration, err)
	default:
		return fmt.Errorf("%w: %w", ErrSignalUnknown, err)
	}
}
func (c *Client) signalCanonical(ctx context.Context, r journal.GraphSignalRequest, payload []byte, requireRunning bool, source *jetstream.RawStreamMsg, published *bool) (uint64, error) {
	if _, ok := c.signalOperations().(CanonicalSignalPort); !ok {
		return 0, fmt.Errorf("%w: source-order reads are required", ErrSignalUnknown)
	}
	if err := c.graphSignalAdmission(ctx, r.Type, r.ID, r.Invocation, requireRunning); err != nil {
		return 0, err
	}
	start, err := c.graphJournal.InspectStart(ctx, r.Type, r.ID)
	if err != nil {
		return 0, canonicalSignalError(err)
	}
	if start == nil || start.State.Pending || start.State.Invocation != r.Invocation || !start.State.Start.MatchesInvocation(source) {
		return 0, ErrStaleGeneration
	}
	var input journal.GraphSignalInput
	for attempt := 0; attempt < 16; attempt++ {
		input, err = c.graphJournal.ReserveSignal(ctx, r, payload, requireRunning)
		if err == nil {
			break
		}
		if !errors.Is(err, journal.ErrStale) || ctx.Err() != nil {
			return 0, canonicalSignalError(err)
		}
		if e := c.recheckGraphSignal(ctx, r.Type, r.ID, r.Invocation, requireRunning); e != nil {
			return 0, e
		}
		if e := c.startOperations().Wait(ctx, 10*time.Millisecond); e != nil {
			return 0, canonicalSignalError(e)
		}
	}
	if err != nil {
		return 0, canonicalSignalError(err)
	}
	return c.finishCanonicalSignal(ctx, input, requireRunning, published)
}

// finishCanonicalSignal retains the input across source publication. A source
// pointer is not ownership: success requires its canonical queue binding, and
// an uncertain source or root outcome is resolved only from that binding.
func (c *Client) finishCanonicalSignal(ctx context.Context, input journal.GraphSignalInput, requireRunning bool, published *bool) (sequence uint64, err error) {
	r := input.Request
	port, ok := c.signalOperations().(CanonicalSignalPort)
	if !ok {
		return 0, fmt.Errorf("%w: source-order reads are required", ErrSignalUnknown)
	}
	view, e := c.graphJournal.Open(ctx, r.Type, r.ID, r.Invocation)
	if e != nil {
		return 0, canonicalSignalError(e)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if e := view.Close(cleanup); err == nil {
			err = canonicalSignalError(e)
		}
	}()
	owned, _, found, e := view.SignalInput(ctx, r)
	if e != nil {
		return 0, canonicalSignalError(e)
	}
	if !found || owned != input {
		return 0, ErrStaleGeneration
	}
	if e = c.recheckGraphSignal(ctx, r.Type, r.ID, r.Invocation, requireRunning); e != nil {
		return 0, e
	}
	binding, _, found, e := view.SignalBinding(ctx, r)
	if e != nil {
		return 0, canonicalSignalError(e)
	}
	if found {
		if binding.Input != input {
			return 0, ErrSignalMismatch
		}
		return c.enqueueCanonicalSignal(ctx, input, binding.Sequence, requireRunning)
	}
	data, _ := json.Marshal(r)
	keyHash := sha256.Sum256(data)
	messageID := "graph-signal:" + hex.EncodeToString(keyHash[:]) + ":" + input.Token
	msg := &nats.Msg{Subject: "wf.sig." + r.Type + "." + r.ID + "." + r.Name, Data: input.PointerBytes(), Header: nats.Header{}}
	msg.Header.Set(journal.GraphSignalTokenHeader, input.Token)
	msg.Header.Set(inputHashHeader, input.InputSHA256)
	msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(r.Invocation, 10))
	if published != nil {
		*published = true
	}
	attempt, stop := context.WithTimeout(ctx, 3*time.Second)
	ack, publishErr := port.PublishSignal(attempt, msg, messageID)
	stop()
	through := ack.Sequence
	if publishErr != nil {
		attempt, stop = context.WithTimeout(ctx, 3*time.Second)
		through, e = port.LastSignalSequence(attempt)
		stop()
		if e != nil {
			return 0, fmt.Errorf("%w: publish: %v; source tip: %w", ErrSignalUnknown, publishErr, e)
		}
	}
	if through == 0 {
		return 0, ErrSignalUnknown
	}
	// Each cut publishes one ordered source. Bounds keep a single client call
	// finite; recovery can resume a durable partial frontier on a later call.
	for step := 0; step < 256; step++ {
		attempt, stop = context.WithTimeout(ctx, 3*time.Second)
		progress, bindErr := c.graphJournal.BindNextSignal(attempt, r.Type, r.ID, r.Invocation, through, port)
		stop()
		binding, _, found, e = c.graphJournal.ReadSignalBinding(ctx, r)
		if e != nil {
			return 0, canonicalSignalError(e)
		}
		if found {
			if binding.Input != input {
				return 0, ErrSignalMismatch
			}
			return c.enqueueCanonicalSignal(ctx, input, binding.Sequence, requireRunning)
		}
		if bindErr != nil {
			if !errors.Is(bindErr, journal.ErrStale) || ctx.Err() != nil {
				return 0, canonicalSignalError(bindErr)
			}
			if e = c.recheckGraphSignal(ctx, r.Type, r.ID, r.Invocation, requireRunning); e != nil {
				return 0, e
			}
			if e = c.startOperations().Wait(ctx, 10*time.Millisecond); e != nil {
				return 0, canonicalSignalError(e)
			}
		} else if !progress {
			return 0, ErrSignalUnknown
		}
	}
	return 0, fmt.Errorf("%w: ordered binding budget exhausted", ErrSignalUnknown)
}
func (c *Client) enqueueCanonicalSignal(ctx context.Context, input journal.GraphSignalInput, sequence uint64, requireRunning bool) (uint64, error) {
	r := input.Request
	if err := c.recheckGraphSignal(ctx, r.Type, r.ID, r.Invocation, requireRunning); err != nil {
		return sequence, err
	}
	if err := c.Enqueue(ctx, r.Type, r.ID, fmt.Sprintf("signal-wakeup:%d", sequence)); err != nil {
		return sequence, fmt.Errorf("%w: %w", ErrEnqueueUnknown, err)
	}
	return sequence, nil
}

// RecoverSignal resumes the exact durable reservation without caller payload.
// A supplied token fences a captured recovery attempt against replacement.
func (c *Client) RecoverSignal(ctx context.Context, r journal.GraphSignalRequest, token string) (uint64, error) {
	if c.graphJournal == nil || !c.graphJournal.CanonicalSignals() {
		return 0, fmt.Errorf("canonical Signals are not configured")
	}
	input, _, found, err := c.graphJournal.ReadSignalInput(ctx, r)
	if err != nil {
		return 0, canonicalSignalError(err)
	}
	if !found {
		return 0, ErrNotFound
	}
	if token != "" && token != input.Token {
		return 0, ErrStaleGeneration
	}
	return c.finishCanonicalSignal(ctx, input, false, nil)
}
