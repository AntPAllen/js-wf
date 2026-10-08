package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

// Graph duplicates use the same canonical terminal validator as graph clients.
// Legacy state is read only for purge markers; terminal mirror bytes cannot
// authorize an ACK or select the parent notification payload.
func (w *Worker) graphTerminalHeldDelivery(ctx context.Context, typ, id string, timer timerWakeup, ops *deliveryOperations) (terminal, canceledTimer bool, err error) {
	outcomePort := w.outcomePort
	if outcomePort == nil {
		if w.state == nil {
			return false, false, nil
		}
		outcomePort = NewOutcomePort(w.state)
	}
	invocationPort := w.invocationPort
	if invocationPort == nil {
		if w.js == nil {
			return false, false, nil
		}
		invocationPort = jetStreamInvocationPort{js: w.js}
	}
	// Purge ordering is still legacy and must remain a separate lifecycle fence.
	purge := func(invocation uint64) (bool, error) {
		started := ops.begin()
		state, e := outcomePort.Get(ctx, identity.Key(typ, id))
		ops.finish(started, "terminal_state_read", 0, "", e)
		if errors.Is(e, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		if e != nil {
			return false, e
		}
		marker, tomb, e := retention.Decode(state.Value)
		return tomb && marker.InvSeq >= invocation, e
	}
	started := ops.begin()
	input, e := invocationPort.LastInvocation(ctx, identity.InvocationSubject(typ, id))
	ops.finish(started, "terminal_invocation_read", 0, "", e)
	if e != nil {
		return false, false, e
	}
	if input == nil || input.Sequence == 0 {
		return false, false, wf.ErrCorruptJournal
	}
	purged, e := purge(input.Sequence)
	if e != nil || purged {
		return false, false, e
	}
	started = ops.begin()
	view, e := w.graphJournal.OpenTerminal(ctx, typ, id, input.Sequence)
	ops.finish(started, "terminal_graph_open", 0, "", e)
	if e != nil {
		return false, false, e
	}
	if view == nil {
		return false, false, nil
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if closeErr := view.Close(cleanup); err == nil && closeErr != nil {
			terminal = false
			canceledTimer = false
			err = closeErr
		}
	}()
	started = ops.begin()
	verified, e := wf.ReadGraphTerminal(ctx, view, input.Sequence, w.graphJournal.PayloadReadLimit())
	ops.finish(started, "terminal_graph_read", 0, "", e)
	if e != nil {
		return false, false, e
	}
	if timer.scheduled && timer.generation == input.Sequence {
		started = ops.begin()
		records := make([]journal.Record, 0, view.Count())
		for i := uint64(0); i < view.Count(); i++ {
			r, readErr := view.Read(ctx, i)
			if readErr != nil {
				ops.finish(started, "terminal_timer_read", 0, "", readErr)
				return false, false, readErr
			}
			records = append(records, r.Record)
		}
		ops.finish(started, "terminal_timer_read", 0, "", nil)
		canceledTimer = timerWasCancelled(records, timer.step)
	}
	started = ops.begin()
	e = NotifyParentWithClient(ctx, w.client, typ, id, input.Sequence, verified.Payload, input.Header)
	ops.finish(started, "terminal_parent_notify", 0, "", e)
	if e != nil {
		return false, false, fmt.Errorf("terminal parent notification: %w", e)
	}
	// Recheck lifecycle after notification before authorizing dispatch ACK.
	started = ops.begin()
	current, e := invocationPort.LastInvocation(ctx, identity.InvocationSubject(typ, id))
	ops.finish(started, "terminal_invocation_read", 0, "", e)
	if e != nil {
		return false, false, e
	}
	if current == nil || current.Sequence != input.Sequence {
		return false, false, nil
	}
	purged, e = purge(input.Sequence)
	if e != nil || purged {
		return false, false, e
	}
	if e = ctx.Err(); e != nil {
		return false, false, e
	}
	return true, canceledTimer, nil
}
