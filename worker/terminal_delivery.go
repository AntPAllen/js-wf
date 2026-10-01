package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"js-wf/identity"
	"js-wf/retention"
	"js-wf/wf"
)

// A held lease need not delay an already durable terminal wakeup. This path
// never changes ownership, journal or outcome. Parent notification still has
// to succeed before ACK, using the same generation-scoped idempotency key as
// terminal execution. Any uncertain read keeps the original NAK path.
func (w *Worker) terminalHeldDelivery(ctx context.Context, typ, id string, timer timerWakeup, ops *deliveryOperations) (bool, bool, error) {
	ctx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	outcomePort := w.outcomePort
	if outcomePort == nil {
		if w.state == nil {
			return false, false, nil
		}
		outcomePort = NewOutcomePort(w.state)
	}
	started := ops.begin()
	state, err := outcomePort.Get(ctx, identity.Key(typ, id))
	ops.finish(started, "terminal_state_read", 0, "", err)
	if err != nil {
		return false, false, err
	}
	_, tomb, err := retention.Decode(state.Value)
	var outcome wf.Outcome
	if err != nil || tomb || json.Unmarshal(state.Value, &outcome) != nil || outcome.InvSeq == 0 {
		return false, false, nil
	}
	invocationPort := w.invocationPort
	if invocationPort == nil {
		if w.js == nil {
			return false, false, nil
		}
		invocationPort = jetStreamInvocationPort{js: w.js}
	}
	started = ops.begin()
	input, err := invocationPort.LastInvocation(ctx, identity.InvocationSubject(typ, id))
	ops.finish(started, "terminal_invocation_read", 0, "", err)
	if err != nil {
		return false, false, err
	}
	if input == nil || input.Sequence != outcome.InvSeq {
		return false, false, nil
	}
	canceledTimer := false
	// Preserve the existing canceled-timer no-op metric. Terminal state alone
	// cannot tell whether this particular timer had been canceled.
	if timer.scheduled && timer.generation == input.Sequence {
		started = ops.begin()
		records, _, err := w.jrn.Read(ctx, typ, id)
		ops.finish(started, "terminal_timer_read", 0, "", err)
		if err != nil {
			return false, false, err
		}
		canceledTimer = timerWasCancelled(records, timer.step)
	}
	started = ops.begin()
	err = NotifyParentWithClient(ctx, w.client, typ, id, input.Sequence, state.Value, input.Header)
	ops.finish(started, "terminal_parent_notify", 0, "", err)
	if err != nil {
		return false, false, fmt.Errorf("terminal parent notification: %w", err)
	}
	if ctx.Err() != nil {
		return false, false, ctx.Err()
	}
	return true, canceledTimer, nil
}
