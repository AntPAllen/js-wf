package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"js-wf/identity"
	"js-wf/retention"
	"js-wf/wf"
)

// A terminal hint only selects a cheaper durable probe. It is never authority
// to acknowledge a delivery: retirement and ID reuse can invalidate it. A
// bounded FIFO limits memory; eviction simply restores ordinary journal replay.
type terminalHints struct {
	mu   sync.Mutex
	keys map[string]bool
	ring [1024]string
	next int
}

func (h *terminalHints) contains(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.keys[key]
}

func (h *terminalHints) add(key string, ready bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.keys == nil {
		h.keys = make(map[string]bool)
	}
	if _, ok := h.keys[key]; ok {
		h.keys[key] = ready
		return
	}
	delete(h.keys, h.ring[h.next])
	h.keys[key] = ready
	h.ring[h.next] = key
	h.next = (h.next + 1) % len(h.ring)
}

func (h *terminalHints) snapshotReady(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.keys[key]; ok {
		h.keys[key] = true
	}
}

// A held lease need not delay an already durable terminal wakeup. An owned
// delivery may use this same probe after a locally observed terminal. This path
// never changes ownership, journal or outcome. Parent notification still has
// to succeed before ACK, using the same generation-scoped idempotency key as
// terminal execution. Uncertain reads retain the held NAK path or fall back
// to full journal execution when ownership has been acquired.
func (w *Worker) terminalHeldDelivery(ctx context.Context, typ, id string, timer timerWakeup, ops *deliveryOperations) (bool, bool, error) {
	ctx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	if w.graphJournal != nil {
		return w.graphTerminalHeldDelivery(ctx, typ, id, timer, ops)
	}
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
