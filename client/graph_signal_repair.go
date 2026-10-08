package client

import (
	"context"
	"fmt"

	"js-wf/identity"
	"js-wf/journal"
)

// RepairBoundSignalAttempt validates one captured canonical binding and repairs
// its wakeup without acquiring input pins or changing the journal's logical
// head. Workers independently acquire/validate input and journal ownership.
func (c *Client) RepairBoundSignalAttempt(ctx context.Context, captured journal.GraphSignalBinding) (bool, error) {
	if c.graphJournal == nil || !c.graphJournal.CanonicalSignals() {
		return false, fmt.Errorf("canonical Signals are not configured")
	}
	r := captured.Input.Request
	check := func() (*journal.GraphStartStatus, error) {
		status, err := c.graphJournal.InspectStart(ctx, r.Type, r.ID)
		if err != nil {
			return nil, err
		}
		if status == nil || status.State.Pending || status.State.Invocation != r.Invocation || status.Purging || status.Retired {
			return nil, journal.ErrStale
		}
		if status.Kind == journal.Completed || status.Kind == journal.Failed || status.SignalConsumed > captured.Index {
			return nil, nil
		}
		input, binding, err := c.graphJournal.InspectSignalRepair(ctx, r.Type, r.ID, r.Invocation, captured.Input.Index)
		if err != nil {
			return nil, err
		}
		if input != captured.Input || binding == nil || *binding != captured {
			return nil, journal.ErrStale
		}
		return status, nil
	}
	status, err := check()
	if err != nil || status == nil {
		return false, err
	}
	raw, err := c.signalOperations().LastInvocation(ctx, identity.InvocationSubject(r.Type, r.ID))
	if err != nil || !status.State.Start.MatchesInvocation(raw) || raw.Sequence != r.Invocation {
		return false, fmt.Errorf("%w: bound Signal invocation: %v", ErrSignalUnknown, err)
	}
	status, err = check()
	if err != nil || status == nil {
		return false, err
	}
	if err = c.Enqueue(ctx, r.Type, r.ID, fmt.Sprintf("signal-wakeup:%d", captured.Sequence)); err != nil {
		return false, fmt.Errorf("%w: %w", ErrEnqueueUnknown, err)
	}
	return true, nil
}
