package client

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

// GraphResultPort supplies invocation identity and legacy purge markers.
// Terminal bytes and payload ownership are always read from the graph store.
// Wait must honor context and may advance virtual time in a transport model.
type GraphResultPort interface {
	LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error)
	State(context.Context, string) ([]byte, error)
	Wait(context.Context, time.Duration) error
}

type jetStreamGraphResultPort struct{ js jetstream.JetStream }

func (p jetStreamGraphResultPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}
func (p jetStreamGraphResultPort) State(ctx context.Context, key string) ([]byte, error) {
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return nil, err
	}
	entry, err := state.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}
func (p jetStreamGraphResultPort) Wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// NewWithGraphJournal reads all terminal outcomes through the experimental
// graph journal. Signal admission and duplicate confirmation use graph history;
// CanonicalStarts stores Start input and pending recovery in the graph; source
// pointers remain in WF_INV. Signals still use legacy publication. Use graph-aware retention;
// remaining canonical publication/import migration is required before online GC.
func NewWithGraphJournal(js jetstream.JetStream, store *journal.GraphStore) (*Client, error) {
	if js == nil || store == nil {
		return nil, fmt.Errorf("invalid graph client configuration")
	}
	c := New(js)
	c.graphJournal = store
	c.graphResultPort = jetStreamGraphResultPort{js: js}
	return c, nil
}

// NewWithGraphJournalPorts runs production graph result decisions through
// supplied transports. Start uses canonical staging when enabled on the store;
// invocation pointers and signals use supplied transports,
// with graph-aware signal admission, retirement and duplicate confirmation.
func NewWithGraphJournalPorts(start StartPort, signal SignalPort, results GraphResultPort, store *journal.GraphStore) (*Client, error) {
	if start == nil || signal == nil || results == nil || store == nil {
		return nil, fmt.Errorf("invalid graph client ports")
	}
	c := NewWithSignalPorts(start, signal)
	c.graphJournal = store
	c.graphResultPort = results
	return c, nil
}

func (c *Client) awaitGraph(ctx context.Context, typ, id string, observed *uint64, failed *bool) ([]byte, error) {
	port := c.graphResultPort
	var input *jetstream.RawStreamMsg
	var err error
	for {
		input, err = retryAwaitRead(ctx, func(attempt context.Context) (*jetstream.RawStreamMsg, error) {
			return port.LastInvocation(attempt, identity.InvocationSubject(typ, id))
		})
		if errors.Is(err, jetstream.ErrMsgNotFound) && c.graphJournal.CanonicalStarts() {
			lifecycle, e := c.graphJournal.InspectRetirement(ctx, typ, id)
			if e != nil {
				return nil, e
			}
			// Pending replacement retains proof of the previous retirement;
			// that proof does not mean the new pending attempt is purged.
			if lifecycle.PendingStart {
				if e = port.Wait(ctx, 100*time.Millisecond); e != nil {
					return nil, e
				}
				continue
			}
			if lifecycle.Retired || lifecycle.Purging {
				*observed = lifecycle.Invocation
				return nil, ErrPurged
			}

			if lifecycle.Invocation != 0 {
				*observed = lifecycle.Invocation
				return nil, ErrStartUnknown
			}
		}
		break
	}
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		marker, tomb, stateErr := c.graphPurgeMarker(ctx, typ, id)
		if stateErr != nil {
			return nil, stateErr
		}
		if tomb {
			*observed = marker.InvSeq
			return nil, ErrPurged
		}
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if input == nil || input.Sequence == 0 {
		return nil, wf.ErrCorruptJournal
	}
	*observed = input.Sequence
	for {
		marker, tomb, err := c.graphPurgeMarker(ctx, typ, id)
		if err != nil {
			return nil, err
		}
		if tomb && marker.InvSeq >= input.Sequence {
			return nil, ErrPurged
		}
		data, terminal, ready, err := c.graphTerminalResult(ctx, typ, id, input.Sequence)
		if err != nil {
			// Bare ErrStale is generation rejection; wrapped CAS contention
			// must not be reported as a purge.
			if err == journal.ErrStale {
				return nil, ErrPurged
			}
			return nil, err
		}
		current, getErr := retryAwaitRead(ctx, func(attempt context.Context) (*jetstream.RawStreamMsg, error) {
			return port.LastInvocation(attempt, identity.InvocationSubject(typ, id))
		})
		if errors.Is(getErr, jetstream.ErrMsgNotFound) || getErr == nil && (current == nil || current.Sequence != input.Sequence) {
			return nil, ErrPurged
		}
		if getErr != nil {
			return nil, getErr
		}
		if ready {
			// Retirement may race payload consumption; never return a terminal result
			// after observing the matching legacy purge marker.
			marker, tomb, getErr = c.graphPurgeMarker(ctx, typ, id)
			if getErr != nil {
				return nil, getErr
			}
			if tomb && marker.InvSeq >= input.Sequence {
				return nil, ErrPurged
			}
			if terminal.Error != "" {
				*failed = true
				if terminal.Error == ErrCancelled.Error() {
					return nil, ErrCancelled
				}
				return nil, errors.New(terminal.Error)
			}
			return data, nil
		}
		if err = port.Wait(ctx, 100*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

func (c *Client) graphPurgeMarker(ctx context.Context, typ, id string) (retention.Tombstone, bool, error) {
	value, err := retryAwaitRead(ctx, func(attempt context.Context) ([]byte, error) {
		return c.graphResultPort.State(attempt, identity.Key(typ, id))
	})
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return retention.Tombstone{}, false, nil
	}
	if err != nil {
		return retention.Tombstone{}, false, err
	}
	return retention.Decode(value)
}

func (c *Client) graphTerminalResult(ctx context.Context, typ, id string, invocation uint64) (data []byte, terminal wf.Outcome, ready bool, err error) {
	view, err := c.graphJournal.OpenTerminal(ctx, typ, id, invocation)
	if err != nil || view == nil {
		return nil, terminal, false, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if closeErr := view.Close(cleanup); err == nil && closeErr != nil {
			data = nil
			ready = false
			err = closeErr
		}
	}()
	if c.graphJournal.CanonicalStarts() {
		input, e := c.graphResultPort.LastInvocation(ctx, identity.InvocationSubject(typ, id))
		if e != nil {
			return nil, terminal, false, e
		}
		if e = view.ValidateStartInvocation(ctx, input); e != nil {
			return nil, terminal, false, e
		}
	}
	verified, err := wf.ReadGraphTerminal(ctx, view, invocation, c.graphJournal.PayloadReadLimit())
	return verified.Result, verified.Outcome, err == nil, err
}
