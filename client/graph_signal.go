package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"
)

// graphSignalAdmission observes lifecycle without consulting compatibility
// state/journal mirrors. It is a read check, not an atomic publication fence.
func (c *Client) graphSignalAdmission(ctx context.Context, typ, id string, invocation uint64, requireRunning bool) error {
	status, err := c.graphJournal.InspectRetirement(ctx, typ, id)
	if err != nil {
		return err
	}
	if status.Invocation == 0 {
		return nil
	}
	if status.Invocation != invocation {
		// A published replacement may precede its worker's Begin. Only retirement
		// of the older graph permits that generation transition.
		if status.Invocation < invocation && status.Retired {
			return nil
		}
		return ErrStaleGeneration
	}
	if status.Purging || status.Retired {
		return ErrPurged
	}
	if requireRunning && (status.Kind == journal.Completed || status.Kind == journal.Failed) {
		_, _, ready, err := c.graphTerminalResult(ctx, typ, id, invocation)
		if err == journal.ErrStale {
			return ErrPurged
		}
		if err != nil {
			return err
		}
		if !ready {
			return journal.ErrGap
		}
		return ErrNotRunning
	}
	return nil
}

func (c *Client) recheckGraphSignal(ctx context.Context, typ, id string, invocation uint64, requireRunning bool) error {
	current, err := c.signalOperations().LastInvocation(ctx, identity.InvocationSubject(typ, id))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return ErrStaleGeneration
	}
	if err != nil {
		return err
	}
	if current == nil || current.Sequence == 0 {
		return wf.ErrCorruptJournal
	}
	if current.Sequence != invocation {
		return ErrStaleGeneration
	}
	return c.graphSignalAdmission(ctx, typ, id, invocation, requireRunning)
}

func (c *Client) graphGenerationRetired(ctx context.Context, typ, id string, invocation uint64) (bool, error) {
	status, err := c.graphJournal.InspectRetirement(ctx, typ, id)
	if err != nil {
		return false, err
	}
	return status.Invocation > invocation || status.Invocation == invocation && (status.Purging || status.Retired), nil
}

func (c *Client) verifyGraphConsumedSignal(ctx context.Context, typ, id, name string, invocation, sequence uint64, hash string) (err error) {
	view, err := c.graphJournal.OpenExisting(ctx, typ, id, invocation)
	if err == journal.ErrStale {
		return ErrStaleGeneration
	}
	if err != nil {
		return err
	}
	if view == nil {
		return fmt.Errorf("%w: duplicate signal %d lacks canonical consumption", ErrSignalUnknown, sequence)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if closeErr := view.Close(cleanup); err == nil {
			err = closeErr
		}
	}()
	matched := false
	for i := uint64(0); i < view.Count(); i++ {
		record, e := view.Read(ctx, i)
		if e != nil {
			return e
		}
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var consumed struct {
			Sequence uint64 `json:"sig_seq"`
			Name     string `json:"name"`
			Hash     string `json:"hash"`
			Payload  []byte `json:"payload,omitempty"`
			Ref      string `json:"ref,omitempty"`
		}
		if json.Unmarshal(record.Payload, &consumed) != nil {
			return wf.ErrCorruptJournal
		}
		if consumed.Sequence != sequence {
			continue
		}
		if matched {
			return wf.ErrCorruptJournal
		}
		if consumed.Name != name || consumed.Hash != hash {
			return ErrSignalMismatch
		}
		data := consumed.Payload
		if consumed.Ref != "" {
			if len(data) != 0 {
				return wf.ErrCorruptJournal
			}
			found := false
			for _, link := range append(record.Blobs, record.EntryBlob) {
				if link.Hash != hash {
					continue
				}
				data, e = view.Payload(ctx, record.Index, link, c.graphJournal.PayloadReadLimit())
				if e != nil {
					return e
				}
				found = true
				break
			}
			if !found {
				return wf.ErrCorruptJournal
			}
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != hash {
			return wf.ErrCorruptJournal
		}
		matched = true
	}
	if !matched {
		return fmt.Errorf("%w: duplicate signal %d lacks canonical consumption", ErrSignalUnknown, sequence)
	}
	return nil
}
