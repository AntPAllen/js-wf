package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"js-wf/identity"
	"js-wf/journal"
)

var ErrReplayObjectMissing = errors.New("offline replay object is missing")

type ReplayOptions struct {
	Type    string
	ID      string
	InvSeq  uint64
	Objects map[string][]byte
}

// Replay runs a workflow against serialized []journal.Record without NATS.
// Referenced step and signal objects must be supplied in ReplayOptions.Objects.
// It checks journal ordering and that the function consumed every step.
func Replay[T any](journalBytes []byte, fn func(*Context) (T, error), options ...ReplayOptions) (result T, err error) {
	if len(options) > 1 {
		return result, fmt.Errorf("at most one replay options value is allowed")
	}
	var opts ReplayOptions
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.Type != "" || opts.ID != "" {
		if err := identity.Validate(opts.Type, opts.ID); err != nil {
			return result, err
		}
		if opts.InvSeq == 0 {
			return result, ErrInvocationIdentity
		}
	} else if opts.InvSeq != 0 {
		return result, ErrInvocationIdentity
	}
	var records []journal.Record
	if err := json.Unmarshal(journalBytes, &records); err != nil {
		return result, fmt.Errorf("decode journal: %w", err)
	}
	if len(records) == 0 || len(records) > journal.MaxEntries {
		return result, ErrCorruptJournal
	}
	var entries []Entry
	var signals []Signal
	var lastSignal uint64
	var lastAttempt int
	loadObject := func(_ context.Context, name string) ([]byte, error) {
		data, ok := opts.Objects[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrReplayObjectMissing, name)
		}
		return data, nil
	}
	for i, record := range records {
		if record.Index != uint64(i) || record.Sequence == 0 || i == 0 && record.Kind != journal.Started {
			return result, ErrCorruptJournal
		}
		if i > 0 {
			prev := records[i-1]
			if record.Sequence <= prev.Sequence || record.Epoch < prev.Epoch || prev.Kind == journal.Completed || prev.Kind == journal.Failed || record.Kind == journal.Started {
				return result, ErrCorruptJournal
			}
		}
		switch record.Kind {
		case journal.Started, journal.Suspended, journal.Completed, journal.Failed:
		case journal.Attempt:
			attempt, err := journal.DecodeAttempt(record.Payload)
			if err != nil || attempt.Count != lastAttempt+1 {
				return result, ErrCorruptJournal
			}
			lastAttempt = attempt.Count
		case journal.StepRequested, journal.StepCompleted:
			entries = append(entries, Entry{Index: record.Index, Kind: Kind(record.Kind), Payload: record.Payload})
		case journal.SignalConsumed:
			var event struct {
				Sequence uint64 `json:"sig_seq"`
				Name     string `json:"name"`
				Payload  []byte `json:"payload"`
				Ref      string `json:"ref"`
				Hash     string `json:"hash"`
			}
			if json.Unmarshal(record.Payload, &event) != nil || event.Sequence <= lastSignal || identity.ValidateToken(event.Name) != nil {
				return result, ErrCorruptJournal
			}
			lastSignal = event.Sequence
			if event.Ref != "" {
				if len(event.Payload) != 0 || event.Hash == "" {
					return result, ErrCorruptJournal
				}
				data, err := loadObject(context.Background(), event.Ref)
				if err != nil {
					return result, err
				}
				event.Payload = data
			}
			if event.Hash != "" {
				digest := sha256.Sum256(event.Payload)
				if hex.EncodeToString(digest[:]) != event.Hash {
					return result, ErrCorruptJournal
				}
			}
			signals = append(signals, Signal{Sequence: event.Sequence, Name: event.Name, Payload: event.Payload})
		default:
			return result, ErrCorruptJournal
		}
	}
	c := NewContext(context.Background(), entries, nil, signals...)
	c.SetResultStore(nil, loadObject)
	if opts.Type != "" {
		c.SetChildSupport(opts.Type, opts.ID, opts.InvSeq, nil)
	}
	defer func() {
		if panicValue := recover(); panicValue != nil {
			var zero T
			result = zero
			err = fmt.Errorf("workflow panic during replay: %v", panicValue)
		}
	}()
	result, err = fn(c)
	if err != nil {
		return result, err
	}
	if err := c.CheckComplete(); err != nil {
		return result, err
	}
	return result, nil
}
