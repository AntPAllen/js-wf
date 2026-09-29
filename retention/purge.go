package retention

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrActive = errors.New("invocation lease is held")
var ErrNotTerminal = errors.New("invocation has no durable terminal result")
var ErrNotFound = errors.New("invocation not found")

// Purge retires one completed generation. Each stage is idempotent, so a
// caller may retry after a crash. The invocation record is purged last.
func Purge(ctx context.Context, js jetstream.JetStream, typ, id string, grace time.Duration) error {
	return purge(ctx, js, typ, id, grace, nil)
}

func purge(ctx context.Context, js jetstream.JetStream, typ, id string, grace time.Duration, afterStage func(string) error) error {
	if err := identity.Validate(typ, id); err != nil {
		return err
	}
	if grace <= 0 {
		return fmt.Errorf("tombstone grace must be positive")
	}
	leasing, err := lease.New(ctx, js)
	if err != nil {
		return err
	}
	return purgeWithPort(ctx, &jetStreamPurgePort{js: js, leasing: leasing, streams: map[string]jetstream.Stream{}}, typ, id, grace, afterStage)
}

// PurgeWithPort runs the production retirement stages over a supplied
// transport. The same ordering and crash recovery run against the simulator.
func PurgeWithPort(ctx context.Context, port PurgePort, typ, id string, grace time.Duration) error {
	return purgeWithPort(ctx, port, typ, id, grace, nil)
}

func purgeWithPort(ctx context.Context, port PurgePort, typ, id string, grace time.Duration, afterStage func(string) error) error {
	stage := func(name string) error {
		if afterStage != nil {
			return afterStage(name)
		}
		return nil
	}
	if err := identity.Validate(typ, id); err != nil {
		return err
	}
	if port == nil {
		return fmt.Errorf("purge transport is nil")
	}
	if grace <= 0 {
		return fmt.Errorf("tombstone grace must be positive")
	}
	l, err := port.Acquire(ctx, typ, id)
	if errors.Is(err, lease.ErrHeld) || errors.Is(err, lease.ErrLost) && errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		// An initialization CAS can lose to another acquirer reclaiming an
		// uninitialized lease under load. No retention work has begun yet.
		return ErrActive
	}
	if err != nil {
		return err
	}
	defer func() { _ = l.Release(context.Background()) }()
	key := identity.Key(typ, id)
	purgeKey := "purging." + key
	input, err := port.Invocation(ctx, identity.InvocationSubject(typ, id))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		if value, stateErr := port.State(ctx, key); stateErr == nil {
			marker, tomb, decodeErr := Decode(value.Value)
			if decodeErr != nil {
				return decodeErr
			}
			if tomb {
				if err := port.PublishPurge(ctx, typ, id, marker.InvSeq); err != nil {
					return err
				}
				return clearPurgeMarker(ctx, port, purgeKey, marker.InvSeq)
			}
		} else if !errors.Is(stateErr, jetstream.ErrKeyNotFound) {
			return stateErr
		}
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if parentType := input.Header.Get("Wf-Parent-Type"); parentType != "" {
		parentID := input.Header.Get("Wf-Parent-ID")
		if err := identity.Validate(parentType, parentID); err != nil {
			return err
		}
		parentInvSeq := input.Header.Get("Wf-Parent-Inv-Seq")
		parentRetired := false
		parentCurrent := false
		if parentInvSeq != "" {
			expected, err := strconv.ParseUint(parentInvSeq, 10, 64)
			if err != nil || expected == 0 {
				return fmt.Errorf("invalid child parent invocation sequence")
			}
			current, err := port.Invocation(ctx, identity.InvocationSubject(parentType, parentID))
			if err == nil && current.Sequence != expected {
				parentRetired = true
			} else if err == nil {
				parentCurrent = true
			}
			if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
				return err
			}
		}
		if !parentRetired {
			parent, err := port.State(ctx, identity.Key(parentType, parentID))
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				if parentInvSeq == "" || parentCurrent {
					return ErrNotTerminal
				}
			} else if err != nil {
				return err
			} else if _, _, err := Decode(parent.Value); err != nil {
				return err
			}
		}
	}
	terminal, err := port.State(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return ErrNotTerminal
	}
	if err != nil {
		return err
	}
	marker, tomb, err := Decode(terminal.Value)
	if err != nil {
		return err
	}
	if tomb {
		if marker.InvSeq != input.Sequence {
			return ErrNotTerminal
		}
		if err := port.PublishPurge(ctx, typ, id, input.Sequence); err != nil {
			return err
		}
		if err := port.PurgeSubject(ctx, "WF_INV", identity.InvocationSubject(typ, id), input.Sequence+1); err != nil {
			return err
		}
		if err := stage("invocation"); err != nil {
			return err
		}
		return clearPurgeMarker(ctx, port, purgeKey, input.Sequence)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(terminal.Value, &outcome); err != nil || outcome.InvSeq != 0 && outcome.InvSeq != input.Sequence {
		return ErrNotTerminal
	}
	priorGeneration, err := purgeMarker(ctx, port, purgeKey)
	if err != nil {
		return err
	}
	if priorGeneration != input.Sequence {
		records, err := port.Journal(ctx, typ, id)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return ErrNotTerminal
		}
		last := records[len(records)-1]
		if (last.Kind != journal.Completed && last.Kind != journal.Failed) || !bytes.Equal(last.Payload, terminal.Value) {
			return ErrNotTerminal
		}
		if err := l.Renew(ctx); err != nil {
			return err
		}
		if err := port.PutState(ctx, purgeKey, []byte(strconv.FormatUint(input.Sequence, 10))); err != nil {
			return err
		}
	}
	if err := stage("marker"); err != nil {
		return err
	}
	if err := l.Renew(ctx); err != nil {
		return err
	}
	if err := port.PurgeSubject(ctx, "WF_SIG", "wf.sig."+typ+"."+id+".*", 0); err != nil {
		return err
	}
	if err := stage("signals"); err != nil {
		return err
	}
	if err := l.Renew(ctx); err != nil {
		return err
	}
	if err := port.PurgeSubject(ctx, "WF_JRN", identity.JournalSubject(typ, id), 0); err != nil {
		return err
	}
	if err := stage("journal"); err != nil {
		return err
	}
	// A fallback timer can outlive the invocation by days. Remove only this
	// invocation generation before publishing its tombstone, so a later Start
	// with the same ID cannot inherit retained timer work.
	hasTimers, err := port.HasFallbackTimers(ctx)
	if err != nil {
		return err
	}
	if hasTimers {
		if err := l.Renew(ctx); err != nil {
			return err
		}
		subject := fmt.Sprintf("wf.timer.%s.%s.%d.*", typ, id, input.Sequence)
		if err := port.PurgeSubject(ctx, "WF_TIMER", subject, 0); err != nil {
			return err
		}
		if err := stage("timers"); err != nil {
			return err
		}
	}
	snapshotKey := "snap." + key
	snapshot, err := port.State(ctx, snapshotKey)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if err == nil {
		if err := port.DeleteState(ctx, snapshotKey, snapshot.Revision); err != nil {
			return err
		}
	}
	if err := stage("snapshot"); err != nil {
		return err
	}
	if err := l.Renew(ctx); err != nil {
		return err
	}
	now := port.Now().UTC()
	marker = Tombstone{Tombstone: true, InvSeq: input.Sequence, PurgedAt: now, ExpiresAt: now.Add(grace)}
	data, _ := json.Marshal(marker)
	if err := port.UpdateState(ctx, key, data, terminal.Revision); err != nil {
		current, getErr := port.State(ctx, key)
		if getErr != nil {
			return err
		}
		prior, wasTomb, decodeErr := Decode(current.Value)
		if decodeErr != nil || !wasTomb || prior.InvSeq != input.Sequence {
			return err
		}
	}
	if err := stage("tombstone"); err != nil {
		return err
	}
	if err := port.PublishPurge(ctx, typ, id, input.Sequence); err != nil {
		return err
	}
	if err := l.Renew(ctx); err != nil {
		return err
	}
	if err := port.PurgeSubject(ctx, "WF_INV", identity.InvocationSubject(typ, id), input.Sequence+1); err != nil {
		return err
	}
	if err := stage("invocation"); err != nil {
		return err
	}
	return clearPurgeMarker(ctx, port, purgeKey, input.Sequence)
}

func purgeMarker(ctx context.Context, port PurgePort, key string) (uint64, error) {
	value, err := port.State(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	seq, err := strconv.ParseUint(string(value.Value), 10, 64)
	if err != nil || seq == 0 {
		return 0, fmt.Errorf("invalid purge marker %s", key)
	}
	return seq, nil
}

func clearPurgeMarker(ctx context.Context, port PurgePort, key string, expected uint64) error {
	value, err := port.State(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	seq, err := strconv.ParseUint(string(value.Value), 10, 64)
	if err != nil {
		return err
	}
	if expected != 0 && seq != expected {
		return nil
	}
	return port.DeleteState(ctx, key, value.Revision)
}
