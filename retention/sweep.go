package retention

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"js-wf/identity"

	"github.com/nats-io/nats.go/jetstream"
)

type SweepResult struct {
	Inspected int `json:"inspected"`
	Expired   int `json:"expired"`
	Deleted   int `json:"deleted"`
}

// TombstoneSweepPort is the retained state, generation lookup, and CAS delete
// boundary used by the production tombstone sweep.
type TombstoneSweepPort interface {
	StateKeys(context.Context) ([]string, error)
	StateValue(context.Context, string) ([]byte, uint64, error)
	CurrentInvocation(context.Context, string) (uint64, error)
	DeleteState(context.Context, string, uint64) error
}

type jetStreamTombstoneSweepPort struct {
	state jetstream.KeyValue
	inv   jetstream.Stream
}

func (p jetStreamTombstoneSweepPort) StateKeys(ctx context.Context) ([]string, error) {
	return p.state.Keys(ctx)
}

func (p jetStreamTombstoneSweepPort) StateValue(ctx context.Context, key string) ([]byte, uint64, error) {
	entry, err := p.state.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return entry.Value(), entry.Revision(), nil
}

func (p jetStreamTombstoneSweepPort) CurrentInvocation(ctx context.Context, subject string) (uint64, error) {
	message, err := p.inv.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return 0, err
	}
	return message.Sequence, nil
}

func (p jetStreamTombstoneSweepPort) DeleteState(ctx context.Context, key string, revision uint64) error {
	return p.state.Delete(ctx, key, jetstream.LastRevision(revision))
}

// SweepTombstones removes expired tombstones after the retired invocation has
// disappeared. A revision condition protects a newer generation's result.
// The caller supplies now so runs can use a controlled clock in tests.
func SweepTombstones(ctx context.Context, js jetstream.JetStream, now time.Time) (SweepResult, error) {
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return SweepResult{}, err
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return SweepResult{}, err
	}
	return SweepTombstonesWithPort(ctx, jetStreamTombstoneSweepPort{state: state, inv: inv}, now)
}

// SweepTombstonesWithPort runs the production sweep loop over a supplied
// retained-state and invocation transport.
func SweepTombstonesWithPort(ctx context.Context, port TombstoneSweepPort, now time.Time) (SweepResult, error) {
	if port == nil || now.IsZero() {
		return SweepResult{}, fmt.Errorf("invalid tombstone sweep port or clock")
	}
	keys, err := port.StateKeys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return SweepResult{}, nil
	}
	if err != nil {
		return SweepResult{}, err
	}
	var result SweepResult
	for _, key := range keys {
		parts := strings.Split(key, ".")
		if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
			continue
		}
		value, revision, err := port.StateValue(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		result.Inspected++
		expired, _, deleted, err := SweepCandidate(ctx, port, key, value, revision, now, false)
		if err != nil {
			return result, err
		}
		if expired {
			result.Expired++
		}
		if deleted {
			result.Deleted++
		}
	}
	return result, nil
}

// SweepCandidate applies the production expiry, generation, and revision
// guards to one state value. A retry after an uncertain delete reads the
// current KV value before calling this again.
func SweepCandidate(ctx context.Context, port TombstoneSweepPort, key string, value []byte, revision uint64, now time.Time, dryRun bool) (expired, eligible, deleted bool, err error) {
	parts := strings.Split(key, ".")
	if port == nil || len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil || revision == 0 || now.IsZero() {
		return false, false, false, fmt.Errorf("invalid tombstone sweep candidate")
	}
	if raw := bytes.TrimSpace(value); len(raw) == 0 || raw[0] != '{' {
		return false, false, false, nil
	}
	marker, tomb, err := Decode(value)
	if err != nil || !tomb || now.Before(marker.ExpiresAt) {
		return false, false, false, err
	}
	expired = true
	current, err := port.CurrentInvocation(ctx, identity.InvocationSubject(parts[0], parts[1]))
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return expired, false, false, err
	}
	if err == nil && current <= marker.InvSeq {
		return expired, false, false, nil
	}
	if dryRun {
		return expired, true, false, nil
	}
	if err := port.DeleteState(ctx, key, revision); err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyNotFound) {
			return expired, true, false, nil
		}
		return expired, true, false, err
	}
	return expired, true, true, nil
}
