package retention

import (
	"bytes"
	"context"
	"errors"
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
	keys, err := state.Keys(ctx)
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
		entry, err := state.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		result.Inspected++
		if raw := bytes.TrimSpace(entry.Value()); len(raw) == 0 || raw[0] != '{' {
			continue
		}
		marker, tomb, err := Decode(entry.Value())
		if err != nil {
			return result, err
		}
		if !tomb || now.Before(marker.ExpiresAt) {
			continue
		}
		result.Expired++
		current, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(parts[0], parts[1]))
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return result, err
		}
		if err == nil && current.Sequence <= marker.InvSeq {
			continue
		}
		if err := state.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
			if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return result, err
		}
		result.Deleted++
	}
	return result, nil
}
