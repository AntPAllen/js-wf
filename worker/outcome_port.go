package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/lease"
	"js-wf/retention"

	"github.com/nats-io/nats.go/jetstream"
)

// OutcomePort contains the KV decisions used to persist a terminal outcome.
type OutcomePort interface {
	Create(context.Context, string, []byte) (uint64, error)
	Get(context.Context, string) (lease.KVEntry, error)
	Update(context.Context, string, []byte, uint64) (uint64, error)
}

type jetStreamOutcomePort struct{ kv jetstream.KeyValue }

func NewOutcomePort(kv jetstream.KeyValue) OutcomePort { return jetStreamOutcomePort{kv: kv} }

func (p jetStreamOutcomePort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	return p.kv.Create(ctx, key, value)
}

func (p jetStreamOutcomePort) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	entry, err := p.kv.Get(ctx, key)
	if err != nil {
		return lease.KVEntry{}, err
	}
	return lease.KVEntry{Value: entry.Value(), Revision: entry.Revision()}, nil
}

func (p jetStreamOutcomePort) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return p.kv.Update(ctx, key, value, revision)
}

// PersistOutcomeWithPort makes a terminal result immutable while allowing a
// later invocation generation to replace an older purge tombstone by CAS.
func PersistOutcomeWithPort(ctx context.Context, port OutcomePort, typ, id string, invSeq uint64, payload []byte) error {
	if err := identity.Validate(typ, id); err != nil {
		return err
	}
	if invSeq == 0 {
		return fmt.Errorf("invocation sequence must be positive")
	}
	key := identity.Key(typ, id)
	_, err := port.Create(ctx, key, payload)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	previous, err := port.Get(ctx, key)
	if err != nil {
		return err
	}
	marker, tomb, err := retention.Decode(previous.Value)
	if err != nil {
		return err
	}
	if tomb {
		if marker.InvSeq >= invSeq {
			return client.ErrPurged
		}
		_, err := port.Update(ctx, key, payload, previous.Revision)
		if err == nil {
			return nil
		}
		current, getErr := port.Get(ctx, key)
		if getErr == nil && bytes.Equal(current.Value, payload) {
			return nil
		}
		return err
	}
	if !bytes.Equal(previous.Value, payload) {
		return fmt.Errorf("terminal result changed for %s", key)
	}
	return nil
}
