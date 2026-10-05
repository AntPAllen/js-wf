package integrity

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// The documented nil watch entry marks completion of the initial latest-value
// set. A closed channel, timeout or malformed entry never certifies a partial
// set. The caller bounds and retries the entire snapshot, not just creation.
func initialAuditState(ctx context.Context, state jetstream.KeyValue, include func(string) bool) (result jetstream.KeyValue, err error) {
	// Preserve the underlying error for retry classification while identifying a
	// failure before the initial-set barrier. Iterator completion alone cannot
	// show whether this later snapshot phase received any state records.
	phase := "watch creation"
	received, included := 0, 0
	var lastRevision uint64
	defer func() {
		if err != nil {
			err = fmt.Errorf("retained state snapshot %s: received=%d included=%d last_revision=%d initial_complete=false: %w", phase, received, included, lastRevision, err)
		}
	}()
	watch, err := state.WatchAll(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = watch.Stop()
		// Release buffered sends from the SDK's synchronous watch callback.
		// After unsubscribe, only already queued updates can remain.
		for {
			select {
			case _, ok := <-watch.Updates():
				if !ok {
					return
				}
			default:
				return
			}
		}
	}()
	phase = "initial set"
	values := make(map[string]jetstream.KeyValueEntry)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case entry, ok := <-watch.Updates():
			if !ok {
				return nil, errors.New("retained state watch closed before initial completion")
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if entry == nil {
				return &auditStateSnapshot{KeyValue: state, values: values}, nil
			}
			received++
			lastRevision = entry.Revision()
			if entry.Bucket() != state.Bucket() || entry.Key() == "" || entry.Revision() == 0 {
				return nil, errors.New("retained state watch invalid entry identity")
			}
			switch entry.Operation() {
			case jetstream.KeyValuePut, jetstream.KeyValueDelete, jetstream.KeyValuePurge:
			default:
				return nil, fmt.Errorf("retained state watch invalid operation %v", entry.Operation())
			}
			if !include(entry.Key()) {
				continue
			}
			if prior := values[entry.Key()]; prior != nil && entry.Revision() <= prior.Revision() {
				return nil, errors.New("retained state watch revisions out of order")
			}
			included++
			values[entry.Key()] = entry
		}
	}
}

// Only the checker's read operations use this immutable per-audit view.
type auditStateSnapshot struct {
	jetstream.KeyValue
	values map[string]jetstream.KeyValueEntry
}

func (s *auditStateSnapshot) Keys(ctx context.Context, opts ...jetstream.WatchOpt) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(opts) != 0 {
		return nil, errors.New("retained state snapshot does not support watch options")
	}
	var keys []string
	for key, entry := range s.values {
		if entry.Operation() == jetstream.KeyValuePut {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil, jetstream.ErrNoKeysFound
	}
	return keys, nil
}

func (s *auditStateSnapshot) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry := s.values[key]
	if entry == nil || entry.Operation() != jetstream.KeyValuePut {
		return nil, jetstream.ErrKeyNotFound
	}
	return entry, nil
}
