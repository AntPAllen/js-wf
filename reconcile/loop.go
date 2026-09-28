package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/lease"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrCursorStale = errors.New("reconciler cursor changed concurrently")

type scanFunc func(context.Context, uint64, int, bool) (ScanResult, error)

func loadCursor(ctx context.Context, state jetstream.KeyValue, kind string) (uint64, uint64, error) {
	entry, err := state.Get(ctx, "scan."+kind)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 1, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	seq, err := strconv.ParseUint(string(entry.Value()), 10, 64)
	if err != nil || seq == 0 {
		return 0, 0, fmt.Errorf("invalid %s scan cursor", kind)
	}
	return seq, entry.Revision(), nil
}

func saveCursor(ctx context.Context, state jetstream.KeyValue, kind string, next, revision uint64) (uint64, error) {
	if next == 0 {
		return 0, fmt.Errorf("zero scan cursor")
	}
	key := "scan." + kind
	data := []byte(strconv.FormatUint(next, 10))
	var newRevision uint64
	var err error
	if revision == 0 {
		newRevision, err = state.Create(ctx, key, data)
	} else {
		newRevision, err = state.Update(ctx, key, data, revision)
	}
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, ErrCursorStale
	}
	return newRevision, err
}

func runLoop(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, scan scanFunc) error {
	if interval <= 0 || interval > 10*time.Second || budget < 1 {
		return fmt.Errorf("invalid reconcile cadence or budget")
	}
	leasing, err := lease.New(ctx, js)
	if err != nil {
		return err
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		l, err := leasing.Acquire(ctx, "system", kind+"-reconciler", workerID)
		if errors.Is(err, lease.ErrHeld) {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				continue
			}
		}
		if err != nil {
			return err
		}
		cursor, revision, err := loadCursor(ctx, state, kind)
		if err != nil {
			_ = l.Release(context.Background())
			return err
		}
		for ctx.Err() == nil {
			if err := l.Renew(ctx); err != nil {
				break
			}
			result, err := scan(ctx, cursor, budget, false)
			if err != nil {
				_ = l.Release(context.Background())
				return err
			}
			if err := l.Renew(ctx); err != nil {
				break
			}
			revision, err = saveCursor(ctx, state, kind, result.NextSequence, revision)
			if errors.Is(err, ErrCursorStale) {
				break
			}
			if err != nil {
				_ = l.Release(context.Background())
				return err
			}
			cursor = result.NextSequence
			select {
			case <-ctx.Done():
			case <-ticker.C:
			}
		}
		_ = l.Release(context.Background())
	}
	return nil
}
