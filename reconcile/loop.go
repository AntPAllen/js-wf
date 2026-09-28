package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/lease"

	"github.com/nats-io/nats.go"
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
	var api *jetstream.APIError
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) ||
		errors.As(err, &api) && api.ErrorCode == 10164 {
		return 0, ErrCursorStale
	}
	return newRevision, err
}

func runLoop(ctx context.Context, js jetstream.JetStream, workerID, kind string, interval time.Duration, budget int, scan scanFunc) error {
	if interval <= 0 || interval > 10*time.Second || budget < 1 {
		return fmt.Errorf("invalid reconcile cadence or budget")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	wait := func() {
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	release := func(l *lease.Lease) {
		releaseCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = l.Release(releaseCtx)
	}
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		leasing, err := lease.New(attempt, js)
		var state jetstream.KeyValue
		if err == nil {
			state, err = js.KeyValue(attempt, "WF_STATE")
		}
		stop()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) {
				return err
			}
			wait()
			continue
		}
		attempt, stop = context.WithTimeout(ctx, 5*time.Second)
		l, err := leasing.Acquire(attempt, "system", kind+"-reconciler", workerID)
		stop()
		if errors.Is(err, lease.ErrHeld) {
			wait()
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) {
				return err
			}
			wait()
			continue
		}
		attempt, stop = context.WithTimeout(ctx, 5*time.Second)
		cursor, revision, err := loadCursor(attempt, state, kind)
		stop()
		if err != nil {
			release(l)
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) {
				return err
			}
			wait()
			continue
		}
		for ctx.Err() == nil {
			attempt, stop = context.WithTimeout(ctx, 5*time.Second)
			err = l.Renew(attempt)
			stop()
			if err != nil {
				break
			}
			attempt, stop = context.WithTimeout(ctx, 5*time.Second)
			result, err := scan(attempt, cursor, budget, false)
			stop()
			if err != nil {
				if !retryableReconcileError(err) && ctx.Err() == nil {
					release(l)
					return err
				}
				break
			}
			attempt, stop = context.WithTimeout(ctx, 5*time.Second)
			err = l.Renew(attempt)
			stop()
			if err != nil {
				break
			}
			attempt, stop = context.WithTimeout(ctx, 5*time.Second)
			revision, err = saveCursor(attempt, state, kind, result.NextSequence, revision)
			stop()
			if errors.Is(err, ErrCursorStale) {
				break
			}
			if err != nil {
				if !retryableReconcileError(err) && ctx.Err() == nil {
					release(l)
					return err
				}
				break
			}
			cursor = result.NextSequence
			wait()
		}
		release(l)
		wait()
	}
	return nil
}

func retryableReconcileError(err error) bool {
	if errors.Is(err, lease.ErrLost) {
		return true
	}
	var api *jetstream.APIError
	if errors.As(err, &api) && (api.ErrorCode == 10008 || api.ErrorCode == 10164) {
		return true
	}
	return errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionReconnecting) || errors.Is(err, nats.ErrNoServers) ||
		errors.Is(err, context.DeadlineExceeded)
}
