package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrCursorStale = errors.New("reconciler cursor changed concurrently")

type scanFunc func(context.Context, uint64, int, bool) (ScanResult, error)

// LoopLease is the fenced lease held while a reconciler scans and saves its
// cursor. A lost lease prevents that scanner from publishing another cursor.
type LoopLease interface {
	Renew(context.Context) error
	Release(context.Context) error
}

// LoopPort is the narrow lease, cursor, and cadence boundary for all scanner
// loops. The scan callback retains the production repair decisions.
type LoopPort interface {
	Prepare(context.Context) error
	Acquire(context.Context, string, string) (LoopLease, error)
	LoadCursor(context.Context, string) (uint64, uint64, error)
	SaveCursor(context.Context, string, uint64, uint64) (uint64, error)
	Wait(context.Context, time.Duration) error
}

type jetStreamLoopPort struct {
	js      jetstream.JetStream
	ticker  *time.Ticker
	leasing *lease.Store
	state   jetstream.KeyValue
}

func (p *jetStreamLoopPort) Prepare(ctx context.Context) error {
	leasing, err := lease.New(ctx, p.js)
	if err != nil {
		return err
	}
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return err
	}
	p.leasing, p.state = leasing, state
	return nil
}

func (p *jetStreamLoopPort) Acquire(ctx context.Context, kind, workerID string) (LoopLease, error) {
	return p.leasing.Acquire(ctx, "system", kind+"-reconciler", workerID)
}

func (p *jetStreamLoopPort) LoadCursor(ctx context.Context, kind string) (uint64, uint64, error) {
	return loadCursor(ctx, p.state, kind)
}

func (p *jetStreamLoopPort) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	return saveCursor(ctx, p.state, kind, next, revision)
}

func (p *jetStreamLoopPort) Wait(ctx context.Context, _ time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ticker.C:
		return nil
	}
}

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
	port := &jetStreamLoopPort{js: js, ticker: time.NewTicker(interval)}
	defer port.ticker.Stop()
	return RunLoopWithPort(ctx, port, workerID, kind, interval, budget, scan)
}

// RunLoopWithPort runs the production lease, scan, and cursor state machine
// against a real or deterministic transport.
func RunLoopWithPort(ctx context.Context, port LoopPort, workerID, kind string, interval time.Duration, budget int, scan func(context.Context, uint64, int, bool) (ScanResult, error)) error {
	if interval <= 0 || interval > 10*time.Second || budget < 1 {
		return fmt.Errorf("invalid reconcile cadence or budget")
	}
	if port == nil || scan == nil {
		return fmt.Errorf("missing reconcile transport or scan")
	}
	wait := func() error {
		err := port.Wait(ctx, interval)
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	release := func(l LoopLease) {
		releaseCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = l.Release(releaseCtx)
	}
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err := port.Prepare(attempt)
		stop()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) {
				return err
			}
			if err := wait(); err != nil {
				return err
			}
			continue
		}
		attempt, stop = context.WithTimeout(ctx, 5*time.Second)
		l, err := port.Acquire(attempt, kind, workerID)
		stop()
		if errors.Is(err, lease.ErrHeld) {
			if err := wait(); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) {
				return err
			}
			if err := wait(); err != nil {
				return err
			}
			continue
		}
		attempt, stop = context.WithTimeout(ctx, 5*time.Second)
		cursor, revision, err := port.LoadCursor(attempt, kind)
		stop()
		if err != nil {
			release(l)
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) {
				return err
			}
			if err := wait(); err != nil {
				return err
			}
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
				// Only scanners that certify a completed prefix may checkpoint
				// a failed pass. Renew ownership with a fresh attempt before
				// cursor CAS; an expired scan context cannot authorize a save.
				if ctx.Err() == nil && result.RetrySequence > cursor {
					attempt, stop = context.WithTimeout(ctx, 5*time.Second)
					checkpointErr := l.Renew(attempt)
					stop()
					if checkpointErr == nil {
						attempt, stop = context.WithTimeout(ctx, 5*time.Second)
						_, checkpointErr = port.SaveCursor(attempt, kind, result.RetrySequence, revision)
						stop()
					}
					if checkpointErr != nil && ctx.Err() == nil && !retryableReconcileError(checkpointErr) && !errors.Is(checkpointErr, ErrCursorStale) {
						release(l)
						return checkpointErr
					}
				}
				// Reacquire and reread after uncertain checkpoint replies.
				break
			}
			attempt, stop = context.WithTimeout(ctx, 5*time.Second)
			err = l.Renew(attempt)
			stop()
			if err != nil {
				break
			}
			attempt, stop = context.WithTimeout(ctx, 5*time.Second)
			revision, err = port.SaveCursor(attempt, kind, result.NextSequence, revision)
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
			if err := wait(); err != nil {
				release(l)
				return err
			}
		}
		release(l)
		if err := wait(); err != nil {
			return err
		}
	}
	return nil
}

func retryableReconcileError(err error) bool {
	if errors.Is(err, lease.ErrLost) || errors.Is(err, journal.ErrUnknown) || errors.Is(err, graphpublication.ErrConflict) || errors.Is(err, graphpublication.ErrRevoked) {
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
