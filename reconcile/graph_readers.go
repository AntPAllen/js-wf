package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/lease"
)

const readerExpiryKind = "graph-reader-expiry"

// ReaderExpiryPort persists the entire watermark/cursor pair atomically. A zero
// cursor means a new pass; uncertain saves must be resolved by reloading.
type ReaderExpiryPort interface {
	Prepare(context.Context) error
	Acquire(context.Context, string, string) (LoopLease, error)
	LoadReaderCursor(context.Context) (graphpublication.ReaderSweepCursor, uint64, error)
	SaveReaderCursor(context.Context, graphpublication.ReaderSweepCursor, uint64) (uint64, error)
	Wait(context.Context, time.Duration) error
}

type ReaderExpiryProtocol interface {
	BeginReaderSweep(context.Context) (graphpublication.ReaderSweepCursor, error)
	ExpireReaderBatch(context.Context, graphpublication.ReaderSweepCursor, int, time.Time) (graphpublication.ReaderSweepResult, error)
}

type nativeReaderExpiryPort struct{ *jetStreamLoopPort }

func validReaderCursor(c graphpublication.ReaderSweepCursor) bool {
	return c == (graphpublication.ReaderSweepCursor{}) || c.Through != math.MaxUint64 && c.Next > 0 && c.Next <= c.Through+1
}

func (p nativeReaderExpiryPort) LoadReaderCursor(ctx context.Context) (graphpublication.ReaderSweepCursor, uint64, error) {
	entry, err := p.state.Get(ctx, "scan."+readerExpiryKind)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return graphpublication.ReaderSweepCursor{}, 0, nil
	}
	if err != nil {
		return graphpublication.ReaderSweepCursor{}, 0, err
	}
	var stored struct {
		Version int
		Cursor  *graphpublication.ReaderSweepCursor
	}
	if err := json.Unmarshal(entry.Value(), &stored); err != nil || stored.Version != 1 || stored.Cursor == nil || !validReaderCursor(*stored.Cursor) {
		return graphpublication.ReaderSweepCursor{}, 0, fmt.Errorf("invalid reader expiry checkpoint")
	}
	cursor := *stored.Cursor
	return cursor, entry.Revision(), nil
}

func (p nativeReaderExpiryPort) SaveReaderCursor(ctx context.Context, cursor graphpublication.ReaderSweepCursor, revision uint64) (uint64, error) {
	if !validReaderCursor(cursor) {
		return 0, fmt.Errorf("invalid reader expiry checkpoint")
	}
	data, err := json.Marshal(struct {
		Version int
		Cursor  graphpublication.ReaderSweepCursor
	}{1, cursor})
	if err != nil {
		return 0, err
	}
	var next uint64
	if revision == 0 {
		next, err = p.state.Create(ctx, "scan."+readerExpiryKind, data)
	} else {
		next, err = p.state.Update(ctx, "scan."+readerExpiryKind, data, revision)
	}
	var api *jetstream.APIError
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.As(err, &api) && api.ErrorCode == 10164 {
		return 0, ErrCursorStale
	}
	return next, err
}

// RunGraphReaderExpiry is explicitly enabled experimental reader maintenance.
// It never collects objects and is not installed in the default runtime.
func RunGraphReaderExpiry(ctx context.Context, js jetstream.JetStream, protocol graphpublication.Protocol, workerID string, interval time.Duration, budget int) error {
	if interval <= 0 || interval > 10*time.Second {
		return fmt.Errorf("invalid reader expiry cadence")
	}
	p := &jetStreamLoopPort{js: js, ticker: time.NewTicker(interval)}
	defer p.ticker.Stop()
	return RunReaderExpiryWithPort(ctx, nativeReaderExpiryPort{p}, protocol, workerID, interval, budget, time.Now)
}

// RunReaderExpiryWithPort shares the production scheduler with deterministic
// transports. Renew precedes every batch and checkpoint. CAS prevents a stale
// leader from replacing a successor's checkpoint; root fencing remains in the
// graph protocol even if a lease expires during an in-flight batch.
func RunReaderExpiryWithPort(ctx context.Context, port ReaderExpiryPort, protocol ReaderExpiryProtocol, workerID string, interval time.Duration, budget int, now func() time.Time) error {
	if port == nil || protocol == nil || now == nil || workerID == "" || interval <= 0 || interval > 10*time.Second || budget < 1 || budget > graphpublication.MaxReaderSweepBatch {
		return fmt.Errorf("invalid reader expiry configuration")
	}
	attempt := func(f func(context.Context) error) error {
		call, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		return f(call)
	}
	wait := func() error {
		if ctx.Err() != nil {
			return nil
		}
		return port.Wait(ctx, interval)
	}
	for ctx.Err() == nil {
		err := attempt(port.Prepare)
		var held LoopLease
		if err == nil {
			err = attempt(func(c context.Context) error {
				var e error
				held, e = port.Acquire(c, readerExpiryKind, workerID)
				return e
			})
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if !retryableReconcileError(err) && !errors.Is(err, lease.ErrHeld) {
				return err
			}
			if err = wait(); err != nil {
				return err
			}
			continue
		}
		err = func() error {
			defer func() {
				c, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_ = held.Release(c)
			}()
			var cursor graphpublication.ReaderSweepCursor
			var revision uint64
			if err := attempt(func(c context.Context) error { var e error; cursor, revision, e = port.LoadReaderCursor(c); return e }); err != nil {
				return err
			}
			if !validReaderCursor(cursor) {
				return fmt.Errorf("invalid reader expiry checkpoint")
			}
			for ctx.Err() == nil {
				if err := attempt(held.Renew); err != nil {
					return err
				}
				if cursor == (graphpublication.ReaderSweepCursor{}) {
					if err := attempt(func(c context.Context) error { var e error; cursor, e = protocol.BeginReaderSweep(c); return e }); err != nil {
						return err
					}
					if cursor.Next != 1 || !validReaderCursor(cursor) {
						return fmt.Errorf("invalid reader expiry watermark")
					}
					// Persist the captured watermark before doing any batch work.
				} else {
					var result graphpublication.ReaderSweepResult
					scanErr := attempt(func(c context.Context) error {
						var e error
						result, e = protocol.ExpireReaderBatch(c, cursor, budget, now())
						return e
					})
					if !validReaderCursor(result.Cursor) || result.Cursor.Through != cursor.Through || result.Cursor.Next < cursor.Next || result.Inspected < 0 || result.Inspected > budget || result.Complete && result.Cursor.Next <= cursor.Through {
						return fmt.Errorf("invalid reader expiry batch checkpoint")
					}
					if scanErr != nil && result.Cursor == cursor {
						return scanErr
					}
					if result.Complete && scanErr == nil {
						cursor = graphpublication.ReaderSweepCursor{}
					} else {
						cursor = result.Cursor
					}
					if err := attempt(held.Renew); err != nil {
						return err
					}
					if err := attempt(func(c context.Context) error {
						var e error
						revision, e = port.SaveReaderCursor(c, cursor, revision)
						return e
					}); err != nil {
						return err
					}
					if scanErr != nil {
						return scanErr
					}
					if err := wait(); err != nil {
						return err
					}
					continue
				}
				if err := attempt(held.Renew); err != nil {
					return err
				}
				if err := attempt(func(c context.Context) error {
					var e error
					revision, e = port.SaveReaderCursor(c, cursor, revision)
					return e
				}); err != nil {
					return err
				}
			}
			return nil
		}()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && !retryableReconcileError(err) && !errors.Is(err, ErrCursorStale) {
			return err
		}
		if err = wait(); err != nil {
			return err
		}
	}
	return nil
}
