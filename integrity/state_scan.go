package integrity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Explicit parallel-checker candidate: a silent watch must not consume the
// whole audit deadline. Progress resets this idle clock; a large active initial
// set still receives the caller's original complete-read budget.
const auditStateProgressInterval = 2 * time.Second

type auditStateProgressTimer interface {
	C() <-chan time.Time
	Reset()
	Stop()
}

type realAuditStateProgressTimer struct{ timer *time.Timer }

func (t realAuditStateProgressTimer) C() <-chan time.Time { return t.timer.C }
func (t realAuditStateProgressTimer) Stop()               { t.timer.Stop() }
func (t realAuditStateProgressTimer) Reset() {
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(auditStateProgressInterval)
}

func initialAuditStateWithProgressTimeout(ctx context.Context, state jetstream.KeyValue, include func(string) bool) (jetstream.KeyValue, error) {
	clock := func() auditStateProgressTimer {
		return realAuditStateProgressTimer{timer: time.NewTimer(auditStateProgressInterval)}
	}
	return initialAuditStateUsingTimers(ctx, state, include, clock, clock)
}

// Watch creation has no entry progress to reset a clock. Cancel its request
// after the same idle interval without shortening the successful initial-set
// read. Join the watchdog before returning; WatchAll itself remains synchronous
// and must honor its request context. No abandoned creation goroutine is used.
func createAuditStateWatch(ctx context.Context, state jetstream.KeyValue, createTimer func() auditStateProgressTimer) (jetstream.KeyWatcher, context.CancelFunc, error) {
	if createTimer == nil {
		watch, err := state.WatchAll(ctx)
		return watch, func() {}, err
	}
	call, cancel := context.WithCancel(ctx)
	clock := createTimer()
	defer clock.Stop()
	created, joined := make(chan struct{}), make(chan struct{})
	timedOut := false
	go func() {
		defer close(joined)
		select {
		case <-created:
		case <-call.Done():
		case <-clock.C():
			select {
			case <-created:
				return
			default:
			}
			if ctx.Err() == nil {
				timedOut = true
				cancel()
			}
		}
	}()
	watch, err := state.WatchAll(call)
	close(created)
	<-joined
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if timedOut {
		err = fmt.Errorf("state watch creation made no progress: %w", nats.ErrTimeout)
	}
	return watch, cancel, err
}

// The documented nil watch entry marks completion of the initial latest-value
// set. A closed channel, timeout or malformed entry never certifies a partial
// set. The caller bounds and retries the entire snapshot, not just creation.
func initialAuditState(ctx context.Context, state jetstream.KeyValue, include func(string) bool) (result jetstream.KeyValue, err error) {
	return initialAuditStateUsingProgress(ctx, state, include, nil)
}

func initialAuditStateUsingProgress(ctx context.Context, state jetstream.KeyValue, include func(string) bool, createTimer func() auditStateProgressTimer) (result jetstream.KeyValue, err error) {
	return initialAuditStateUsingTimers(ctx, state, include, createTimer, nil)
}

func initialAuditStateUsingTimers(ctx context.Context, state jetstream.KeyValue, include func(string) bool, createTimer, createCreationTimer func() auditStateProgressTimer) (result jetstream.KeyValue, err error) {
	// Preserve the underlying error for retry classification while identifying a
	// failure before the initial-set barrier. Iterator completion alone cannot
	// show whether this later snapshot phase received any state records.
	phase := "watch creation"
	received, included := 0, 0
	var lastRevision uint64
	observer, _ := ctx.Value(stateSnapshotObserverKey{}).(func(StateSnapshotObservation))
	started := time.Now()
	deadline, hasDeadline := ctx.Deadline()
	complete := false
	emit := func(event string, failure error) {
		if observer == nil {
			return
		}
		frame := StateSnapshotObservation{Event: event, Time: time.Now().UTC(), ElapsedNS: time.Since(started).Nanoseconds(), Received: received, Included: included, LastRevision: lastRevision, InitialComplete: complete}
		if hasDeadline {
			frame.Deadline, frame.BudgetNS = deadline.UTC(), deadline.Sub(started).Nanoseconds()
		}
		if failure != nil {
			frame.Error = failure.Error()
		}
		observer(frame)
	}
	emit("watch_start", nil)
	defer func() {
		if err != nil {
			err = fmt.Errorf("retained state snapshot %s: received=%d included=%d last_revision=%d initial_complete=false: %w", phase, received, included, lastRevision, err)
		}
		emit("attempt_return", err)
	}()
	watch, cancelWatch, err := createAuditStateWatch(ctx, state, createCreationTimer)
	defer cancelWatch()
	if watch != nil {
		defer func() {
			emit("watch_stop_start", nil)
			stopError := watch.Stop()
			defer emit("watch_stopped", stopError)
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
	}
	if err != nil {
		emit("watch_creation_error", err)
		return nil, err
	}
	if watch == nil {
		return nil, errors.New("retained state watch creation returned no watcher")
	}
	emit("watch_created", nil)
	phase = "initial set"
	var progressTimer auditStateProgressTimer
	var progress <-chan time.Time
	if createTimer != nil {
		progressTimer = createTimer()
		progress = progressTimer.C()
		defer progressTimer.Stop()
	}
	values := make(map[string]jetstream.KeyValueEntry)
	for {
		var entry jetstream.KeyValueEntry
		var ok bool
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-progress:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Buffered progress wins over an idle tick delayed by scheduling.
			// A nil entry is still the native initial-completion barrier.
			select {
			case entry, ok = <-watch.Updates():
			default:
				failure := fmt.Errorf("state watch made no progress: %w", nats.ErrTimeout)
				emit("watch_idle_timeout", failure)
				return nil, failure
			}
		case entry, ok = <-watch.Updates():
		}
		if !ok {
			if createTimer != nil {
				// The explicit recovery candidate retries a lost real subscription,
				// discarding its partial set under the same caller deadline.
				return nil, fmt.Errorf("retained state watch closed before initial completion: %w", nats.ErrTimeout)
			}
			return nil, errors.New("retained state watch closed before initial completion")
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if progressTimer != nil {
			progressTimer.Reset()
		}
		if entry == nil {
			complete = true
			emit("initial_complete", nil)
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
			if received%10000 == 0 {
				emit("progress", nil)
			}
			continue
		}
		if prior := values[entry.Key()]; prior != nil && entry.Revision() <= prior.Revision() {
			return nil, errors.New("retained state watch revisions out of order")
		}
		included++
		values[entry.Key()] = entry
		if received%10000 == 0 {
			emit("progress", nil)
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
