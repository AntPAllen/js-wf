package integrity

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type scriptedStateProgress struct {
	ticks   chan time.Time
	resets  int
	stopped bool
	onReset func(int)
}

func (p *scriptedStateProgress) C() <-chan time.Time { return p.ticks }
func (p *scriptedStateProgress) Stop()               { p.stopped = true }
func (p *scriptedStateProgress) Reset() {
	p.resets++
	if p.onReset != nil {
		p.onReset(p.resets)
	}
}

type scriptedStateWatches struct {
	jetstream.KeyValue
	watches []*auditWatch
	calls   []context.Context
	clocks  []*scriptedStateProgress
	t       *testing.T
}

func (s *scriptedStateWatches) Bucket() string { return "WF_STATE" }
func (s *scriptedStateWatches) WatchAll(ctx context.Context, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	if len(s.calls) > 0 {
		previous := len(s.calls) - 1
		if s.calls[previous].Err() == nil || !s.watches[previous].stopped || !s.clocks[previous].stopped {
			s.t.Fatal("new attempt started before prior watch/clock/context cleanup")
		}
	}
	watch := s.watches[len(s.calls)]
	s.calls = append(s.calls, ctx)
	return watch, nil
}

func TestSeededStateProgressRetryDiscardsPartialSet(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		rng := rand.New(rand.NewSource(seed))
		partial := rng.Intn(6)
		first := &auditWatch{updates: make(chan jetstream.KeyValueEntry, partial)}
		for i := 0; i < partial; i++ {
			key := "stale-only"
			if i == partial-1 {
				key = "complete" // Lower revision in a new full set must be allowed.
			}
			first.updates <- auditWatchEntry{key: key, rev: uint64(100 + i)}
		}
		second := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 2)}
		second.updates <- auditWatchEntry{key: "complete", rev: 1}
		second.updates <- nil
		clock := &scriptedStateProgress{ticks: make(chan time.Time, 1)}
		pulse := func() { clock.ticks <- time.Unix(2, 0) }
		if partial == 0 {
			pulse()
		} else {
			clock.onReset = func(count int) {
				if count == partial {
					pulse()
				}
			}
		}
		clocks := []*scriptedStateProgress{clock, {ticks: make(chan time.Time)}}
		state := &scriptedStateWatches{watches: []*auditWatch{first, second}, clocks: clocks, t: t}
		ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
		deadline, _ := ctx.Deadline()
		value, err := auditStateRead(ctx, func(call context.Context) (jetstream.KeyValue, error) {
			actual, _ := call.Deadline()
			if !actual.Equal(deadline) {
				t.Fatal("progress retry changed the full original deadline")
			}
			return initialAuditStateUsingProgress(call, state, func(string) bool { return true }, func() auditStateProgressTimer {
				return clocks[len(state.calls)-1]
			})
		})
		stop()
		if err != nil || len(state.calls) != 2 || !first.stopped || !second.stopped || !clocks[0].stopped || !clocks[1].stopped {
			t.Fatalf("seed=%d partial=%d err=%v attempts=%d", seed, partial, err, len(state.calls))
		}
		if _, err := value.Get(context.Background(), "stale-only"); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal("abandoned partial set leaked into successful full snapshot")
		}
		entry, err := value.Get(context.Background(), "complete")
		if err != nil || entry.Revision() != 1 {
			t.Fatal("fresh complete set was replaced by abandoned partial revision")
		}
	}
}

func TestStateProgressBufferedUpdatesAndBarrierWinIdleTick(t *testing.T) {
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 1)}
	watch.updates <- auditWatchEntry{key: "included", rev: 1}
	clock := &scriptedStateProgress{ticks: make(chan time.Time, 1)}
	clock.ticks <- time.Unix(2, 0)
	clock.onReset = func(count int) {
		select {
		case <-clock.ticks:
		default:
		}
		if count < 8 {
			watch.updates <- auditWatchEntry{key: "excluded", rev: uint64(count + 1)}
		} else if count == 8 {
			watch.updates <- nil
		} else {
			return
		}
		clock.ticks <- time.Unix(int64(count*2), 0)
	}
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	value, err := initialAuditStateUsingProgress(ctx, auditWatchState{watch: watch}, func(key string) bool { return key != "excluded" }, func() auditStateProgressTimer { return clock })
	if err != nil || value == nil || clock.resets != 9 || !clock.stopped || !watch.stopped {
		t.Fatalf("buffered progress/barrier lost: %v resets=%d", err, clock.resets)
	}
	if _, err := value.Get(ctx, "excluded"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal("excluded progress was included in cohort")
	}
}

func TestStateProgressBoundedRetryAndCancellation(t *testing.T) {
	state := &scriptedStateWatches{t: t}
	for range 3 {
		state.watches = append(state.watches, &auditWatch{updates: make(chan jetstream.KeyValueEntry)})
		clock := &scriptedStateProgress{ticks: make(chan time.Time, 1)}
		clock.ticks <- time.Unix(2, 0)
		state.clocks = append(state.clocks, clock)
	}
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	value, err := auditStateRead(ctx, func(call context.Context) (jetstream.KeyValue, error) {
		return initialAuditStateUsingProgress(call, state, func(string) bool { return true }, func() auditStateProgressTimer {
			return state.clocks[len(state.calls)-1]
		})
	})
	if value != nil || !errors.Is(err, nats.ErrTimeout) || len(state.calls) != 3 || !state.clocks[2].stopped || !state.watches[2].stopped {
		t.Fatalf("silent retries unbounded or partial set accepted: %v attempts=%d", err, len(state.calls))
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 1)}
	watch.updates <- nil
	clock := &scriptedStateProgress{ticks: make(chan time.Time, 1)}
	clock.ticks <- time.Unix(2, 0)
	value, err = initialAuditStateUsingProgress(canceled, auditWatchState{watch: watch}, func(string) bool { return true }, func() auditStateProgressTimer { return clock })
	if value != nil || !errors.Is(err, context.Canceled) || !clock.stopped || !watch.stopped {
		t.Fatal("cancellation lost to buffered completion/idle tick")
	}
}
