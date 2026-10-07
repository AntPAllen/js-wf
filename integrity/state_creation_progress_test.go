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

type creationProgressState struct {
	jetstream.KeyValue
	t        *testing.T
	stalls   int
	calls    []context.Context
	watches  []*auditWatch
	clocks   []*scriptedStateProgress
	deadline time.Time
}

func (*creationProgressState) Bucket() string { return "WF_STATE" }
func (s *creationProgressState) WatchAll(ctx context.Context, _ ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	if d, ok := ctx.Deadline(); !ok || !d.Equal(s.deadline) {
		s.t.Fatal("creation watchdog changed the original full-set deadline")
	}
	if n := len(s.calls); n > 0 && (s.calls[n-1].Err() == nil || !s.watches[n-1].stopped || !s.clocks[n-1].stopped) {
		s.t.Fatal("retry began before creation context, timer and late watcher cleanup")
	}
	s.calls = append(s.calls, ctx)
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 2)}
	s.watches = append(s.watches, watch)
	if len(s.calls) <= s.stalls {
		<-ctx.Done()
		// A creation response can arrive concurrently with cancellation. Even
		// a returned watcher and nil error must not certify a canceled attempt.
		watch.updates <- auditWatchEntry{key: "abandoned", rev: 100}
		watch.updates <- nil
		return watch, nil
	}
	watch.updates <- auditWatchEntry{key: "complete", rev: 1}
	watch.updates <- nil
	return watch, nil
}

func TestSeededStateCreationProgressRetryAndLifetime(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		stalls := 1 + rand.New(rand.NewSource(seed)).Intn(2)
		ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
		deadline, _ := ctx.Deadline()
		state := &creationProgressState{t: t, stalls: stalls, deadline: deadline}
		creation := func() auditStateProgressTimer {
			clock := &scriptedStateProgress{ticks: make(chan time.Time, 1)}
			if len(state.clocks) < stalls {
				clock.ticks <- time.Unix(2, 0)
			}
			state.clocks = append(state.clocks, clock)
			return clock
		}
		progress := func() auditStateProgressTimer {
			if state.calls[len(state.calls)-1].Err() != nil {
				t.Fatal("successful watcher canceled when creation watchdog was disarmed")
			}
			return &scriptedStateProgress{ticks: make(chan time.Time)}
		}
		value, err := auditStateRead(ctx, func(call context.Context) (jetstream.KeyValue, error) {
			return initialAuditStateUsingTimers(call, state, func(string) bool { return true }, progress, creation)
		})
		stop()
		if err != nil || len(state.calls) != stalls+1 {
			t.Fatalf("seed=%d stalls=%d attempts=%d err=%v", seed, stalls, len(state.calls), err)
		}
		for i, watch := range state.watches {
			if !watch.stopped || !state.clocks[i].stopped || state.calls[i].Err() == nil {
				t.Fatal("watch creation lifecycle left an attempt running")
			}
		}
		if _, err := value.Get(context.Background(), "abandoned"); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal("late canceled creation leaked its snapshot into the fresh attempt")
		}
		if entry, err := value.Get(context.Background(), "complete"); err != nil || entry.Revision() != 1 {
			t.Fatal("fresh full snapshot did not win")
		}
	}
}

func TestStateCreationProgressStopsAfterThreeAttempts(t *testing.T) {
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	deadline, _ := ctx.Deadline()
	state := &creationProgressState{t: t, stalls: 3, deadline: deadline}
	value, err := auditStateRead(ctx, func(call context.Context) (jetstream.KeyValue, error) {
		return initialAuditStateUsingTimers(call, state, func(string) bool { return true }, nil, func() auditStateProgressTimer {
			clock := &scriptedStateProgress{ticks: make(chan time.Time, 1)}
			clock.ticks <- time.Unix(2, 0)
			state.clocks = append(state.clocks, clock)
			return clock
		})
	})
	if value != nil || !errors.Is(err, nats.ErrTimeout) || len(state.calls) != 3 || ctx.Err() != nil {
		t.Fatalf("creation retry extended parent budget or accepted a late snapshot: %v", err)
	}
	for i, watch := range state.watches {
		if !watch.stopped || !state.clocks[i].stopped || state.calls[i].Err() == nil {
			t.Fatal("exhausted creation retries left a watcher or watchdog running")
		}
	}
}

type creationErrorState struct {
	jetstream.KeyValue
	err error
}

func (s creationErrorState) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return nil, s.err
}

func TestStateCreationProgressPreservesParentAndIndependentError(t *testing.T) {
	denied := errors.New("independent creation rejection")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		ctx  context.Context
		want error
	}{{ctx, context.Canceled}, {t.Context(), denied}} {
		clock := &scriptedStateProgress{ticks: make(chan time.Time)}
		watch, stop, err := createAuditStateWatch(tc.ctx, creationErrorState{err: denied}, func() auditStateProgressTimer { return clock })
		stop()
		if watch != nil || !errors.Is(err, tc.want) || !clock.stopped || errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("creation changed independent/parent error: %v", err)
		}
	}
}
