//go:build linux

package integrity

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Observe watch lifecycle without relaying updates or changing snapshot logic.
// Stop completion alone does not prove that the initial-set barrier was reached.
type capacityWatchTiming struct {
	StartedNS      int64
	CreatedNS      int64
	CreationError  string
	StopStartedNS  int64
	StopFinishedNS int64
	StopError      string
}

type capacityStateTrace struct {
	mu      sync.Mutex
	origin  *time.Time
	watches []capacityWatchTiming
}

func (s *capacityStateTrace) snapshot() []capacityWatchTiming {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]capacityWatchTiming(nil), s.watches...)
}

type capacityStateTraceJS struct {
	jetstream.JetStream
	trace *capacityStateTrace
}

func (s capacityStateTraceJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := s.JetStream.KeyValue(ctx, bucket)
	if err != nil || bucket != "WF_STATE" {
		return kv, err
	}
	return capacityStateTraceKV{KeyValue: kv, trace: s.trace}, nil
}

type capacityStateTraceKV struct {
	jetstream.KeyValue
	trace *capacityStateTrace
}

func (s capacityStateTraceKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	s.trace.mu.Lock()
	index := len(s.trace.watches)
	s.trace.watches = append(s.trace.watches, capacityWatchTiming{StartedNS: int64(time.Since(*s.trace.origin))})
	s.trace.mu.Unlock()
	watch, err := s.KeyValue.WatchAll(ctx, opts...)
	s.trace.mu.Lock()
	s.trace.watches[index].CreatedNS = int64(time.Since(*s.trace.origin))
	s.trace.watches[index].CreationError = fmt.Sprint(err)
	s.trace.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return capacityStateTraceWatch{KeyWatcher: watch, trace: s.trace, index: index}, nil
}

type capacityStateTraceWatch struct {
	jetstream.KeyWatcher
	trace *capacityStateTrace
	index int
}

func (s capacityStateTraceWatch) Stop() error {
	s.trace.mu.Lock()
	s.trace.watches[s.index].StopStartedNS = int64(time.Since(*s.trace.origin))
	s.trace.mu.Unlock()
	err := s.KeyWatcher.Stop()
	s.trace.mu.Lock()
	s.trace.watches[s.index].StopFinishedNS = int64(time.Since(*s.trace.origin))
	s.trace.watches[s.index].StopError = fmt.Sprint(err)
	s.trace.mu.Unlock()
	return err
}

func TestCapacityStateTracePreservesInitialSetContract(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			origin := time.Now()
			trace := &capacityStateTrace{origin: &origin}
			watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 2)}
			watch.updates <- auditWatchEntry{key: "term.x", rev: 1}
			if complete {
				watch.updates <- nil
			}
			close(watch.updates)
			kv := capacityStateTraceKV{KeyValue: auditWatchState{watch: watch}, trace: trace}
			result, err := initialAuditState(context.Background(), kv, func(string) bool { return true })
			if (err == nil) != complete {
				t.Fatalf("completion=%v err=%v", complete, err)
			}
			if complete {
				if _, err := result.Get(context.Background(), "term.x"); err != nil {
					t.Fatal(err)
				}
			}
			frames := trace.snapshot()
			if !watch.stopped || len(frames) != 1 {
				t.Fatalf("stopped=%v frames=%+v", watch.stopped, frames)
			}
			frame := frames[0]
			if frame.StartedNS < 0 || frame.CreatedNS < frame.StartedNS || frame.StopStartedNS < frame.CreatedNS || frame.StopFinishedNS < frame.StopStartedNS {
				t.Fatalf("invalid lifecycle %+v", frame)
			}
		})
	}
}
