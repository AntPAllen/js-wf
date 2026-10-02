package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/worker"
)

func runDispatchClockTransition(seed int64, replay *Trace) (trace Trace, runErr error) {
	return runDispatchPendingClock(seed, replay, false)
}

func runStoredPendingClock(seed int64, replay *Trace) (trace Trace, runErr error) {
	return runDispatchPendingClock(seed, replay, true)
}

func runDispatchPendingClock(seed int64, replay *Trace, stored bool) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	workload := "dispatch_clock_transition"
	if stored {
		workload = "consumer_pending_clock_restore"
	}
	if err := s.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	sign, err := s.Choose([]string{"ahead", "behind"})
	if err != nil {
		return trace, err
	}
	policy, err := s.Choose([]string{"ack_wait", "progress", "nak"})
	if err != nil {
		return trace, err
	}
	streamClock, streamOffset := "healthy", int64(0)
	if stored {
		streamClock, err = s.Choose([]string{"ahead", "behind", "healthy"})
		if err != nil {
			return trace, err
		}
		if streamClock == "ahead" {
			streamOffset = 60000
		} else if streamClock == "behind" {
			streamOffset = -60000
		}
	}
	m := NewDispatchTransport(s, worker.DefaultAckWait)
	if stored {
		if err := m.EnableStoredPendingClock(); err != nil {
			return trace, err
		}
	}
	offset := time.Minute
	if sign == "behind" {
		offset = -time.Minute
	}
	if err := m.SetConsumerClockOffset(offset); err != nil {
		return trace, err
	}
	if stored {
		m.PublishRunMessage("wf.run.0", []byte("test"), nil, time.UnixMilli(streamOffset))
	} else {
		m.PublishRun("wf.run.0", []byte("test"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var handleErr error
	calls := 0
	handler := func(_ context.Context, msg jetstream.Msg) {
		calls++
		metadata, err := msg.Metadata()
		if err != nil {
			handleErr = err
			cancel()
			return
		}
		if metadata.NumDelivered == 1 {
			switch policy {
			case "progress":
				handleErr = msg.InProgress()
			case "nak":
				delay := 100 * time.Millisecond
				if stored {
					delay = 5 * time.Second
				}
				handleErr = msg.NakWithDelay(delay)
			}
			if handleErr == nil {
				if stored {
					handleErr = m.TransferConsumerLeadership(0)
				} else {
					handleErr = m.SetConsumerClockOffset(0)
				}
			}
			if handleErr != nil {
				cancel()
			}
			return
		}
		if metadata.NumDelivered != 2 {
			handleErr = fmt.Errorf("unexpected delivery %d", metadata.NumDelivered)
		} else {
			handleErr = msg.Ack()
		}
		cancel()
	}
	if err := worker.RunPartitionWithPort(ctx, 0, m, handler, 1); err != nil || handleErr != nil {
		return trace, fmt.Errorf("loop=%v handler=%v", err, handleErr)
	}
	expected := int64(0)
	if sign == "ahead" {
		expected = 60000 + worker.DefaultAckWait.Milliseconds()
		if policy == "nak" {
			expected = 61000
		}
	}
	if stored {
		expected = max(int64(0), worker.DefaultAckWait.Milliseconds()+streamOffset)
		if policy != "ack_wait" {
			expected = 0
			if sign == "ahead" {
				expected = 60000 + worker.DefaultAckWait.Milliseconds()
				if policy == "nak" {
					expected = 65000
				}
			}
		}
	}
	if calls != 2 || m.Pending() != 0 || len(m.RetainedSequences()) != 0 || s.NowMillis() != expected {
		return trace, fmt.Errorf("calls=%d pending=%d retained=%d time=%d want=%d", calls, m.Pending(), len(m.RetainedSequences()), s.NowMillis(), expected)
	}
	outcome := sign + "_" + policy
	if stored {
		outcome += "_stream_" + streamClock
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_dispatch_clock", Outcome: outcome, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededStoredConsumerPendingClockReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runStoredPendingClock(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "consumer-pending-clock.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed%d: %v save:%v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runStoredPendingClock(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed%d replay:%v", seed, err)
			}
		}
		for _, event := range generated.Transport {
			if event.Operation != "characterize_dispatch_clock" {
				continue
			}
			if os.Getenv("SIM_WRITE_STORED_PENDING_PINS") == "1" && !covered[event.Outcome] {
				if err := generated.Save(filepath.Join("testdata", "regressions", "consumer-pending-clock-"+event.Outcome+".json")); err != nil {
					t.Fatal(err)
				}
			}
			covered[event.Outcome] = true
		}
	}
	if len(covered) != 18 {
		t.Fatalf("missing pending clock cells:%v", covered)
	}
}

func TestSeededDispatchClockTransitionReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runDispatchClockTransition(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "dispatch-clock.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed %d: %v; save: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runDispatchClockTransition(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_dispatch_clock" {
				if os.Getenv("SIM_WRITE_DISPATCH_CLOCK_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "dispatch-clock-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if len(covered) != 6 {
		t.Fatalf("missing clock policies: %v", covered)
	}
}
