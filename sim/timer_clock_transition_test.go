package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This transport-level workload characterizes absolute-deadline sensitivity.
// It does not claim the production worker or NATS tolerates a clock transition.
func runTimerClockTransition(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("timer_clock_transition"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	sign, err := s.Choose([]string{"ahead", "behind"})
	if err != nil {
		return trace, err
	}
	cut, err := s.Choose([]string{"before_due", "after_due_without_quorum"})
	if err != nil {
		return trace, err
	}
	offset := time.Minute
	if sign == "behind" {
		offset = -time.Minute
	}
	base := time.Unix(1700000000, 0).UTC()
	m := NewTimerScheduleTransport(s, base)
	if err := m.SetLeaderClockOffset(offset); err != nil {
		return trace, err
	}
	var receipts []time.Time
	m.OnNativeDelivery(func(_ *nats.Msg, at time.Time) { receipts = append(receipts, at) })
	due := m.LeaderNow().Add(2 * time.Second)
	msg := &nats.Msg{Subject: "wf.schedule.test.id.timer", Header: nats.Header{}}
	msg.Header.Set(jetstream.ScheduleHeader, "@at "+due.Format(time.RFC3339Nano))
	msg.Header.Set(jetstream.ScheduleTargetHeader, "wf.run.0")
	if _, err := m.PublishNative(context.Background(), msg, "timer-1"); err != nil {
		return trace, err
	}
	m.SetScheduleQuorum(false)
	advance := time.Second
	if cut == "after_due_without_quorum" {
		advance = 3 * time.Second
	}
	if err := m.Advance(advance); err != nil {
		return trace, err
	}
	if len(receipts) != 0 {
		return trace, fmt.Errorf("delivered without quorum")
	}
	if err := m.SetLeaderClockOffset(0); err != nil {
		return trace, err
	}
	if len(receipts) != 0 {
		return trace, fmt.Errorf("clock transition bypassed quorum")
	}
	m.SetScheduleQuorum(true)
	expected := base.Add(advance)
	if sign == "ahead" {
		if len(receipts) != 0 {
			return trace, fmt.Errorf("ahead deadline delivered at heal")
		}
		remaining := due.Sub(base.Add(advance))
		if err := m.Advance(remaining - time.Millisecond); err != nil {
			return trace, err
		}
		if len(receipts) != 0 {
			return trace, fmt.Errorf("ahead deadline delivered early")
		}
		if err := m.Advance(time.Millisecond); err != nil {
			return trace, err
		}
		expected = due
	}
	if len(receipts) != 1 || !receipts[0].Equal(expected) {
		return trace, fmt.Errorf("receipts=%v expected=%s", receipts, expected)
	}
	if err := m.Advance(time.Minute); err != nil {
		return trace, err
	}
	if len(receipts) != 1 {
		return trace, fmt.Errorf("duplicate delivery after transition")
	}
	s.RecordTransport(TransportEvent{Operation: "check_timer_clock_transition", Outcome: sign + ":" + cut, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededTimerClockTransitionReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runTimerClockTransition(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "timer-clock-transition.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed %d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		replayed, err := runTimerClockTransition(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed %d replay err=%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "check_timer_clock_transition" {
				if os.Getenv("SIM_WRITE_TIMER_CLOCK_PINS") == "1" && !covered[event.Outcome] {
					path := filepath.Join("testdata", "regressions", "timer-clock-"+event.Outcome+".json")
					if err := generated.Save(path); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if seededScheduleLimit(t) >= 100 && len(covered) != 4 {
		t.Fatalf("missing transition cases: %v", covered)
	}
}
