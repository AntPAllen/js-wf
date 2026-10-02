package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// A prior unshifted run reaches a worker whose TimerNow and scheduler now use
// another clock. Subsequent native wakeups carry that scheduling leader's time.
func runWorkerFreshTimerWakeup(seed int64, replay *Trace) (trace Trace, runErr error) {
	return runWorkerFreshTimerWakeupClock(seed, replay, false)
}

func runWorkerFreshTimerWakeupClock(seed int64, replay *Trace, common bool) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	workload := "worker_fresh_timer_wakeup"
	if common {
		workload = "worker_common_clock_timers"
	}
	if err := s.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	sign, err := s.Choose([]string{"ahead", "behind"})
	if err != nil {
		return trace, err
	}
	mode, err := s.Choose([]string{"sleep", "await", "select_signal", "select_many"})
	if err != nil {
		return trace, err
	}
	durationChoice, err := s.Choose([]string{"250ms", "1s"})
	if err != nil {
		return trace, err
	}
	duration, err := time.ParseDuration(durationChoice)
	if err != nil {
		return trace, err
	}
	base := time.Unix(1700000000, 0).UTC()
	transport := NewWorkerTransport(s, worker.DefaultAckWait)
	transport.StartTransport.OnRunCommit(func(run Message) { transport.Dispatch.PublishRunMessage(run.Subject, run.Data, nil, base) })
	s.RecordTransport(TransportEvent{Operation: "fresh_timer_initial_clock", Outcome: "unshifted", AtMillis: s.NowMillis()})
	timers := NewTimerScheduleTransport(s, base)
	offset := time.Minute
	if sign == "behind" {
		offset = -time.Minute
	}
	if err := timers.SetLeaderClockOffset(offset); err != nil {
		return trace, err
	}
	timers.OnNativeDelivery(func(msg *nats.Msg, _ time.Time) {
		transport.Dispatch.PublishRunMessage(msg.Subject, msg.Data, msg.Header, timers.LeaderNow())
	})
	journals := NewJournalTransport(s)
	store := journal.NewWithPorts(journals, journals)
	outcomes := NewKVTransport(s, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	const typ, count = "test", 8
	id := integratedWorkerIDs(1)[0]
	calls := 0
	var options []worker.Option
	if common {
		options = append(options, worker.WithTimerClock("utc-quorum-v1", func(context.Context) (time.Time, time.Time, error) {
			now := base.Add(time.Duration(s.NowMillis()) * time.Millisecond)
			return now, now, nil
		}))
	}
	w, err := worker.NewWithPorts("fresh-clock-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls++
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("wait-%d", i)
			var err error
			if mode == "sleep" {
				err = wf.Sleep(c, name, duration)
			} else {
				var timer *wf.TimerHandle
				timer, err = c.Timer(name, duration)
				if err == nil {
					switch mode {
					case "await":
						err = timer.Await()
					case "select_signal":
						_, _, err = timer.SelectSignal("go")
					case "select_many":
						_, _, err = wf.Select(c, timer)
					}
				}
			}
			if err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`42`), nil
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: lease.NewWithKVPort(NewKVTransport(s, provision.LeaseTTL)), Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Timer: timers, TimerNow: func(context.Context) (time.Time, error) { return timers.LeaderNow(), nil }, NativeTimer: true, Client: c}, options...)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	deliver := func() error {
		one, cancel := context.WithCancel(ctx)
		defer cancel()
		transport.Dispatch.StopWhenDrained(cancel)
		return w.RunPartitionWithTransport(one, 0, transport.Dispatch)
	}
	if err := deliver(); err != nil {
		return trace, err
	}
	initial, _, err := store.Read(ctx, typ, id)
	if err != nil || len(initial) == 0 || initial[len(initial)-1].Kind != journal.Suspended || len(timers.NativeSources()) != 1 || s.NowMillis() != 0 {
		return trace, fmt.Errorf("seed %d fresh timer did not suspend: entries=%d sources=%d virtual_ms=%d err=%v", seed, len(initial), len(timers.NativeSources()), s.NowMillis(), err)
	}
	for i := 0; i < count; i++ {
		if err := timers.Advance(duration - time.Millisecond); err != nil {
			return trace, err
		}
		if transport.Dispatch.Pending() != 0 {
			return trace, fmt.Errorf("seed %d timer %d fired before its duration", seed, i)
		}
		if err := timers.Advance(time.Millisecond); err != nil {
			return trace, err
		}
		if transport.Dispatch.Pending() != 1 {
			return trace, fmt.Errorf("seed %d timer %d missing due delivery", seed, i)
		}
		if err := deliver(); err != nil {
			return trace, err
		}
	}
	records, _, err := store.Read(ctx, typ, id)
	entries := 42
	if mode == "sleep" {
		entries = 26
	}
	if err != nil || len(records) != entries || records[len(records)-1].Kind != journal.Completed || calls != 9 || len(timers.NativeSources()) != count || s.NowMillis() != int64(count)*duration.Milliseconds() || transport.Dispatch.Pending() != 0 || len(transport.Dispatch.RetainedSequences()) != 0 {
		return trace, fmt.Errorf("seed %d terminal entries=%d calls=%d time=%d err=%v", seed, len(records), calls, s.NowMillis(), err)
	}
	if common {
		for _, record := range records {
			if record.Kind != journal.StepRequested {
				continue
			}
			var req struct {
				Kind   string `json:"kind"`
				Domain string `json:"clock_domain"`
			}
			if err := json.Unmarshal(record.Payload, &req); err != nil {
				return trace, err
			}
			if (req.Kind == "timer" || req.Kind == "timer_start" || req.Kind == "timer_await" || req.Kind == "timer_signal_select") && req.Domain != "utc-quorum-v1" {
				return trace, fmt.Errorf("lost durable clock domain")
			}
		}
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: entries, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d retained=%+v err=%v", seed, report, err)
	}
	s.RecordTransport(TransportEvent{Operation: "check_worker_fresh_timer", Outcome: sign + "_" + mode + "_" + durationChoice, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededWorkerFreshTimerWakeupReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runWorkerFreshTimerWakeup(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "fresh-timer-wakeup.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed %d: %v; save: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runWorkerFreshTimerWakeup(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
		for _, e := range generated.Transport {
			if e.Operation == "check_worker_fresh_timer" {
				if os.Getenv("SIM_WRITE_FRESH_TIMER_PINS") == "1" && !covered[e.Outcome] && strings.HasPrefix(e.Outcome, "behind_") && strings.HasSuffix(e.Outcome, "250ms") {
					if err := generated.Save(filepath.Join("testdata", "regressions", "worker-fresh-timer-"+e.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[e.Outcome] = true
			}
		}
	}
	if len(covered) != 16 {
		t.Fatalf("missing source/API/duration cases: %v", covered)
	}
}

// Exercise the opt-in domain without reinterpreting legacy traces.
func TestSeededWorkerCommonClockAcrossSkewedDeliveryTimestamps(t *testing.T) {
	covered := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runWorkerFreshTimerWakeupClock(seed, nil, true)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "common-clock-timers.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed %d: %v; save: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "check_worker_fresh_timer" {
				if os.Getenv("SIM_WRITE_COMMON_CLOCK_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "worker-common-clock-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
		if seed <= 10 {
			replayed, err := runWorkerFreshTimerWakeupClock(seed, &generated, true)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	if len(covered) != 16 {
		t.Fatalf("API/skew/duration coverage=%d want16", len(covered))
	}
}
