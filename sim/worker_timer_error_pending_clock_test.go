package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// Characterize the production non-cancelled timer-error retry path. The
// pending-clock restoration is calibrated independently against pinned NATS;
// the unapplied, locally successful NAK is an explicit transport hypothesis.
func runTimerErrorPendingClock(seed int64, replay *Trace, lost bool) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("worker_timer_error_pending_clock"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(s, worker.DefaultAckWait)
	if err := transport.Dispatch.EnableStoredPendingClock(); err != nil {
		return trace, err
	}
	if err := transport.Dispatch.SetConsumerClockOffset(time.Minute); err != nil {
		return trace, err
	}
	transport.StartTransport.OnRunCommit(func(run Message) {
		transport.Dispatch.PublishRunMessage(run.Subject, run.Data, nil, time.UnixMilli(60000))
	})
	journals := NewJournalTransport(s)
	store := journal.NewWithPorts(journals, journals)
	leases := lease.NewWithKVPort(NewKVTransport(s, provision.LeaseTTL))
	outcomes := NewKVTransport(s, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	timers := NewTimerScheduleTransport(s, time.UnixMilli(0))
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "wait", 2*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`42`), nil
	}
	var observations []worker.DispatchEvent
	newWorker := func(name string, fail bool) (*worker.Worker, error) {
		return worker.NewWithPorts(name, map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{
			Journal: store, Leases: leases, Outcome: outcomes, Invocation: transport.SignalTransport,
			Signals: transport.SignalTransport, Client: c, Timer: timers, NativeTimer: true,
			HeartbeatTicks: make(chan time.Time),
			TimerNow: func(context.Context) (time.Time, error) {
				if fail {
					return time.Time{}, context.DeadlineExceeded
				}
				return time.UnixMilli(s.NowMillis()), nil
			},
		}, worker.WithDispatchObserver(func(event worker.DispatchEvent) { observations = append(observations, event) }), worker.WithTimerClock("utc-quorum-v1", func(context.Context) (time.Time, time.Time, error) {
			now := time.UnixMilli(s.NowMillis())
			return now, now, nil
		}))
	}
	first, err := newWorker("timer-error-first", true)
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	if lost {
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "nak", Kind: "drop_before_commit_success"}); err != nil {
			return trace, err
		}
	}
	one, stopOne := context.WithCancel(ctx)
	defer stopOne()
	transport.Dispatch.StopAfterNextNak(stopOne)
	if err := first.RunPartitionWithTransport(one, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	var retrySeen, nakSeen bool
	for _, event := range observations {
		if event.Stage == "execution_retry" && event.Error == "timer schedule was not confirmed: context deadline exceeded" {
			retrySeen = true
		}
		if event.Stage == "nak" {
			nakSeen = true
			if event.Error != "" {
				return trace, fmt.Errorf("local NAK acceptance was not successful: %s", event.Error)
			}
		}
	}
	if !retrySeen || !nakSeen {
		return trace, fmt.Errorf("missing timer-error retry/NAK observations")
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 2 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || first.Metrics().HandoffEnqueues != 0 {
		return trace, fmt.Errorf("non-cancelled timer retry journal=%+v handoffs=%d err=%v", records, first.Metrics().HandoffEnqueues, err)
	}
	if err := transport.Dispatch.TransferConsumerLeadership(0); err != nil {
		return trace, err
	}
	second, err := newWorker("timer-error-second", false)
	if err != nil {
		return trace, err
	}
	two, stopTwo := context.WithCancel(ctx)
	defer stopTwo()
	transport.Dispatch.StopWhenDrained(stopTwo)
	if err := second.RunPartitionWithTransport(two, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	records, _, err = store.Read(ctx, typ, id)
	if err != nil || len(records) != 4 || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("timer retry completion journal=%+v err=%v", records, err)
	}
	want := int64(61000)
	if lost {
		want = 60000 + worker.DefaultAckWait.Milliseconds()
	}
	if s.NowMillis() < want || s.NowMillis() > want+2000 {
		return trace, fmt.Errorf("timer retry restored clock delay=%d want=%d..%d", s.NowMillis(), want, want+2000)
	}
	s.RecordTransport(TransportEvent{Operation: "check_timer_error_pending_clock", Outcome: fmt.Sprintf("locally_successful_unapplied_nak=%v", lost), AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestWorkerTimerErrorPendingClockCharacterization(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("unapplied_nak_%v", lost), func(t *testing.T) {
			trace, err := runTimerErrorPendingClock(42, nil, lost)
			if err != nil {
				t.Fatal(err)
			}
			if root := os.Getenv("TIMER_ERROR_TRACE_ROOT"); root != "" {
				if err := os.MkdirAll(root, 0755); err != nil {
					t.Fatal(err)
				}
				if err := trace.Save(filepath.Join(root, fmt.Sprintf("timer-error-unapplied-%v.json", lost))); err != nil {
					t.Fatal(err)
				}
			}
			replayed, err := runTimerErrorPendingClock(42, &trace, lost)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("timer retry replay differs: %v", err)
			}
			t.Logf("TIMER_ERROR_PENDING_CLOCK unapplied_nak=%v virtual_ms=%d exact_replay=true production_timer=true", lost, trace.Transport[len(trace.Transport)-1].AtMillis)
		})
	}
}
