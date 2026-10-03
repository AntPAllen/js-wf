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
	"js-wf/reconcile"
	"js-wf/wf"
	"js-wf/worker"
)

// Failed native hints transfer recovery to a durable Suspended journal entry.
// Actual production worker and scanner decisions must complete at the common
// deadline without relying on restored consumer timestamps or a NAK.
func runTimerHintRecovery(seed int64, replay *Trace, lost bool) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("worker_timer_hint_recovery"); err != nil {
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
	var operations []worker.OperationEvent
	newWorker := func(name string, fail bool) (*worker.Worker, error) {
		return worker.NewWithPorts(name, map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{
			Journal: store, Leases: leases, Outcome: outcomes, Invocation: transport.SignalTransport,
			Signals: transport.SignalTransport, Client: c, Timer: timers, NativeTimer: true,
			HeartbeatTicks: make(chan time.Time), OperationObserver: func(event worker.OperationEvent) { operations = append(operations, event) },
			TimerNow: func(context.Context) (time.Time, error) {
				if fail && !lost {
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
		if err := timers.QueueFault(DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	one, stopOne := context.WithCancel(ctx)
	defer stopOne()
	transport.Dispatch.StopAfterNextNak(stopOne)
	transport.Dispatch.StopAfterNextAck(stopOne)
	if err := first.RunPartitionWithTransport(one, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 3 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.Suspended || first.Metrics().HandoffEnqueues != 0 || transport.Dispatch.Pending() != 0 || len(transport.Dispatch.RetainedSequences()) != 0 {
		return trace, fmt.Errorf("missing durable timer suspension: journal=%+v pending=%d err=%v", records, transport.Dispatch.Pending(), err)
	}
	var ackSeen, failedHint bool
	for _, event := range observations {
		if event.Stage == "nak" || event.Stage == "execution_retry" {
			return trace, fmt.Errorf("native hint failure depended on retry/NAK: %+v", event)
		}
		if event.Stage == "ack" && event.Error == "" {
			ackSeen = true
		}
	}
	for _, event := range operations {
		if event.Operation == "timer_native_hint" && event.Error != "" && event.TimerPublished != nil && !*event.TimerPublished {
			failedHint = true
		}
	}
	if !ackSeen || !failedHint {
		return trace, fmt.Errorf("missing failed hint and confirmed ACK observations")
	}
	scan := reconcile.NewSuspendedScanWithPort(commonClockRepairPort{transport.SignalTransport, store})
	scan.Grace = 0
	scan.Now = func() time.Time { return time.UnixMilli(3600000) }
	scan.DomainNow = func(context.Context, string) (time.Time, error) { return time.UnixMilli(s.NowMillis()), nil }
	result, err := scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued != 0 {
		return trace, fmt.Errorf("timer repaired before due: %+v %v", result, err)
	}
	if err := s.AdvanceMillis(1999); err != nil {
		return trace, err
	}
	result, err = scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued != 0 {
		return trace, fmt.Errorf("timer repaired before deadline: %+v %v", result, err)
	}
	if err := s.AdvanceMillis(1); err != nil {
		return trace, err
	}
	result, err = scan.Scan(ctx, 1, 1, false)
	if err != nil || result.Reenqueued != 1 || transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("missing due repair: %+v %v", result, err)
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
	if err != nil || len(records) != 5 || records[3].Kind != journal.StepCompleted || records[4].Kind != journal.Completed || transport.Dispatch.Pending() != 0 || len(transport.Dispatch.RetainedSequences()) != 0 {
		return trace, fmt.Errorf("timer retry completion journal=%+v err=%v", records, err)
	}
	if s.NowMillis() != 2000 {
		return trace, fmt.Errorf("timer recovery missed common deadline: %d", s.NowMillis())
	}
	s.RecordTransport(TransportEvent{Operation: "check_timer_hint_recovery", Outcome: fmt.Sprintf("unapplied_publish=%v", lost), AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestWorkerTimerHintFailureRecovery(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("unapplied_publish_%v", lost), func(t *testing.T) {
			trace, err := runTimerHintRecovery(42, nil, lost)
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
			replayed, err := runTimerHintRecovery(42, &trace, lost)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("timer retry replay differs: %v", err)
			}
			t.Logf("TIMER_HINT_RECOVERY unapplied_publish=%v virtual_ms=%d exact_replay=true production_timer=true", lost, trace.Transport[len(trace.Transport)-1].AtMillis)
		})
	}
}
