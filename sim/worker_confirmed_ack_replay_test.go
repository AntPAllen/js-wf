package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

func runWorkerConfirmedAck(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_confirmed_ack_boundary"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"drop_before_commit", "lose_ack_after_commit", "lose_reply_held_retention"})
	if err != nil {
		return trace, err
	}
	delay, err := schedule.Choose([]string{"1s", "3s", "13s"})
	if err != nil {
		return trace, err
	}
	ackWait, _ := time.ParseDuration(delay)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transport := NewWorkerTransport(schedule, ackWait)
	journals := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journals, journals)
	leases := lease.NewWithKVPort(NewKVTransport(schedule, provision.LeaseTTL))
	outcomes := NewKVTransport(schedule, 0)
	id := integratedWorkerIDs(1)[0]
	if _, err := client.NewWithStartPort(transport.StartTransport).Start(ctx, "test", id, []byte(`null`)); err != nil {
		return trace, err
	}
	kind := mode
	if mode == "lose_reply_held_retention" {
		kind = "lose_ack_after_commit"
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "retention", Kind: "hold_after_ack"}); err != nil {
			return trace, err
		}
	}
	if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "ack", Kind: kind}); err != nil {
		return trace, err
	}
	effects := 0
	w, err := worker.NewWithPorts("ack-worker", map[string]worker.Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", nil, func(context.Context) (int, error) { effects++; return 77, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: leases, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)})
	if err != nil {
		return trace, err
	}
	if mode == "lose_reply_held_retention" {
		transport.Dispatch.StopAfterNextAck(cancel)
	} else {
		transport.Dispatch.StopWhenDrained(cancel)
	}
	if err := w.RunPartitionWithTransport(ctx, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	if ctx.Err() == context.DeadlineExceeded || effects != 1 || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("mode%s effects%d pending%d ctx%v", mode, effects, transport.Dispatch.Pending(), ctx.Err())
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("terminal history %+v err%v", report, err)
	}
	if mode == "lose_reply_held_retention" {
		if transport.Dispatch.CheckDrained() == nil {
			return trace, fmt.Errorf("consumer ACK mistaken for physical drain")
		}
		if err := transport.Dispatch.CommitRetention(1); err != nil {
			return trace, err
		}
	}
	if err := transport.Dispatch.CheckDrained(); err != nil {
		return trace, err
	}
	expected := int64(0)
	if mode == "drop_before_commit" {
		expected = ackWait.Milliseconds()
	}
	if schedule.NowMillis() != expected {
		return trace, fmt.Errorf("recovery time%d want%d", schedule.NowMillis(), expected)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_confirmed_worker_ack", Sequence: 1, Outcome: mode + "/" + delay, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerConfirmedAckReplay(t *testing.T) {
	if path := os.Getenv("SIM_CONFIRMED_ACK_OUT"); path != "" {
		trace, err := runWorkerConfirmedAck(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	coverage := map[string]bool{}
	for seed := range seededSchedules(t) {
		trace, err := runWorkerConfirmedAck(seed, nil)
		if err != nil {
			t.Fatalf("seed%d %v", seed, err)
		}
		coverage[trace.Decisions[0].Chosen+"/"+trace.Decisions[1].Chosen] = true
		if seed <= 10 {
			got, err := runWorkerConfirmedAck(0, &trace)
			if err != nil || !reflect.DeepEqual(trace, got) {
				t.Fatalf("seed%d replay %v", seed, err)
			}
		}
	}
	if len(coverage) != 9 {
		t.Fatal("missing fault/timing combinations", coverage)
	}
}
