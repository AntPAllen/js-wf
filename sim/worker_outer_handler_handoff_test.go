package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

func runOuterHandlerHandoff(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	defer func() { trace = schedule.Trace() }()
	if err := schedule.SetWorkload("outer_handler_handoff_control"); err != nil {
		return trace, err
	}
	mode, err := schedule.Choose([]string{"cancel", "heartbeat_closed", "renew_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journals := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journals, journals)
	leaseKV, outcomes := NewKVTransport(schedule, 30*time.Second), NewKVTransport(schedule, 0)
	leasing := lease.NewWithKVPort(leaseKV)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	typ, id := "test", integratedWorkerIDs(1)[0]
	entered, release := make(chan struct{}), make(chan struct{})
	lateResult := make(chan error, 1)
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	ticks := make(chan time.Time, 1)
	ports := worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: ticks}
	first, err := worker.NewWithPorts("outer-first", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		close(entered)
		<-release // Ignore cancellation outside any SDK effect.
		_, err := wf.Run(c, "late", 0, func(context.Context) (int, error) { return 99, nil })
		lateResult <- err
		return json.RawMessage(`99`), nil
	}}, ports)
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	transport.Dispatch.StopAfterNextNak(stopFirst)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartitionWithTransport(firstCtx, 0, transport.Dispatch) }()
	select {
	case <-entered:
	case <-ctx.Done():
		return trace, fmt.Errorf("handler did not enter: %w", ctx.Err())
	}
	switch mode {
	case "cancel":
		stopFirst()
	case "heartbeat_closed":
		close(ticks)
	case "renew_ack_lost":
		if err := leaseKV.QueueFault(KVFault{Operation: "update", Kind: KVLoseAckAfterCommit}); err != nil {
			return trace, err
		}
		if err := schedule.AdvanceMillis(3000); err != nil {
			return trace, err
		}
		ticks <- time.UnixMilli(3000)
	}
	select {
	case err := <-firstDone:
		if err != nil {
			return trace, err
		}
	case <-ctx.Done():
		return trace, fmt.Errorf("outer handler trapped delivery: %w", ctx.Err())
	}
	partial, _, err := store.Read(ctx, typ, id)
	if err != nil || len(partial) != 1 || partial[0].Kind != journal.Started {
		return trace, fmt.Errorf("partial journal=%+v err=%v", partial, err)
	}
	ports.HeartbeatTicks = make(chan time.Time)
	second, err := worker.NewWithPorts("outer-second", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "successor", 0, func(context.Context) (int, error) { return 42, nil })
		return json.RawMessage(fmt.Sprint(value)), err
	}}, ports)
	if err != nil {
		return trace, err
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	transport.Dispatch.StopWhenDrained(stopSecond)
	if err := second.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	accepted, tail, err := store.Read(ctx, typ, id)
	if err != nil || len(accepted) != 4 || accepted[1].Kind != journal.StepRequested || accepted[2].Kind != journal.StepCompleted || accepted[3].Kind != journal.Completed || accepted[1].Epoch <= partial[0].Epoch {
		return trace, fmt.Errorf("successor journal=%+v err=%v", accepted, err)
	}
	var outcome wf.Outcome
	if json.Unmarshal(accepted[3].Payload, &outcome) != nil || string(outcome.Result) != "42" {
		return trace, fmt.Errorf("wrong outcome: %+v", outcome)
	}
	close(release)
	released = true
	select {
	case err := <-lateResult:
		if !errors.Is(err, context.Canceled) {
			return trace, fmt.Errorf("late SDK append not rejected: %v", err)
		}
	case <-ctx.Done():
		return trace, fmt.Errorf("late handler did not return: %w", ctx.Err())
	}
	after, afterTail, err := store.Read(ctx, typ, id)
	if err != nil || tail != afterTail || !reflect.DeepEqual(accepted, after) || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("late handler changed accepted result: %v", err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("integrity=%+v err=%v", report, err)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestOuterHandlerCancellationHandoffAndLateAppend(t *testing.T) {
	for seed := int64(1); seed <= 1000; seed++ {
		trace, err := runOuterHandlerHandoff(seed, nil)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if seed <= 10 {
			replay, err := runOuterHandlerHandoff(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, replay) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
}
