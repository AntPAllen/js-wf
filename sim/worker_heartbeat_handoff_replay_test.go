package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func runSeededHeartbeatHandoff(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("worker_heartbeat_handoff"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"renew_drop", "renew_ack_lost", "progress_drop", "ticks_closed"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journalTransport := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journalTransport, journalTransport)
	leaseTransport := NewKVTransport(schedule, 30*time.Second)
	leasing := lease.NewWithKVPort(leaseTransport)
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	ticks := make(chan time.Time, 1)
	entered := make(chan context.Context, 1)
	var effects int
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "held-effect", 0, func(effectCtx context.Context) (int, error) {
			effects++
			if effects == 1 {
				entered <- effectCtx
				<-effectCtx.Done()
				return 0, effectCtx.Err()
			}
			return 42, nil
		})
		if err != nil {
			return nil, err
		}
		return json.RawMessage(strconv.Itoa(value)), nil
	}
	first, err := worker.NewWithPorts("heartbeat-first", map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{
		Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport,
		Signals: transport.SignalTransport, Client: c, HeartbeatTicks: ticks,
	})
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
	var effectCtx context.Context
	select {
	case effectCtx = <-entered:
	case <-ctx.Done():
		return trace, fmt.Errorf("seed %d first effect did not start: %w", seed, ctx.Err())
	}
	switch mode {
	case "renew_drop", "renew_ack_lost":
		kind := KVDropBeforeCommit
		if mode == "renew_ack_lost" {
			kind = KVLoseAckAfterCommit
		}
		if err := leaseTransport.QueueFault(KVFault{Operation: "update", Kind: kind}); err != nil {
			return trace, err
		}
		if err := schedule.AdvanceMillis(1000); err != nil {
			return trace, err
		}
		ticks <- time.UnixMilli(1000)
	case "progress_drop":
		if err := transport.Dispatch.QueueFault(DispatchFault{Operation: "progress", Kind: "drop_before_commit"}); err != nil {
			return trace, err
		}
		if err := schedule.AdvanceMillis(1000); err != nil {
			return trace, err
		}
		ticks <- time.UnixMilli(1000)
	case "ticks_closed":
		close(ticks)
	}
	select {
	case err := <-firstDone:
		if err != nil {
			return trace, fmt.Errorf("seed %d first worker: %w", seed, err)
		}
	case <-ctx.Done():
		return trace, fmt.Errorf("seed %d first worker did not stop: %w", seed, ctx.Err())
	}
	if effectCtx.Err() == nil || transport.Dispatch.Pending() != 1 || effects != 1 {
		return trace, fmt.Errorf("seed %d first handoff effect=%v pending=%d effects=%d", seed, effectCtx.Err(), transport.Dispatch.Pending(), effects)
	}
	partial, _, err := store.Read(ctx, typ, id)
	if err != nil || len(partial) != 2 || partial[0].Kind != journal.Started || partial[1].Kind != journal.StepRequested {
		return trace, fmt.Errorf("seed %d partial journal=%+v err=%v", seed, partial, err)
	}
	if _, err := leaseTransport.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		return trace, fmt.Errorf("seed %d old lease survived cleanup: %v", seed, err)
	}
	second, err := worker.NewWithPorts("heartbeat-second", map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{
		Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport,
		Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time),
	})
	if err != nil {
		return trace, err
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	transport.Dispatch.StopWhenDrained(stopSecond)
	if err := second.RunPartitionWithTransport(secondCtx, 0, transport.Dispatch); err != nil {
		return trace, fmt.Errorf("seed %d successor: %w", seed, err)
	}
	if transport.Dispatch.Pending() != 0 || effects != 2 {
		return trace, fmt.Errorf("seed %d successor pending=%d effects=%d", seed, transport.Dispatch.Pending(), effects)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 4 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed || records[2].Epoch <= records[1].Epoch {
		return trace, fmt.Errorf("seed %d successor journal=%+v err=%v", seed, records, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journalTransport, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_heartbeat_handoff", Subject: identity.JournalSubject(typ, id), Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededHeartbeatHandoffReplay(t *testing.T) {
	if os.Getenv("SIM_HEARTBEAT_HANDOFF_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededHeartbeatHandoff(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_HEARTBEAT_HANDOFF_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededHeartbeatHandoff(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-heartbeat-handoff-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededHeartbeatHandoff(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"renew_drop", "renew_ack_lost", "progress_drop", "ticks_closed"} {
		if observed[mode] == 0 {
			t.Fatalf("handoff mode %s was not scheduled", mode)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-heartbeat-handoff-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededHeartbeatHandoffReplay$")
		cmd.Env = append(os.Environ(), "SIM_HEARTBEAT_HANDOFF_HELPER=1", "SIM_HEARTBEAT_HANDOFF_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("worker heartbeat handoff trace changed across processes")
	}
}
