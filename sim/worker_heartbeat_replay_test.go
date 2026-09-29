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

func runSeededWorkerHeartbeat(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_heartbeat_execution"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"progress_early", "progress_late", "progress_twice", "no_progress"})
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
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	ticks := make(chan time.Time, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	var effects int
	w, err := worker.NewWithPorts("modeled-heartbeat-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "held-effect", 0, func(effectCtx context.Context) (int, error) {
			effects++
			close(entered)
			select {
			case <-release:
				return 42, nil
			case <-effectCtx.Done():
				return 0, effectCtx.Err()
			}
		})
		if err != nil {
			return nil, err
		}
		return json.RawMessage(strconv.Itoa(value)), nil
	}}, worker.ModeledWorkerPorts{
		Journal: store, Leases: lease.NewWithKVPort(leaseTransport), Outcome: outcomes,
		Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c,
		HeartbeatTicks: ticks,
	})
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	transport.Dispatch.StopWhenDrained(stop)
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartitionWithTransport(ctx, 0, transport.Dispatch) }()
	select {
	case <-entered:
	case <-ctx.Done():
		return trace, fmt.Errorf("seed %d held effect did not start: %w", seed, ctx.Err())
	}
	heartbeat := func(at int64) error {
		if err := schedule.AdvanceMillis(at - schedule.NowMillis()); err != nil {
			return err
		}
		progressed := make(chan struct{})
		transport.Dispatch.OnNextProgress(func() { close(progressed) })
		ticks <- time.UnixMilli(at)
		select {
		case <-progressed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	switch mode {
	case "progress_early", "progress_twice":
		if err := heartbeat(1000); err != nil {
			return trace, err
		}
	case "progress_late":
		if err := heartbeat(2999); err != nil {
			return trace, err
		}
	}
	if mode == "progress_twice" {
		if err := heartbeat(3500); err != nil {
			return trace, err
		}
	}
	checkAt := int64(3100)
	if mode == "progress_twice" {
		checkAt = 5500
	}
	if err := schedule.AdvanceMillis(checkAt - schedule.NowMillis()); err != nil {
		return trace, err
	}
	consumer, err := transport.Dispatch.Consumer(ctx, 0)
	if err != nil {
		return trace, err
	}
	batch, fetchErr := consumer.FetchOne(ctx)
	redelivered := false
	if fetchErr == nil {
		for message := range batch.Messages() {
			metadata, err := message.Metadata()
			if err != nil || metadata.NumDelivered != 2 {
				return trace, fmt.Errorf("seed %d unexpected redelivery metadata=%+v err=%v", seed, metadata, err)
			}
			redelivered = true
		}
	} else if !errors.Is(fetchErr, jetstream.ErrNoMessages) {
		return trace, fmt.Errorf("seed %d fetch after heartbeat: %w", seed, fetchErr)
	}
	if redelivered != (mode == "no_progress") {
		return trace, fmt.Errorf("seed %d mode=%s redelivered=%v", seed, mode, redelivered)
	}
	close(release)
	select {
	case err := <-workerDone:
		if err != nil {
			return trace, fmt.Errorf("seed %d worker: %w", seed, err)
		}
	case <-time.After(2 * time.Second):
		return trace, fmt.Errorf("seed %d worker did not drain", seed)
	}
	if transport.Dispatch.Pending() != 0 || effects != 1 {
		return trace, fmt.Errorf("seed %d pending=%d effects=%d", seed, transport.Dispatch.Pending(), effects)
	}
	records, _, err := store.Read(context.Background(), typ, id)
	if err != nil || len(records) != 4 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed {
		return trace, fmt.Errorf("seed %d heartbeat journal=%+v err=%v", seed, records, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journalTransport, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_heartbeat", Subject: identity.JournalSubject(typ, id), Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerHeartbeatReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_HEARTBEAT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerHeartbeat(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_HEARTBEAT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerHeartbeat(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-heartbeat-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededWorkerHeartbeat(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"progress_early", "progress_late", "progress_twice", "no_progress"} {
		if observed[mode] == 0 {
			t.Fatalf("heartbeat mode %s was not scheduled", mode)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-heartbeat-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerHeartbeatReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_HEARTBEAT_HELPER=1", "SIM_WORKER_HEARTBEAT_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("worker heartbeat trace changed across processes")
	}
}
