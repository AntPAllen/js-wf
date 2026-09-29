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

	"github.com/nats-io/nats.go"
)

func runSeededWorkerRunningCancel(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("worker_running_cancel"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "publish_drop", "publish_ack_lost", "enqueue_drop", "enqueue_ack_lost", "missed_notification_poll", "stale_poll_then_match"})
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
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	effectEntered := make(chan context.Context, 1)
	var effectCalls int
	w, err := worker.NewWithPorts("modeled-cancel-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.Run(c, "blocked-effect", 0, func(effectCtx context.Context) (int, error) {
			effectCalls++
			effectEntered <- effectCtx
			<-effectCtx.Done()
			return 0, effectCtx.Err()
		})
		return nil, err
	}}, worker.ModeledWorkerPorts{
		Journal: store, Leases: lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second)),
		Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport,
		Client: c, CancellationNotifications: true, CancellationPoll: transport.SignalTransport,
	})
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	transport.Dispatch.StopWhenDrained(stop)
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartitionWithTransport(ctx, 0, transport.Dispatch) }()
	var effectCtx context.Context
	select {
	case effectCtx = <-effectEntered:
	case <-ctx.Done():
		return trace, fmt.Errorf("seed %d effect did not enter: %w", seed, ctx.Err())
	}
	stale := &nats.Msg{Subject: "wf.sig." + typ + "." + id + "." + client.CancelSignalName, Header: nats.Header{}}
	stale.Header.Set("Wf-Inv-Seq", strconv.FormatUint(handle.InvSeq+1, 10))
	w.ObserveCancellationNotification(stale)
	if effectCtx.Err() != nil {
		return trace, fmt.Errorf("seed %d stale generation canceled effect", seed)
	}
	schedule.RecordTransport(TransportEvent{Operation: "cancel_notify", Subject: stale.Subject, Sequence: handle.InvSeq + 1, Outcome: "stale_generation", AtMillis: schedule.NowMillis()})
	if mode == "stale_poll_then_match" {
		transport.CommitSignal(stale)
		found, err := w.PollRunningCancellation(ctx, typ, id, handle.InvSeq)
		if err != nil || found || effectCtx.Err() != nil {
			return trace, fmt.Errorf("seed %d stale durable poll found=%v err=%v", seed, found, err)
		}
	}
	switch mode {
	case "publish_drop":
		err = transport.QueueSignalFault(SignalDropBeforeCommit)
	case "publish_ack_lost":
		err = transport.QueueSignalFault(SignalLoseAckAfterCommit)
	case "enqueue_drop", "enqueue_ack_lost":
		kind := "drop_before_commit"
		if mode == "enqueue_ack_lost" {
			kind = "lose_ack_after_commit"
		}
		err = transport.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind})
	}
	if err != nil {
		return trace, err
	}
	sequence, cancelErr := c.Cancel(ctx, typ, id)
	if mode == "publish_drop" {
		if !errors.Is(cancelErr, client.ErrSignalUnknown) || len(transport.SignalFor(typ, id, client.CancelSignalName)) != 0 {
			return trace, fmt.Errorf("seed %d dropped cancel: seq=%d err=%v", seed, sequence, cancelErr)
		}
		sequence, cancelErr = c.Cancel(ctx, typ, id)
	} else if mode == "publish_ack_lost" {
		if !errors.Is(cancelErr, client.ErrSignalUnknown) {
			return trace, fmt.Errorf("seed %d hidden cancel acknowledgment: %v", seed, cancelErr)
		}
	} else if mode == "enqueue_drop" || mode == "enqueue_ack_lost" {
		if !errors.Is(cancelErr, client.ErrEnqueueUnknown) {
			return trace, fmt.Errorf("seed %d hidden enqueue acknowledgment: %v", seed, cancelErr)
		}
	} else if cancelErr != nil {
		return trace, cancelErr
	}
	if sequence == 0 && mode == "publish_ack_lost" {
		stored := transport.SignalFor(typ, id, client.CancelSignalName)
		if len(stored) != 1 {
			return trace, fmt.Errorf("seed %d committed cancel count=%d", seed, len(stored))
		}
		sequence = stored[0].Sequence
	}
	if sequence == 0 || transport.SignalGeneration(sequence) != strconv.FormatUint(handle.InvSeq, 10) {
		return trace, fmt.Errorf("seed %d cancel generation seq=%d", seed, sequence)
	}
	notification := &nats.Msg{Subject: stale.Subject, Header: nats.Header{}}
	notification.Header.Set("Wf-Inv-Seq", strconv.FormatUint(handle.InvSeq, 10))
	if mode == "missed_notification_poll" || mode == "stale_poll_then_match" {
		if effectCtx.Err() != nil {
			return trace, fmt.Errorf("seed %d canceled before durable poll", seed)
		}
		if err := schedule.AdvanceMillis((15 * time.Second).Milliseconds()); err != nil {
			return trace, err
		}
		found, err := w.PollRunningCancellation(ctx, typ, id, handle.InvSeq)
		if err != nil || !found {
			return trace, fmt.Errorf("seed %d durable poll found=%v err=%v", seed, found, err)
		}
	} else {
		w.ObserveCancellationNotification(notification)
		schedule.RecordTransport(TransportEvent{Operation: "cancel_notify", Subject: notification.Subject, Sequence: handle.InvSeq, Outcome: "matching_generation", AtMillis: schedule.NowMillis()})
	}
	select {
	case <-effectCtx.Done():
	case <-time.After(2 * time.Second):
		return trace, fmt.Errorf("seed %d effect was not canceled", seed)
	}
	select {
	case err := <-workerDone:
		if err != nil {
			return trace, fmt.Errorf("seed %d worker: %w", seed, err)
		}
	case <-time.After(2 * time.Second):
		return trace, fmt.Errorf("seed %d worker did not drain", seed)
	}
	if transport.Dispatch.Pending() != 0 || effectCalls != 1 {
		return trace, fmt.Errorf("seed %d pending=%d effects=%d", seed, transport.Dispatch.Pending(), effectCalls)
	}
	records, _, err := store.Read(context.Background(), typ, id)
	if err != nil || len(records) != 4 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.SignalConsumed || records[3].Kind != journal.Failed {
		return trace, fmt.Errorf("seed %d canceled journal=%+v err=%v", seed, records, err)
	}
	var consumed struct {
		Name string `json:"name"`
	}
	var terminal wf.Outcome
	if json.Unmarshal(records[2].Payload, &consumed) != nil || consumed.Name != client.CancelSignalName || json.Unmarshal(records[3].Payload, &terminal) != nil || terminal.Error != client.ErrCancelled.Error() || terminal.InvSeq != handle.InvSeq {
		return trace, fmt.Errorf("seed %d cancel payload consumed=%+v terminal=%+v", seed, consumed, terminal)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journalTransport, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("seed %d retained check=%+v err=%v", seed, report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_running_cancel", Subject: identity.JournalSubject(typ, id), Sequence: sequence, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerRunningCancelReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_RUNNING_CANCEL_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerRunningCancel(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_RUNNING_CANCEL_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observedModes := map[string]int{}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededWorkerRunningCancel(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-running-cancel-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observedModes[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededWorkerRunningCancel(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d running cancel replay: %v", seed, err)
			}
		}
	}
	for _, mode := range []string{"clean", "publish_drop", "publish_ack_lost", "enqueue_drop", "enqueue_ack_lost", "missed_notification_poll", "stale_poll_then_match"} {
		if observedModes[mode] == 0 {
			t.Fatalf("cancel fault mode %s was not scheduled", mode)
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-running-cancel-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerRunningCancelReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_RUNNING_CANCEL_HELPER=1", "SIM_WORKER_RUNNING_CANCEL_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("running cancellation trace changed across processes")
	}
}
