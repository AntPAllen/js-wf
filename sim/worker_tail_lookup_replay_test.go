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

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

type unavailableWorkerTail struct {
	*JournalTransport
	target    int
	remaining int
	failed    int
	prefix    []Message
}

func (p *unavailableWorkerTail) Last(ctx context.Context, subject string) (journal.AppendTail, error) {
	list := p.Messages(subject)
	if len(list) == p.target && p.remaining > 0 {
		p.remaining--
		p.failed++
		p.prefix = list
		p.schedule.RecordTransport(TransportEvent{Operation: "last", Subject: subject, Sequence: uint64(len(list)), Outcome: "api_503_10008", AtMillis: p.schedule.NowMillis()})
		return journal.AppendTail{}, &jetstream.APIError{Code: 503, ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}
	}
	return p.JournalTransport.Last(ctx, subject)
}

// Reproduce the observed API edge, not the server process that produced it.
// Actual workers must abandon the delivery, release, then recover on replacement.
func runWorkerTailLookupRecovery(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_tail_lookup_unavailable"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	stage, err := schedule.Choose([]string{"started", "requested", "completed", "terminal"})
	if err != nil {
		return trace, err
	}
	count, err := schedule.Choose([]string{"1", "2", "3"})
	if err != nil {
		return trace, err
	}
	failures, _ := strconv.Atoi(count)
	targets := map[string]int{"started": 0, "requested": 1, "completed": 2, "terminal": 3}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const typ, id = "test", "tail-recovery"
	transport := NewWorkerTransport(schedule, worker.DefaultAckWait)
	live := NewJournalTransport(schedule)
	port := &unavailableWorkerTail{JournalTransport: live, target: targets[stage], remaining: failures}
	store := journal.NewWithPorts(port, live)
	kv := NewKVTransport(schedule, provision.LeaseTTL)
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	effects := 0
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", 1, func(context.Context) (int, error) { effects++; return 42, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}}
	makeWorker := func(index int) (*worker.Worker, error) {
		return worker.NewWithPorts(fmt.Sprintf("tail-worker-%d", index), handlers, worker.ModeledWorkerPorts{Journal: store, Leases: lease.NewWithKVPort(kv), Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)})
	}
	subject := identity.JournalSubject(typ, id)
	partition := identity.Partition(typ, id, provision.Partitions)
	for i := 0; i < failures; i++ {
		w, err := makeWorker(i)
		if err != nil {
			return trace, err
		}
		attempt, stop := context.WithCancel(ctx)
		transport.Dispatch.StopAfterNextNak(stop)
		transport.Dispatch.StopWhenDrained(stop)
		err = w.RunPartitionWithTransport(attempt, partition, transport.Dispatch)
		stop()
		if err != nil {
			return trace, err
		}
		if port.failed != i+1 || transport.Dispatch.Pending() != 1 || !reflect.DeepEqual(live.Messages(subject), port.prefix) || len(port.prefix) != port.target {
			return trace, fmt.Errorf("failed delivery %d failed=%d pending=%d prefix=%v retained=%v", i, port.failed, transport.Dispatch.Pending(), port.prefix, live.Messages(subject))
		}
		if _, err := kv.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("failed delivery retained lease: %v", err)
		}
		if _, err := outcomes.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("failed delivery published outcome: %v", err)
		}
	}
	w, err := makeWorker(failures)
	if err != nil {
		return trace, err
	}
	transport.Dispatch.StopWhenDrained(cancel)
	if err := w.RunPartitionWithTransport(ctx, partition, transport.Dispatch); err != nil {
		return trace, err
	}
	wantEffects := 1
	if stage == "completed" {
		wantEffects += failures
	}
	if effects != wantEffects || port.remaining != 0 || transport.Dispatch.Pending() != 0 || ctx.Err() == context.DeadlineExceeded {
		return trace, fmt.Errorf("effects=%d want=%d remaining=%d pending=%d context=%v", effects, wantEffects, port.remaining, transport.Dispatch.Pending(), ctx.Err())
	}
	records, _, err := store.Read(context.Background(), typ, id)
	if err != nil || len(records) != 4 || records[3].Kind != journal.Completed {
		return trace, fmt.Errorf("terminal journal len=%d err=%v", len(records), err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, live, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		return trace, fmt.Errorf("retained integrity=%+v err=%v", report, err)
	}
	result, err := outcomes.Get(context.Background(), identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	var outcome struct {
		Result []byte `json:"result"`
	}
	if err := json.Unmarshal(result.Value, &outcome); err != nil || string(outcome.Result) != "42" {
		return trace, fmt.Errorf("result=%s err=%v", result.Value, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_tail_recovery", Sequence: uint64(failures), Outcome: stage, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerTailLookupRecovery(t *testing.T) {
	if path := os.Getenv("SIM_WORKER_TAIL_OUT"); path != "" {
		seed := int64(42)
		if raw := os.Getenv("FAULT_SEED"); raw != "" {
			var err error
			seed, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		trace, err := runWorkerTailLookupRecovery(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]int{}
	for seed := range seededSchedules(t) {
		trace, err := runWorkerTailLookupRecovery(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-tail-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[trace.Decisions[0].Chosen+"/"+trace.Decisions[1].Chosen]++
		if seed <= 10 {
			again, err := replayTrace(trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 12 {
		t.Fatalf("mode coverage=%v", modes)
	}
	t.Logf("API 503/10008 recovery stages/counts=%v", modes)
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "worker-tail.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerTailLookupRecovery$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_TAIL_OUT="+path, "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, raw) {
			t.Fatal("tail recovery trace differs across processes")
		}
		previous = raw
	}
}
