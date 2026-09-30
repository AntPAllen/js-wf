package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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

type latencyLeasePort struct {
	lease.KVPort
	schedule *Scheduler
	delay    int64
	updates  int
}

func (p *latencyLeasePort) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	if err := p.schedule.AdvanceMillis(p.delay); err != nil {
		return 0, err
	}
	p.updates++
	return p.KVPort.Update(ctx, key, data, revision)
}

type latencyJournalPort struct {
	journal.AppendPort
	journal.ReadPort
	schedule  *Scheduler
	delay     int64
	publishes int
	reads     int
}

func (p *latencyJournalPort) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	if err := p.schedule.AdvanceMillis(p.delay); err != nil {
		return 0, err
	}
	p.publishes++
	return p.AppendPort.Publish(ctx, subject, data, expected)
}
func (p *latencyJournalPort) Next(ctx context.Context, subject string, from uint64) (journal.AppendTail, error) {
	if err := p.schedule.AdvanceMillis(p.delay); err != nil {
		return journal.AppendTail{}, err
	}
	p.reads++
	return p.ReadPort.Next(ctx, subject, from)
}
func (p *latencyJournalPort) Wait(ctx context.Context, d time.Duration) error {
	return p.ReadPort.Wait(ctx, d)
}

type latencySignalPort struct {
	worker.SignalDrainPort
	schedule *Scheduler
	delay    int64
	reads    int
}

func (p *latencySignalPort) LastSignalSequence(ctx context.Context) (uint64, error) {
	if err := p.schedule.AdvanceMillis(p.delay); err != nil {
		return 0, err
	}
	p.reads++
	return p.SignalDrainPort.LastSignalSequence(ctx)
}
func (p *latencySignalPort) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	if err := p.schedule.AdvanceMillis(p.delay); err != nil {
		return nil, err
	}
	p.reads++
	return p.SignalDrainPort.NextSignal(ctx, from, subject)
}

// A cost slice, not a full replay of a broker failure: one production worker
// drains 16 buffered signals with deterministic request latency. Heartbeat
// ticks stay quiet to isolate the renew-before-each-entry protocol; those
// per-entry renewals keep the lease alive throughout virtual execution.
func runWorkerSignalWriteLatency(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_signal_write_latency"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	selected, err := schedule.Choose([]string{"0", "20", "170", "300"})
	if err != nil {
		return trace, err
	}
	delay, err := strconv.ParseInt(selected, 10, 64)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const typ, id = "test", "latency"
	transport := NewWorkerTransport(schedule, worker.DefaultAckWait)
	journals := NewJournalTransport(schedule)
	journalPort := &latencyJournalPort{AppendPort: journals, ReadPort: journals, schedule: schedule, delay: delay}
	store := journal.NewWithPorts(journalPort, journalPort)
	kv := NewKVTransport(schedule, provision.LeaseTTL)
	leasePort := &latencyLeasePort{KVPort: kv, schedule: schedule, delay: delay}
	signalPort := &latencySignalPort{SignalDrainPort: transport.SignalTransport, schedule: schedule, delay: delay}
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	w, err := worker.NewWithPorts("signal-cost-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		sum := 0
		for range 16 {
			data, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			var value int
			if err := json.Unmarshal(data, &value); err != nil {
				return nil, err
			}
			sum += value
		}
		return json.Marshal(sum)
	}}, worker.ModeledWorkerPorts{Journal: store, Leases: lease.NewWithKVPort(leasePort), Outcome: outcomes, Invocation: transport.SignalTransport, Signals: signalPort, Client: c, HeartbeatTicks: make(chan time.Time)})
	if err != nil {
		return trace, err
	}
	handle, err := c.Start(ctx, typ, id, []byte("null"))
	if err != nil {
		return trace, err
	}
	for i := range 16 {
		headers := nats.Header{}
		headers.Set("Wf-Inv-Seq", strconv.FormatUint(handle.InvSeq, 10))
		transport.CommitSignal(&nats.Msg{Subject: "wf.sig.test.latency.go", Header: headers, Data: []byte(strconv.Itoa(i))})
	}
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	transport.Dispatch.StopWhenDrained(stopRun)
	if err := w.RunPartitionWithTransport(runCtx, identity.Partition(typ, id, provision.Partitions), transport.Dispatch); err != nil {
		return trace, err
	}
	elapsed := schedule.NowMillis()
	reads := journalPort.reads + signalPort.reads
	if leasePort.updates != 51 || journalPort.publishes != 50 || reads != 18 || elapsed != 119*delay || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("signal write cost: elapsed=%d renewals=%d appends=%d reads=%d pending=%d", elapsed, leasePort.updates, journalPort.publishes, reads, transport.Dispatch.Pending())
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	var result wf.Outcome
	if len(records) != 50 || json.Unmarshal(records[49].Payload, &result) != nil || string(result.Result) != "120" {
		return trace, fmt.Errorf("signal result: records=%d result=%s", len(records), result.Result)
	}
	consumed := 0
	for _, record := range records {
		if record.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	if consumed != 16 {
		return trace, fmt.Errorf("consumed signals=%d", consumed)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	if report, err := integrity.CheckSnapshot(snapshot); err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 50, Terminal: 1}) {
		return trace, fmt.Errorf("signal integrity: report=%+v err=%v", report, err)
	}
	gate := "under_30s"
	if elapsed >= 30_000 {
		gate = "known_over_30s_control"
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_signal_write_cost", Subject: fmt.Sprintf("renewals=%d appends=%d reads=%d", leasePort.updates, journalPort.publishes, reads), Sequence: uint64(elapsed), Outcome: gate, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerSignalWriteLatencyReplay(t *testing.T) {
	if path := os.Getenv("SIM_SIGNAL_COST_OUT"); path != "" {
		trace, err := runWorkerSignalWriteLatency(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		trace, err := runWorkerSignalWriteLatency(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "signal-cost-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[trace.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := replayTrace(trace)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 4 {
		t.Fatalf("request latency coverage: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "signal-cost.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerSignalWriteLatencyReplay$")
		cmd.Env = append(os.Environ(), "SIM_SIGNAL_COST_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(data, previous) {
			t.Fatal("signal cost trace changed across processes")
		}
		previous = data
	}
}
