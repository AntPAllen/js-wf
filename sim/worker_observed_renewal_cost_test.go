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
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

type observedRenewalCosts struct {
	SourceCommit string `json:"source_commit"`
	SourceRun    uint64 `json:"source_run"`
	Invocation   string `json:"invocation"`
	LateSignals  int64  `json:"late_signals_after_update_start_ns"`
	Renewals     []struct {
		Index  uint64       `json:"index"`
		Kind   journal.Kind `json:"kind"`
		Update int64        `json:"update_ns"`
	} `json:"renewals"`
}

type observedCostLeasePort struct {
	lease.KVPort
	schedule        *Scheduler
	costs           []int64
	initialization  bool
	initializations int
	renewals        int
	lateAfter       int64
	lateSignals     func()
}

func (p *observedCostLeasePort) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	rev, err := p.KVPort.Create(ctx, key, data)
	if err == nil {
		p.initialization = true
	}
	return rev, err
}
func (p *observedCostLeasePort) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	if p.initialization {
		p.initialization = false
		p.initializations++
		return p.KVPort.Update(ctx, key, data, revision)
	}
	if p.renewals >= len(p.costs) {
		return 0, fmt.Errorf("extra observed-cost renewal %d", p.renewals)
	}
	delay := p.costs[p.renewals]
	if p.renewals == 3 {
		before := min(delay, p.lateAfter)
		if err := p.schedule.AdvanceMillis(before); err != nil {
			return 0, err
		}
		p.lateSignals()
		delay -= before
	}
	if err := p.schedule.AdvanceMillis(delay); err != nil {
		return 0, err
	}
	p.renewals++
	return p.KVPort.Update(ctx, key, data, revision)
}

// Project the real failure's observed renewal costs through production worker
// decisions. This is not a broker/heartbeat/reply-loss or full-cluster replay.
func runWorkerObservedRenewalCost(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_observed_renewal_cost"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"floor", "ceil", "zero"})
	if err != nil {
		return trace, err
	}
	raw, err := os.ReadFile("testdata/costs/fcf4537-seed4-renewals.json")
	if err != nil {
		return trace, err
	}
	var fixture observedRenewalCosts
	if err := json.Unmarshal(raw, &fixture); err != nil {
		return trace, err
	}
	if fixture.SourceRun != 36835269534 || fixture.SourceCommit != "fcf45374293d130b86af241eb90c78f61e73671c" || len(fixture.Renewals) != 52 || fixture.LateSignals != 317721021 {
		return trace, fmt.Errorf("wrong observed-cost fixture")
	}
	schedule.RecordTransport(TransportEvent{Operation: "observed_cost_source", Subject: fixture.Invocation, Outcome: fixture.SourceCommit, Sequence: fixture.SourceRun, DataSHA256: digest(raw), AtMillis: schedule.NowMillis()})
	costs := make([]int64, len(fixture.Renewals))
	var rawTotal, sum int64
	for i, row := range fixture.Renewals {
		if row.Index != uint64(i) || row.Update < 0 || row.Kind == "" {
			return trace, fmt.Errorf("invalid source renewal %d", i)
		}
		rawTotal += row.Update
		costs[i] = row.Update / int64(time.Millisecond)
		if mode == "ceil" {
			costs[i] = (row.Update + int64(time.Millisecond) - 1) / int64(time.Millisecond)
		}
		if mode == "zero" {
			costs[i] = 0
		}
		sum += costs[i]
	}
	if rawTotal != 30500405601 {
		return trace, fmt.Errorf("source costs changed: %d", rawTotal)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const typ, id = "test", "observed-cost"
	transport := NewWorkerTransport(schedule, worker.DefaultAckWait)
	journals := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journals, journals)
	outcomes := NewKVTransport(schedule, 0)
	port := &observedCostLeasePort{KVPort: NewKVTransport(schedule, provision.LeaseTTL), schedule: schedule, costs: costs, lateAfter: (fixture.LateSignals + 999999) / 1000000}
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	var operations []worker.OperationEvent
	var expectedIndex uint64
	observe := func(event worker.OperationEvent) { operations = append(operations, event) }
	handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
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
	}}
	makeWorker := func(name string) (*worker.Worker, error) {
		return worker.NewWithPorts(name, handlers, worker.ModeledWorkerPorts{Journal: store, Leases: lease.NewWithKVPort(port), Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationObserver: observe, OperationNow: func() time.Time { return time.UnixMilli(schedule.NowMillis()) }})
	}
	first, err := makeWorker("observed-before")
	if err != nil {
		return trace, err
	}
	runOne := func(w *worker.Worker) error {
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		transport.Dispatch.StopAfterNextAck(stop)
		return w.RunPartitionWithTransport(runCtx, identity.Partition(typ, id, provision.Partitions), transport.Dispatch)
	}
	if err := runOne(first); err != nil {
		return trace, err
	}
	publish := func(start, end int) {
		for i := start; i < end; i++ {
			headers := nats.Header{}
			headers.Set("Wf-Inv-Seq", fmt.Sprint(handle.InvSeq))
			transport.CommitSignal(&nats.Msg{Subject: "wf.sig." + typ + "." + id + ".go", Header: headers, Data: []byte(fmt.Sprint(i))})
		}
	}
	var enabled int64
	port.lateSignals = func() { publish(12, 16); enabled = schedule.NowMillis() }
	publish(0, 12)
	if err := c.Enqueue(ctx, typ, id, "observed-wave-one"); err != nil {
		return trace, err
	}
	if err := runOne(first); err != nil {
		return trace, err
	}
	middle, _, err := store.Read(ctx, typ, id)
	if err != nil || len(middle) != 40 || middle[39].Kind != journal.Suspended {
		return trace, fmt.Errorf("first-wave cut entries=%d err=%v", len(middle), err)
	}
	second, err := makeWorker("observed-after")
	if err != nil {
		return trace, err
	}
	if err := c.Enqueue(ctx, typ, id, "observed-wave-two"); err != nil {
		return trace, err
	}
	if err := runOne(second); err != nil {
		return trace, err
	}
	if port.initializations != 3 || port.renewals != 52 || schedule.NowMillis() != sum || transport.Dispatch.Pending() != 0 {
		return trace, fmt.Errorf("renewals=%d init=%d elapsed=%d want=%d pending=%d", port.renewals, port.initializations, schedule.NowMillis(), sum, transport.Dispatch.Pending())
	}
	for _, event := range operations {
		if event.Operation != "lease_renew_append" {
			continue
		}
		if expectedIndex >= uint64(len(fixture.Renewals)) || event.JournalIndex != expectedIndex || event.JournalKind != fixture.Renewals[expectedIndex].Kind || event.Error != "" || !event.LeaseUpdateAttempted || event.LeaseUpdateDuration != time.Duration(costs[expectedIndex])*time.Millisecond {
			return trace, fmt.Errorf("observed renewal index=%d: %+v", expectedIndex, event)
		}
		expectedIndex++
	}
	if expectedIndex != 52 {
		return trace, fmt.Errorf("missing observed renewals: %d", expectedIndex)
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	consumed := 0
	for _, record := range records {
		if record.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	if consumed != 16 || len(records) != 52 {
		return trace, fmt.Errorf("signals=%d records=%d", consumed, len(records))
	}
	value, err := outcomes.Get(ctx, identity.Key(typ, id))
	var outcome wf.Outcome
	if err != nil || json.Unmarshal(value.Value, &outcome) != nil || string(outcome.Result) != "120" {
		return trace, fmt.Errorf("outcome=%+v err=%v", outcome, err)
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report != (integrity.Report{Invocations: 1, Journals: 1, Entries: 52, Terminal: 1}) {
		return trace, fmt.Errorf("integrity=%+v err=%v", report, err)
	}
	elapsed := schedule.NowMillis() - enabled
	gate := "known_observed_over_30s_control"
	if mode == "zero" {
		gate = "under_30s"
		if elapsed != 0 {
			return trace, fmt.Errorf("zero-cost liveness=%d", elapsed)
		}
	} else if elapsed < 30000 {
		return trace, fmt.Errorf("observed latency control did not miss gate: %d", elapsed)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_observed_renewal_cost", Subject: mode, Sequence: uint64(elapsed), Outcome: gate, AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}

func TestSeededWorkerObservedRenewalCost(t *testing.T) {
	if path := os.Getenv("SIM_OBSERVED_COST_OUT"); path != "" {
		seed := int64(42)
		if value := os.Getenv("FAULT_SEED"); value != "" {
			var err error
			seed, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		trace, err := runWorkerObservedRenewalCost(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		trace, err := runWorkerObservedRenewalCost(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "observed-cost-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[trace.Decisions[0].Chosen]++
		if seed <= 10 {
			again, err := replayTrace(trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("replay seed=%d: %v", seed, err)
			}
		}
	}
	if len(observed) != 3 {
		t.Fatalf("mode coverage: %v", observed)
	}
	t.Logf("observed cost profiles=%v; floor/ceil are expected failing liveness controls, zero is the clean cost control", observed)
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "observed-cost.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerObservedRenewalCost$")
		cmd.Env = append(os.Environ(), "SIM_OBSERVED_COST_OUT="+path, "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(raw, previous) {
			t.Fatal("observed cost trace changed across processes")
		}
		previous = raw
	}
}
