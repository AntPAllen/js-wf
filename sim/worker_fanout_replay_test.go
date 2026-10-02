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
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

// A returned cancellation interrupts one delivery at a durable SDK boundary.
// This models parent replay, not a SIGKILL or abandonment of lease cleanup.
func runSeededWorkerFanout(seed int64, replay *Trace, count int) (trace Trace, runErr error) {
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
	defer func() { trace = schedule.Trace() }()
	if err := schedule.SetWorkload(fmt.Sprintf("worker_fanout_%d", count)); err != nil {
		return trace, err
	}
	phase, err := schedule.Choose([]string{"creation", "results"})
	if err != nil {
		return trace, err
	}
	cuts := make([]string, count)
	for i := range cuts {
		cuts[i] = strconv.Itoa(i)
	}
	selected, err := schedule.Choose(cuts)
	if err != nil {
		return trace, err
	}
	cut, _ := strconv.Atoi(selected)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	const parentType, parentID, childType = "parent", "fanout-boundary", "child"
	transport := NewWorkerTransport(schedule, 3*time.Second)
	journals := NewJournalTransport(schedule)
	store := journal.NewWithPorts(journals, journals)
	leasing := lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second))
	outcomes := NewKVTransport(schedule, 0)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	effects := make([]int, count)
	var interrupted bool
	var prefix []journal.Record
	handlers := map[string]worker.Handler{
		parentType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			promises := make([]wf.Promise, count)
			for i := range promises {
				promise, err := wf.CallAsync(c, childType, json.RawMessage(strconv.Itoa(i)))
				if err != nil {
					return nil, err
				}
				promises[i] = promise
				if !interrupted && phase == "creation" && i == cut {
					interrupted = true
					prefix, _, err = store.Read(ctx, parentType, parentID)
					if err != nil {
						return nil, err
					}
					return nil, context.Canceled
				}
			}
			sum := 0
			for i, promise := range promises {
				value, err := wf.AwaitPromise(c, promise)
				if err != nil {
					return nil, err
				}
				var n int
				if err := json.Unmarshal(value, &n); err != nil {
					return nil, err
				}
				if !interrupted && phase == "results" && i == cut {
					interrupted = true
					prefix, _, err = store.Read(ctx, parentType, parentID)
					if err != nil {
						return nil, err
					}
					return nil, context.Canceled // before adding the decoded result locally
				}
				sum += n
			}
			return json.Marshal(sum)
		},
		childType: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			var n int
			if err := json.Unmarshal(input, &n); err != nil || n < 0 || n >= count {
				return nil, fmt.Errorf("invalid child input %s", input)
			}
			value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { effects[n]++; return n * 2, nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
	}
	newWorker := func(id string) (*worker.Worker, error) {
		return worker.NewWithPorts(id, handlers, worker.ModeledWorkerPorts{Journal: store, Leases: leasing, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)})
	}
	w, err := newWorker("fanout-original")
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, parentType, parentID, []byte(`null`)); err != nil {
		return trace, err
	}
	successor := false
	for deliveries := 0; transport.Dispatch.Pending() > 0; deliveries++ {
		if err := ctx.Err(); err != nil {
			return trace, fmt.Errorf("model wall-clock deadline after %d deliveries: %w", deliveries, err)
		}
		if deliveries > count*20+100 {
			return trace, fmt.Errorf("fanout did not drain: pending=%d", transport.Dispatch.Pending())
		}
		subject, err := schedule.Choose(transport.Dispatch.PendingSubjects())
		if err != nil {
			return trace, err
		}
		part, err := strconv.ParseUint(strings.TrimPrefix(subject, "wf.run."), 10, 32)
		if err != nil {
			return trace, err
		}
		runCtx, stop := context.WithCancel(ctx)
		transport.Dispatch.StopAfterNextAck(stop)
		transport.Dispatch.StopAfterNextNak(stop)
		err = w.RunPartitionWithTransport(runCtx, uint32(part), transport.Dispatch)
		stop()
		if err != nil {
			return trace, fmt.Errorf("delivery %d: %w", deliveries, err)
		}
		if interrupted && !successor {
			if len(prefix) == 0 || prefix[len(prefix)-1].Kind != journal.StepCompleted {
				return trace, fmt.Errorf("interruption did not retain completed SDK boundary")
			}
			w, err = newWorker("fanout-successor")
			if err != nil {
				return trace, err
			}
			successor = true
		}
	}
	if !interrupted || !successor {
		return trace, fmt.Errorf("selected boundary not exercised")
	}
	created, awaited, completed := 0, 0, 0
	for _, record := range prefix {
		if record.Kind == journal.StepCompleted {
			completed++
		}
		if record.Kind != journal.StepRequested {
			continue
		}
		var request struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(record.Payload, &request); err != nil {
			return trace, err
		}
		switch request.Kind {
		case "call_async":
			created++
		case "signal":
			awaited++
		}
	}
	wantCreated, wantAwaited := cut+1, 0
	if phase == "results" {
		wantCreated, wantAwaited = count, cut+1
	}
	if created != wantCreated || awaited != wantAwaited || completed != wantCreated+wantAwaited {
		return trace, fmt.Errorf("wrong interruption boundary created=%d awaited=%d completed=%d", created, awaited, completed)
	}
	records, _, err := store.Read(ctx, parentType, parentID)
	if err != nil {
		return trace, err
	}
	if len(records) <= len(prefix) || !reflect.DeepEqual(prefix, records[:len(prefix)]) {
		return trace, fmt.Errorf("committed parent prefix changed")
	}
	terminal := records[len(records)-1]
	var out wf.Outcome
	if err := json.Unmarshal(terminal.Payload, &out); err != nil {
		return trace, err
	}
	if terminal.Kind != journal.Completed || string(out.Result) != strconv.Itoa(count*(count-1)) || terminal.Epoch <= prefix[len(prefix)-1].Epoch || terminal.WorkerID != "fanout-successor" {
		return trace, fmt.Errorf("incorrect parent terminal kind=%s result=%s epoch=%d", terminal.Kind, out.Result, terminal.Epoch)
	}
	ids := map[string]bool{}
	for _, record := range records {
		if record.Kind != journal.StepRequested {
			continue
		}
		var request struct {
			Kind    string `json:"kind"`
			ChildID string `json:"child_id"`
		}
		if err := json.Unmarshal(record.Payload, &request); err != nil {
			return trace, err
		}
		if request.Kind != "call_async" {
			continue
		}
		if request.ChildID == "" || ids[request.ChildID] {
			return trace, fmt.Errorf("missing or repeated child identity")
		}
		ids[request.ChildID] = true
		childIndex := len(ids) - 1
		child, _, err := store.Read(ctx, childType, request.ChildID)
		if err != nil || len(child) != 4 || child[3].Kind != journal.Completed {
			return trace, fmt.Errorf("child %s incomplete: %v", request.ChildID, err)
		}
		var childOut wf.Outcome
		if err := json.Unmarshal(child[3].Payload, &childOut); err != nil || string(childOut.Result) != strconv.Itoa(childIndex*2) {
			return trace, fmt.Errorf("child %d result=%s err=%v", childIndex, childOut.Result, err)
		}
	}
	if len(ids) != count {
		return trace, fmt.Errorf("children=%d want=%d", len(ids), count)
	}
	for i, executed := range effects {
		if executed != 1 {
			return trace, fmt.Errorf("child %d effects=%d", i, executed)
		}
	}
	snapshot, err := retainedModelSnapshot(transport.StartTransport, journals, outcomes)
	if err != nil {
		return trace, err
	}
	report, err := integrity.CheckSnapshot(snapshot)
	if err != nil || report.Invocations != count+1 || report.Journals != count+1 || report.Terminal != count+1 {
		return trace, fmt.Errorf("fanout retained=%+v err=%v", report, err)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_fanout", Outcome: phase + ":" + selected, Sequence: uint64(count), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerFanoutInterruptedReplay(t *testing.T) {
	if os.Getenv("SIM_FANOUT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerFanout(seed, nil, 6)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_FANOUT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededWorkerFanout(seed, nil, 6)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-fanout-failure-")
				if dirErr != nil {
					t.Fatal(dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed=%d: %v; save: %v", seed, err, saveErr)
			}
			t.Fatalf("seed=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededWorkerFanout(seed, &generated, 6)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
	}
	var data [2][]byte
	for i := range data {
		path := filepath.Join(t.TempDir(), "fanout.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerFanoutInterruptedReplay$")
		cmd.Env = append(os.Environ(), "SIM_FANOUT_HELPER=1", "FAULT_SEED=42", "SIM_FANOUT_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("helper %d: %v: %s", i, err, output)
		}
		var err error
		data[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(data[0], data[1]) {
		t.Fatal("fanout trace changed across processes")
	}
}

func TestFiveHundredChildModeledParentInterruption(t *testing.T) {
	for _, seed := range []int64{2, 42} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			generated, err := runSeededWorkerFanout(seed, nil, 500)
			if err != nil {
				t.Fatalf("seed=%d: %v", seed, err)
			}
			phase := "results"
			if seed == 2 {
				phase = "creation"
			}
			if generated.Decisions[0].Chosen != phase {
				t.Fatalf("seed=%d phase=%s want=%s", seed, generated.Decisions[0].Chosen, phase)
			}
			t.Logf("seed=%d phase=%s cut=%s", seed, phase, generated.Decisions[1].Chosen)
			replayed, err := runSeededWorkerFanout(seed, &generated, 500)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		})
	}
}
