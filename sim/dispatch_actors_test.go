package sim

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func runTwoDispatchWorkers(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("dispatch_two_workers"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewDispatchTransport(schedule, 3*time.Second)
	for i := 0; i < 10; i++ {
		model.PublishRun("wf.run.0", []byte(strconv.Itoa(i)))
	}
	if choice, err := schedule.Choose([]string{"leader_change", "steady"}); err != nil {
		return trace, err
	} else if choice == "leader_change" {
		if err := model.QueueFault(DispatchFault{Operation: "fetch", Kind: "leader_changed"}); err != nil {
			return trace, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	loopCtx, stopLoops := context.WithCancel(ctx)
	defer stopLoops()
	var mu sync.Mutex
	acked := map[int]bool{}
	maxDelivery := map[int]uint64{}
	var handlerErr error
	handle := func(_ context.Context, msg jetstream.Msg) {
		id, err := strconv.Atoi(string(msg.Data()))
		if err != nil || id < 0 || id >= 10 {
			mu.Lock()
			handlerErr = fmt.Errorf("invalid dispatch message %q: %v", msg.Data(), err)
			mu.Unlock()
			stopLoops()
			return
		}
		metadata, err := msg.Metadata()
		if err != nil {
			mu.Lock()
			handlerErr = err
			mu.Unlock()
			stopLoops()
			return
		}
		mu.Lock()
		if metadata.NumDelivered > maxDelivery[id] {
			maxDelivery[id] = metadata.NumDelivered
		}
		mu.Unlock()
		if id%3 == 0 && metadata.NumDelivered == 1 {
			return
		}
		if err := msg.Ack(); err != nil {
			mu.Lock()
			handlerErr = err
			mu.Unlock()
			stopLoops()
			return
		}
		mu.Lock()
		acked[id] = true
		complete := len(acked) == 10
		mu.Unlock()
		if complete {
			stopLoops()
		}
	}
	actors := []DispatchActor{}
	for _, name := range []string{"alpha", "beta"} {
		actors = append(actors, DispatchActor{Name: name, Run: func(_ context.Context, port worker.DispatchPort) error {
			return worker.RunPartitionWithPort(loopCtx, 0, port, handle, 1)
		}})
	}
	results, err := RunDispatchActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	mu.Lock()
	defer mu.Unlock()
	if results["alpha"] != nil || results["beta"] != nil || handlerErr != nil || len(acked) != 10 || model.Pending() != 0 {
		return trace, fmt.Errorf("two-worker result=%v handler=%v acked=%d pending=%d", results, handlerErr, len(acked), model.Pending())
	}
	for _, id := range []int{0, 3, 6, 9} {
		if maxDelivery[id] < 2 {
			return trace, fmt.Errorf("message %d was not redelivered", id)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeTwoDispatchWorkersReplay(t *testing.T) {
	if os.Getenv("SIM_TWO_DISPATCH_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoDispatchWorkers(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TWO_DISPATCH_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runTwoDispatchWorkers(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-two-dispatch-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-two-dispatch.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runTwoDispatchWorkers(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d two-worker replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("two-worker-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeTwoDispatchWorkersReplay$")
		cmd.Env = append(os.Environ(), "SIM_TWO_DISPATCH_HELPER=1", "SIM_TWO_DISPATCH_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("two-worker child %d: %v: %s", i, err, output)
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
		t.Fatal("two-worker dispatch trace changed across processes")
	}
}
