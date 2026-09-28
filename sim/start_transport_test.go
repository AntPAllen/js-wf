package sim

import (
	"bytes"
	"context"
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
)

func TestStartTransportFaultBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("dropped publish retries without duplicate", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(1))
		if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "drop_before_commit"}); err != nil {
			t.Fatal(err)
		}
		handle, err := client.NewWithStartPort(model).Start(ctx, "test", "one", []byte(`1`))
		if err != nil || handle.InvSeq == 0 || len(model.Runs()) != 1 {
			t.Fatalf("start after dropped publish: handle=%+v runs=%v err=%v", handle, model.Runs(), err)
		}
	})
	t.Run("lost invocation ack stores once without enqueue", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(2))
		if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"}); err != nil {
			t.Fatal(err)
		}
		c := client.NewWithStartPort(model)
		first, err := c.Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrAlreadyStarted) || first.InvSeq == 0 || len(model.Runs()) != 0 {
			t.Fatalf("lost invocation ack: handle=%+v runs=%v err=%v", first, model.Runs(), err)
		}
		second, err := c.Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrAlreadyStarted) || second.InvSeq != first.InvSeq || len(model.Runs()) != 0 {
			t.Fatalf("matching retry: handle=%+v runs=%v err=%v", second, model.Runs(), err)
		}
		_, err = c.Start(ctx, "test", "one", []byte(`2`))
		if !errors.Is(err, client.ErrInputMismatch) {
			t.Fatalf("changed retry: %v", err)
		}
	})
	t.Run("lost enqueue ack commits exactly once", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(3))
		if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"}); err != nil {
			t.Fatal(err)
		}
		handle, err := client.NewWithStartPort(model).Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrEnqueueUnknown) || handle.InvSeq == 0 || len(model.Runs()) != 1 {
			t.Fatalf("lost run ack: handle=%+v runs=%v err=%v", handle, model.Runs(), err)
		}
	})
	t.Run("large input retains object and hash", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(4))
		input := bytes.Repeat([]byte("x"), client.MaxInlineInput+1)
		if _, err := client.NewWithStartPort(model).Start(ctx, "test", "large", input); err != nil {
			t.Fatal(err)
		}
		stored, ok := model.Invocation(identity.InvocationSubject("test", "large"))
		if !ok || len(stored.Data) != 0 || !bytes.Equal(model.Input(stored.Header.Get("Wf-Input-Ref")), input) {
			t.Fatal("large input object or invocation reference differs")
		}
	})
	t.Run("stale read after duplicate is retried", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(5))
		c := client.NewWithStartPort(model)
		first, err := c.Start(ctx, "test", "one", []byte(`1`))
		if err != nil {
			t.Fatal(err)
		}
		if err := model.QueueFault(StartFault{Operation: "last_invocation", Kind: "stale_read"}); err != nil {
			t.Fatal(err)
		}
		second, err := c.Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrAlreadyStarted) || second.InvSeq != first.InvSeq || len(model.Runs()) != 1 {
			t.Fatalf("stale-read retry: handle=%+v runs=%v err=%v", second, model.Runs(), err)
		}
	})
}

func runSeededStartScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("client_start_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewStartTransport(schedule)
	c := client.NewWithStartPort(model)
	committedRuns := 0
	for i := 0; i < 20; i++ {
		choice, err := schedule.Choose([]string{"normal", "lost_inv_ack", "lost_run_ack", "drop_run"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("case-%02d", i)
		switch choice {
		case "lost_inv_ack":
			err = model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"})
		case "lost_run_ack":
			err = model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
		case "drop_run":
			err = model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
		}
		if err != nil {
			return trace, err
		}
		input := []byte(fmt.Sprintf(`{"case":%d}`, i))
		handle, startErr := c.Start(context.Background(), "test", id, input)
		stored, ok := model.Invocation(identity.InvocationSubject("test", id))
		if !ok || stored.Sequence != handle.InvSeq || handle.InvSeq == 0 {
			return trace, fmt.Errorf("seed %d case %d missing invocation: handle=%+v", seed, i, handle)
		}
		switch choice {
		case "normal":
			committedRuns++
			if startErr != nil {
				return trace, fmt.Errorf("seed %d case %d normal: %w", seed, i, startErr)
			}
		case "lost_inv_ack":
			if !errors.Is(startErr, client.ErrAlreadyStarted) {
				return trace, fmt.Errorf("seed %d case %d lost inv ack: %v", seed, i, startErr)
			}
		case "lost_run_ack":
			committedRuns++
			if !errors.Is(startErr, client.ErrEnqueueUnknown) {
				return trace, fmt.Errorf("seed %d case %d lost run ack: %v", seed, i, startErr)
			}
		case "drop_run":
			if !errors.Is(startErr, client.ErrEnqueueUnknown) {
				return trace, fmt.Errorf("seed %d case %d drop run: %v", seed, i, startErr)
			}
		}
		if got := len(model.Runs()); got != committedRuns {
			return trace, fmt.Errorf("seed %d case %d retained %d runs, want %d", seed, i, got, committedRuns)
		}
		retry, retryErr := c.Start(context.Background(), "test", id, input)
		if !errors.Is(retryErr, client.ErrAlreadyStarted) || retry.InvSeq != handle.InvSeq {
			return trace, fmt.Errorf("seed %d case %d matching retry: handle=%+v err=%v", seed, i, retry, retryErr)
		}
		_, mismatchErr := c.Start(context.Background(), "test", id, []byte(`changed`))
		if !errors.Is(mismatchErr, client.ErrInputMismatch) {
			return trace, fmt.Errorf("seed %d case %d mismatched retry: %v", seed, i, mismatchErr)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededStartModelReplay(t *testing.T) {
	if os.Getenv("SIM_START_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededStartScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_START_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := int64(1); seed <= 1000; seed++ {
		generated, err := runSeededStartScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-start-sim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-start.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededStartScenario(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d start trace replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("start-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededStartModelReplay$")
		cmd.Env = append(os.Environ(), "SIM_START_TRACE_HELPER=1", "SIM_START_TRACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("seeded start child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded start trace changed across processes")
	}
}

func runTwoStarterRace(seed int64, mismatch bool, replay *Trace) (trace Trace, runErr error) {
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
	workload := "client_two_starter_same"
	if mismatch {
		workload = "client_two_starter_mismatch"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewStartTransport(schedule)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	handles := [2]client.Handle{}
	actors := make([]StartActor, 0, 2)
	for i, name := range []string{"alpha", "beta"} {
		i, name := i, name
		actors = append(actors, StartActor{Name: name, Run: func(ctx context.Context, port client.StartPort) error {
			input := []byte(`same`)
			if mismatch && i == 1 {
				input = []byte(`different`)
			}
			var err error
			handles[i], err = client.NewWithStartPort(port).Start(ctx, "test", "race", input)
			return err
		}})
	}
	results, err := RunStartActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	stored, ok := model.Invocation(identity.InvocationSubject("test", "race"))
	if !ok || len(model.Runs()) != 1 || handles[0].InvSeq != stored.Sequence || handles[1].InvSeq != stored.Sequence {
		return trace, fmt.Errorf("race state: stored=%+v runs=%v handles=%v", stored, model.Runs(), handles)
	}
	successes, already, changed := 0, 0, 0
	for _, result := range results {
		switch {
		case result == nil:
			successes++
		case errors.Is(result, client.ErrAlreadyStarted):
			already++
		case errors.Is(result, client.ErrInputMismatch):
			changed++
		default:
			return trace, fmt.Errorf("unexpected race result: %v", result)
		}
	}
	if successes != 1 || !mismatch && already != 1 || mismatch && changed != 1 {
		return trace, fmt.Errorf("race results: success=%d already=%d mismatch=%d", successes, already, changed)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeStartRacesReplay(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		for _, mismatch := range []bool{false, true} {
			generated, err := runTwoStarterRace(seed, mismatch, nil)
			if err != nil {
				t.Fatalf("FAULT_SEED=%d mismatch=%v: %v", seed, mismatch, err)
			}
			replayed, err := runTwoStarterRace(seed, mismatch, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d mismatch=%v replay: %v", seed, mismatch, err)
			}
		}
	}
}
