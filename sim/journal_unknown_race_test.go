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

	"js-wf/identity"
	"js-wf/journal"
)

func runTwoWriterUnknownCAS(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("journal_two_writer_unknown"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewJournalTransport(schedule)
	first, err := journal.NewWithAppendPort(model).Append(context.Background(), "test", "unknown-race", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	if err != nil {
		return trace, err
	}
	fault, err := schedule.Choose([]string{string(DropBeforeCommit), string(LoseAckAfterCommit)})
	if err != nil {
		return trace, err
	}
	if err := model.QueueFault(Fault{Kind: AppendFault(fault)}); err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	actors := make([]AppendActor, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		name := name
		actors = append(actors, AppendActor{Name: name, Run: func(ctx context.Context, port journal.AppendPort) error {
			_, err := journal.NewWithAppendPort(port).Append(ctx, "test", "unknown-race", journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2, WorkerID: name}, first)
			return err
		}})
	}
	results, err := RunAppendActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	messages := model.Messages(identity.JournalSubject("test", "unknown-race"))
	if len(messages) != 2 || messages[1].Sequence != first+1 {
		return trace, fmt.Errorf("seed %d fault=%s retained=%+v", seed, fault, messages)
	}
	var winner journal.Entry
	if err := json.Unmarshal(messages[1].Data, &winner); err != nil {
		return trace, err
	}
	unknowns, stales, successes := 0, 0, 0
	for _, outcome := range results {
		switch {
		case errors.Is(outcome, journal.ErrUnknown):
			unknowns++
		case errors.Is(outcome, journal.ErrStale):
			stales++
		case outcome == nil:
			successes++
		default:
			return trace, fmt.Errorf("seed %d fault=%s unexpected outcome=%v", seed, fault, outcome)
		}
	}
	if unknowns != 1 || (fault == string(LoseAckAfterCommit) && (stales != 1 || !errors.Is(results[winner.WorkerID], journal.ErrUnknown))) || (fault == string(DropBeforeCommit) && (successes != 1 || results[winner.WorkerID] != nil)) {
		return trace, fmt.Errorf("seed %d fault=%s winner=%s outcomes=%v", seed, fault, winner.WorkerID, results)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeTwoWriterUnknownReplay(t *testing.T) {
	if os.Getenv("SIM_CAS_UNKNOWN_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoWriterUnknownCAS(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CAS_UNKNOWN_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runTwoWriterUnknownCAS(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-cas-unknown-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-cas-unknown.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runTwoWriterUnknownCAS(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("cas-unknown-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeTwoWriterUnknownReplay$")
		cmd.Env = append(os.Environ(), "SIM_CAS_UNKNOWN_HELPER=1", "SIM_CAS_UNKNOWN_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("CAS unknown trace changed across processes")
	}
}

func runTwoWriterTailLookupRecovery(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("journal_two_writer_tail_lookup"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewJournalTransport(schedule)
	first, err := journal.NewWithAppendPort(model).Append(context.Background(), "test", "tail-lookup", journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1}, 0)
	if err != nil {
		return trace, err
	}
	model.QueueLastFault()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	actors := make([]AppendActor, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		name := name
		actors = append(actors, AppendActor{Name: name, Run: func(ctx context.Context, port journal.AppendPort) error {
			store := journal.NewWithAppendPort(port)
			entry := journal.Entry{Kind: journal.StepRequested, Index: 1, Epoch: 2, WorkerID: name}
			_, err := store.Append(ctx, "test", "tail-lookup", entry, first)
			if errors.Is(err, journal.ErrUnknown) {
				_, err = store.Append(ctx, "test", "tail-lookup", entry, first)
			}
			return err
		}})
	}
	results, err := RunAppendActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	var wins, stales, failedLookups int
	for _, result := range results {
		switch {
		case result == nil:
			wins++
		case errors.Is(result, journal.ErrStale):
			stales++
		default:
			return trace, fmt.Errorf("seed %d unexpected writer outcome: %v", seed, result)
		}
	}
	for _, event := range schedule.Trace().Transport {
		if event.Operation == "last" && event.Outcome == "transient_unavailable" {
			failedLookups++
		}
	}
	messages := model.Messages(identity.JournalSubject("test", "tail-lookup"))
	if wins != 1 || stales != 1 || failedLookups != 1 || len(messages) != 2 || messages[1].Sequence != first+1 {
		return trace, fmt.Errorf("seed %d wins=%d stales=%d failed lookups=%d retained=%v", seed, wins, stales, failedLookups, messages)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeTwoWriterTailLookupRecovery(t *testing.T) {
	if os.Getenv("SIM_CAS_TAIL_LOOKUP_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runTwoWriterTailLookupRecovery(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CAS_TAIL_LOOKUP_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runTwoWriterTailLookupRecovery(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "cas-tail-lookup-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runTwoWriterTailLookupRecovery(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d tail lookup replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("cas-tail-lookup-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestCooperativeTwoWriterTailLookupRecovery$")
		cmd.Env = append(os.Environ(), "SIM_CAS_TAIL_LOOKUP_HELPER=1", "SIM_CAS_TAIL_LOOKUP_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	firstTrace, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	secondTrace, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstTrace, secondTrace) {
		t.Fatal("tail lookup failure trace changed across processes")
	}
}
