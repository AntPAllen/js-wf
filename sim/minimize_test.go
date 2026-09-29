package sim

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

var errProbeInvariant = errors.New("probe invariant failed")

func runProbeFailure(replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(1)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("failure_probe"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	for i := 0; i < 10; i++ {
		choice, err := schedule.Choose([]string{"safe", "unsafe"})
		if err != nil {
			return trace, err
		}
		schedule.RecordTransport(TransportEvent{Operation: "probe", Outcome: choice})
		if choice == "unsafe" {
			if err := schedule.Finish(); err != nil {
				return trace, err
			}
			return trace, errProbeInvariant
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func runActorRemovalProbe(replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(42)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("actor_removal_probe"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	var failed bool
	actors := []CooperativeActor{
		{Name: "bug", Run: func(ctx context.Context, yield YieldFunc) error {
			return yield(ctx, "trigger", func() { failed = true })
		}},
		{Name: "noise", Run: func(ctx context.Context, yield YieldFunc) error {
			for i := 0; i < 3; i++ {
				if err := yield(ctx, fmt.Sprintf("noise_%d", i), func() {}); err != nil {
					return err
				}
			}
			return nil
		}},
	}
	results, err := RunCooperative(context.Background(), schedule, actors)
	if err != nil {
		return trace, err
	}
	for _, result := range results {
		if result != nil {
			return trace, result
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	if failed {
		return trace, errProbeInvariant
	}
	return trace, nil
}

func TestMinimizeFailureTraceRemovesCooperativeActor(t *testing.T) {
	original, err := runActorRemovalProbe(nil)
	if !errors.Is(err, errProbeInvariant) || len(original.Decisions) != 4 {
		t.Fatalf("original actor failure: decisions=%d err=%v", len(original.Decisions), err)
	}
	minimized, runs, err := MinimizeFailureTrace(original, runActorRemovalProbe, func(err error) bool {
		return errors.Is(err, errProbeInvariant)
	}, 100)
	if err != nil || runs < 3 || len(minimized.Decisions) != 1 || !reflect.DeepEqual(minimized.DisabledActors, []string{"noise"}) {
		t.Fatalf("actor reduction: decisions=%d disabled=%v runs=%d err=%v", len(minimized.Decisions), minimized.DisabledActors, runs, err)
	}
	path := filepath.Join(t.TempDir(), "actor-reduced.json")
	if err := minimized.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := runActorRemovalProbe(&loaded)
	if !errors.Is(err, errProbeInvariant) || !reflect.DeepEqual(loaded, replayed) {
		t.Fatalf("actor-reduced disk replay: err=%v equal=%v", err, reflect.DeepEqual(loaded, replayed))
	}
}

func TestMinimizeFailureTracePreservesInvariantAndExactReplay(t *testing.T) {
	seed := int64(0)
	for candidate := int64(1); candidate < 1000; candidate++ {
		choice, err := NewScheduler(candidate).Choose([]string{"safe", "unsafe"})
		if err != nil {
			t.Fatal(err)
		}
		if choice == "unsafe" {
			seed = candidate
			break
		}
	}
	if seed == 0 {
		t.Fatal("no seed chose unsafe first")
	}
	original := Trace{Version: TraceVersion, Seed: seed, Workload: "failure_probe", StepLimit: DefaultMaxSteps, Decisions: make([]Decision, 10), Transport: make([]TransportEvent, 10)}
	for i := range original.Decisions {
		choice := "safe"
		if i == 9 {
			choice = "unsafe"
		}
		original.Decisions[i] = Decision{Enabled: []string{"safe", "unsafe"}, Chosen: choice}
		original.Transport[i] = TransportEvent{Operation: "probe", Outcome: choice}
	}
	minimized, runs, err := MinimizeFailureTrace(original, runProbeFailure, func(err error) bool { return errors.Is(err, errProbeInvariant) }, 100)
	if err != nil || runs < 3 || len(minimized.Decisions) >= len(original.Decisions) {
		t.Fatalf("minimized decisions=%d original=%d runs=%d err=%v", len(minimized.Decisions), len(original.Decisions), runs, err)
	}
	path := filepath.Join(t.TempDir(), "minimized.json")
	if err := minimized.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := runProbeFailure(&loaded)
	if !errors.Is(err, errProbeInvariant) || !reflect.DeepEqual(loaded, got) {
		t.Fatalf("minimized disk replay: %v trace equal=%v", err, reflect.DeepEqual(loaded, got))
	}
}

func TestMinimizerRejectsDifferentFailureAndBudget(t *testing.T) {
	trace := Trace{Version: TraceVersion, Seed: 1, Workload: "probe", StepLimit: DefaultMaxSteps, Decisions: []Decision{}, Transport: []TransportEvent{}}
	_, runs, err := MinimizeFailureTrace(trace, func(*Trace) (Trace, error) { return trace, fmt.Errorf("other error") }, func(err error) bool { return errors.Is(err, errProbeInvariant) }, 2)
	if err == nil || runs != 1 {
		t.Fatalf("different failure accepted: runs=%d err=%v", runs, err)
	}
	if _, _, err := MinimizeFailureTrace(trace, runProbeFailure, func(error) bool { return true }, 1); err == nil {
		t.Fatal("invalid minimization budget accepted")
	}
}
