package sim

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func endlessActor(ctx context.Context, yield YieldFunc) error {
	for {
		if err := yield(ctx, "tick", func() {}); err != nil {
			return err
		}
	}
}

func TestStepLimitFailsWithPendingActionsAndReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limit.json")
	generated := NewScheduler(17)
	if err := generated.SetWorkload("limit_probe"); err != nil {
		t.Fatal(err)
	}
	if err := generated.SetMaxSteps(3); err != nil {
		t.Fatal(err)
	}
	actors := []CooperativeActor{{Name: "loop", Run: endlessActor}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := RunCooperative(ctx, generated, actors)
	var limit *StepLimitError
	if !errors.Is(err, ErrStepLimit) || !errors.As(err, &limit) || limit.Limit != 3 || !reflect.DeepEqual(limit.Enabled, []string{"loop:tick"}) || limit.LastChosen != "loop:tick" {
		t.Fatalf("step-limit diagnostic: %+v err=%v", limit, err)
	}
	if got := len(generated.Trace().Decisions); got != 3 {
		t.Fatalf("recorded %d choices, want three", got)
	}
	if err := generated.Trace().Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := ReplayScheduler(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := replayed.SetWorkload("limit_probe"); err != nil {
		t.Fatal(err)
	}
	_, err = RunCooperative(ctx, replayed, actors)
	var replayLimit *StepLimitError
	if !errors.As(err, &replayLimit) || !reflect.DeepEqual(limit, replayLimit) || !reflect.DeepEqual(generated.Trace(), replayed.Trace()) {
		t.Fatalf("step-limit replay: generated=%+v replay=%+v err=%v", limit, replayLimit, err)
	}
}

func TestCooperativeStallListsUnfinishedActors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := RunCooperative(ctx, NewScheduler(18), []CooperativeActor{{Name: "blocked", Run: func(ctx context.Context, _ YieldFunc) error {
		<-ctx.Done()
		return ctx.Err()
	}}})
	var stalled *StallError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &stalled) || !reflect.DeepEqual(stalled.Unfinished, []string{"blocked"}) || len(stalled.Pending) != 0 {
		t.Fatalf("stall diagnostic: %+v err=%v", stalled, err)
	}
}

func TestSchedulerRejectsVirtualClockOverflowAndInvalidTraceLimit(t *testing.T) {
	schedule := NewScheduler(19)
	if err := schedule.AdvanceMillis(math.MaxInt64); err != nil {
		t.Fatal(err)
	}
	if err := schedule.AdvanceMillis(1); err == nil || !strings.Contains(err.Error(), "invalid virtual delay") {
		t.Fatalf("overflow: %v", err)
	}
	trace := schedule.Trace()
	trace.Workload = "probe"
	trace.StepLimit = 0
	if _, err := trace.Marshal(); err == nil {
		t.Fatal("version 3 trace without step limit was accepted")
	}
	trace.Version = 2
	trace.Decisions = []Decision{{AtMillis: 0, Enabled: []string{"advance"}, Chosen: "advance"}}
	legacy, err := ReplayScheduler(trace)
	if err != nil {
		t.Fatalf("legacy version 2 trace rejected: %v", err)
	}
	if err := legacy.SetWorkload("probe"); err != nil {
		t.Fatal(err)
	}
	if choice, err := legacy.Choose([]string{"advance"}); err != nil || choice != "advance" {
		t.Fatalf("legacy trace choice=%q err=%v", choice, err)
	}
	if err := legacy.Finish(); err != nil {
		t.Fatalf("legacy trace did not replay: %v", err)
	}
}
