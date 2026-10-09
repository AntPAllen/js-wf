package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var graphAwaitContentionModes = []string{"observe", "pinned", "release", "generation", "purge", "cancel", "unknown", "corrupt"}

func runGraphAwaitContention(seed int64, replay *Trace) (trace Trace, err error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	defer func() { trace = schedule.Trace() }()
	if err = schedule.SetWorkload("graph_await_contention"); err != nil {
		return trace, err
	}
	cases := make([]string, 0, 24)
	for _, version := range []int{4, 5, 6} {
		for _, mode := range graphAwaitContentionModes {
			cases = append(cases, fmt.Sprintf("v%d_%s", version, mode))
		}
	}
	chosen, err := schedule.Choose(cases)
	if err != nil {
		return trace, err
	}
	parts := strings.SplitN(chosen, "_", 2)
	version, err := strconv.Atoi(strings.TrimPrefix(parts[0], "v"))
	if err != nil {
		return trace, err
	}
	return RunGraphAwaitContentionSchedule(schedule, version, parts[1])
}

func TestSeededGraphAwaitContentionReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, err error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-await-contention-failure-")
			if e != nil {
				t.Fatal(e)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphAwaitContention(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphAwaitContention(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("Await contention replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_AWAIT_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, "graph-await-"+strings.ReplaceAll(mode, "_", "-")+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != 3*len(graphAwaitContentionModes) {
		t.Fatalf("Await coverage=%v", observed)
	}
	t.Logf("Await contention: modes=%v; captured generation, fresh retry, cancellation/unknown/corruption fail closed and orphan drain", observed)
}
