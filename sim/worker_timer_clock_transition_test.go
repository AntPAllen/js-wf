package sim

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSeededWorkerTimerClockTransitionReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runWorkerTimerBurstScenario(seed, nil, true)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-timer-clock.json")
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("seed %d: %v; save: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runWorkerTimerBurstScenario(seed, &generated, true)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed %d replay: %v", seed, err)
			}
		}
		for _, event := range generated.Transport {
			if event.Operation != "characterize_worker_timer_clock" {
				continue
			}
			if os.Getenv("SIM_WRITE_WORKER_CLOCK_PINS") == "1" && !covered[event.Outcome] {
				if err := generated.Save(filepath.Join("testdata", "regressions", "worker-timer-clock-"+event.Outcome+".json")); err != nil {
					t.Fatal(err)
				}
			}
			covered[event.Outcome] = true
		}
	}
	if len(covered) != 2 {
		t.Fatalf("missing clock directions: %v", covered)
	}
}
