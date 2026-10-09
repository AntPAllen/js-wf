package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// The four dimensions are independent scheduler choices, rather than mutually
// exclusive recipes. Production repair and worker replay must resolve all cuts.
func TestSeededGraphSignalRuntimeCombinedReplay(t *testing.T) {
	observed := map[string]int{}
	dimensions := []map[string]int{{}, {}, {}, {}}
	captures := map[string]bool{}
	fail := func(seed int64, trace Trace, err error) {
		path, e := saveSeedFailureTrace(t.Name(), seed, trace)
		if e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
	}
	for seed := range seededSchedules(t) {
		generated, e := runGraphSignalRuntimeCombined(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		replayed, e := runGraphSignalRuntimeCombined(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("replay differs: %v", e))
		}
		if len(generated.Decisions) < 4 {
			fail(seed, generated, fmt.Errorf("missing independent dimensions"))
		}
		cuts := 0
		key := ""
		for i := range dimensions {
			choice := generated.Decisions[i].Chosen
			dimensions[i][choice]++
			if i > 0 {
				key += "/"
			}
			key += choice
			if choice != "healthy" {
				cuts++
			}
		}
		observed[key]++
		capture := ""
		if cuts >= 3 {
			capture = "three-or-more-cuts"
		}
		if generated.Decisions[0].Chosen == "batch_restart" && cuts >= 3 {
			capture = "batch-with-combined-cuts"
		}
		if generated.Decisions[3].Chosen == "prepared_append_repair" && cuts >= 3 {
			capture = "prepared-repair-with-combined-cuts"
		}
		if capture != "" && !captures[capture] {
			captures[capture] = true
			if dir := os.Getenv("SIM_GRAPH_SIGNAL_COMBINED_ROOT"); dir != "" {
				if e = generated.Save(filepath.Join(dir, capture+".json")); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	for i, want := range []int{7, 4, 3, 4} {
		if len(dimensions[i]) != want {
			t.Fatalf("dimension %d coverage=%v", i, dimensions[i])
		}
	}
	if len(captures) != 3 || len(observed) < 100 {
		t.Fatalf("combined coverage=%d captures=%v", len(observed), captures)
	}
	t.Logf("independent publication/discovery/enqueue/consumption choices: %d distinct combinations; dimensions=%v; all injected port faults reached; effect one, exact consumed prefix, terminal equality and complete fixture drain", len(observed), dimensions)
}
