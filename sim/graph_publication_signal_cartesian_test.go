package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Exhaust the declared four-dimensional fault domain in addition to the
// contiguous seeded gate. Seed discovery performs only scheduler choices; every
// selected seed then executes and exactly replays the real production fixture.
func TestGraphSignalRuntimeCartesianReplay(t *testing.T) {
	dimensions := [][]string{
		{"healthy", "source_drop", "source_lost_ack", "queue_drop", "queue_lost_readback", "reserved", "batch_restart"},
		{"healthy", "catalog_drop", "lifecycle_unknown", "dry_catalog"},
		{"healthy", "enqueue_drop", "enqueue_lost_ack"},
		{"healthy", "consumption_lost_ack", "consumption_unknown", "prepared_append_repair"},
	}
	want := 1
	for _, options := range dimensions {
		sort.Strings(options)
		want *= len(options)
	}
	seeds := map[string]int64{}
	for seed := int64(1); seed <= 100000 && len(seeds) < want; seed++ {
		schedule := NewScheduler(seed)
		choices := make([]string, len(dimensions))
		for i, options := range dimensions {
			choice, err := schedule.Choose(options)
			if err != nil {
				t.Fatal(err)
			}
			choices[i] = choice
		}
		key := strings.Join(choices, "/")
		if _, exists := seeds[key]; !exists {
			seeds[key] = seed
		}
	}
	if len(seeds) != want || want != 336 {
		t.Fatalf("Cartesian seed discovery incomplete: %d/%d", len(seeds), want)
	}
	keys := make([]string, 0, want)
	for key := range seeds {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	completed, beyondFirstThousand := 0, 0
	for _, key := range keys {
		seed := seeds[key]
		t.Run(key, func(t *testing.T) {
			fail := func(trace Trace, err error) {
				path, saveErr := saveSeedFailureTrace(t.Name(), seed, trace)
				if saveErr != nil {
					t.Fatal(saveErr)
				}
				t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
			}
			generated, err := runGraphSignalRuntimeCombined(seed, nil)
			if err != nil {
				fail(generated, err)
			}
			if len(generated.Decisions) < len(dimensions) {
				fail(generated, fmt.Errorf("missing dimension choices"))
			}
			choices := make([]string, len(dimensions))
			for i, options := range dimensions {
				decision := generated.Decisions[i]
				if !reflect.DeepEqual(decision.Enabled, options) {
					fail(generated, fmt.Errorf("dimension %d domain changed: %v", i, decision.Enabled))
				}
				choices[i] = decision.Chosen
			}
			if strings.Join(choices, "/") != key {
				fail(generated, fmt.Errorf("seed did not execute expected combination %s", key))
			}
			replayed, err := runGraphSignalRuntimeCombined(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				fail(generated, fmt.Errorf("exact replay differs: %v", err))
			}
			if seed > 1000 {
				beyondFirstThousand++
				if dir := os.Getenv("SIM_GRAPH_SIGNAL_CARTESIAN_ROOT"); dir != "" {
					if err := generated.Save(filepath.Join(dir, fmt.Sprintf("seed-%d.json", seed))); err != nil {
						t.Fatal(err)
					}
				}
			}
			completed++
			t.Logf("SIGNAL_CARTESIAN seed=%d combination=%s exact_replay=true", seed, key)
		})
	}
	if completed != want {
		t.Fatalf("Cartesian runtime completion=%d/%d", completed, want)
	}
	t.Logf("SIGNAL_CARTESIAN_COMPLETE completed=%d requested=%d beyond_first_1000=%d", completed, want, beyondFirstThousand)
}
