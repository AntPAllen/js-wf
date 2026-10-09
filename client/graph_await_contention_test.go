package client_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"js-wf/sim"
)

func runAwaitContention(seed int64, version int, mode string, replay *sim.Trace) (sim.Trace, error) {
	schedule := sim.NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = sim.ReplayScheduler(*replay)
		if err != nil {
			return sim.Trace{}, err
		}
	}
	if err := schedule.SetWorkload(fmt.Sprintf("graph_await_contention_v%d_%s", version, mode)); err != nil {
		return sim.Trace{}, err
	}
	return sim.RunGraphAwaitContentionSchedule(schedule, version, mode)
}

func TestGraphAwaitContentionFreshReadAndGenerationFence(t *testing.T) {
	for _, version := range []int{4, 6} {
		for _, mode := range []string{"observe", "pinned", "release", "generation", "purge", "cancel", "unknown"} {
			t.Run(fmt.Sprintf("v%d/%s", version, mode), func(t *testing.T) {
				for seed := int64(1); seed <= 16; seed++ {
					generated, err := runAwaitContention(seed, version, mode, nil)
					if err != nil {
						if directory := os.Getenv("GRAPH_AWAIT_CONTENTION_EVIDENCE"); directory != "" {
							if saveErr := generated.Save(filepath.Join(directory, fmt.Sprintf("v%d-%s-seed%d.json", version, mode, seed))); saveErr != nil {
								t.Fatal(saveErr)
							}
						}
						t.Fatalf("seed=%d %v", seed, err)
					}
					replayed, err := runAwaitContention(seed, version, mode, &generated)
					if err != nil || !reflect.DeepEqual(generated, replayed) {
						t.Fatalf("seed=%d exact replay differs: %v", seed, err)
					}
				}
			})
		}
	}
}

// The real context timer is exercised once; the seed matrix stays entirely fast.
func TestGraphAwaitOwnReadDeadline(t *testing.T) {
	for _, mode := range []string{"deadline", "deadline-corrupt"} {
		t.Run(mode, func(t *testing.T) {
			_, err := runAwaitContention(1, 6, mode, nil)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
