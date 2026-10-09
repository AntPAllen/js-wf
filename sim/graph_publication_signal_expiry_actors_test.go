package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Find the first seed for every declared fault combination cheaply, then run
// each through the actual clients, worker, scanner and collector with replay.
func TestGraphSignalExpiryCartesianReplay(t *testing.T) {
	seeds := map[string]int64{}
	for seed := int64(1); len(seeds) < 30 && seed <= 1000; seed++ {
		s := NewScheduler(seed)
		publication, e := s.Choose(graphSignalActorModes)
		if e != nil {
			t.Fatal(e)
		}
		clock, e := s.Choose([]string{"live", "expire_intents"})
		if e != nil {
			t.Fatal(e)
		}
		root, e := s.Choose([]string{"healthy", "drop_before_commit", "lose_ack_after_commit"})
		if e != nil {
			t.Fatal(e)
		}
		key := publication + "/" + clock + "/" + root
		if _, ok := seeds[key]; !ok {
			seeds[key] = seed
		}
	}
	if len(seeds) != 30 {
		t.Fatalf("only %d/30 first-seed combinations found", len(seeds))
	}
	for _, publication := range graphSignalActorModes {
		for _, clock := range []string{"live", "expire_intents"} {
			for _, root := range []string{"healthy", "drop_before_commit", "lose_ack_after_commit"} {
				key := publication + "/" + clock + "/" + root
				seed := seeds[key]
				t.Run(key, func(t *testing.T) {
					generated, e := runGraphSignalExpiryActors(seed, nil)
					if e != nil {
						path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
						t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, e)
					}
					if got := generated.Decisions[0].Chosen + "/" + generated.Decisions[1].Chosen + "/" + generated.Decisions[2].Chosen; got != key {
						t.Fatalf("declared/actual combination differs: %s/%s", key, got)
					}
					replayed, e := runGraphSignalExpiryActors(seed, &generated)
					if e != nil || !reflect.DeepEqual(generated, replayed) {
						path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
						t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, fmt.Errorf("replay differs: %w", e))
					}
				})
			}
		}
	}
}

func TestSeededGraphSignalExpiryActorsReplay(t *testing.T) {
	observed := map[string]int{}
	captured := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, e := runGraphSignalExpiryActors(seed, nil)
		if e != nil {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, e)
		}
		replayed, e := runGraphSignalExpiryActors(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s replay: %v", seed, path, e)
		}
		key := generated.Decisions[0].Chosen + "/" + generated.Decisions[1].Chosen + "/" + generated.Decisions[2].Chosen
		observed[key]++
		clock := generated.Decisions[1].Chosen
		cut := false
		for _, event := range generated.Transport {
			if event.Operation == "graph_publication_intent_expiry_cut" {
				cut = true
			}
		}
		if cut != (clock == "expire_intents") {
			t.Fatalf("seed %d expiry cut=%v choice=%s", seed, cut, clock)
		}
		capture := clock + "-" + generated.Decisions[2].Chosen
		if !captured[capture] {
			captured[capture] = true
			if dir := os.Getenv("SIM_GRAPH_SIGNAL_EXPIRY_ACTORS_ROOT"); dir != "" {
				if e = generated.Save(filepath.Join(dir, capture+".json")); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	if len(observed) != 30 || len(captured) != 6 {
		t.Fatalf("combined coverage=%d captures=%d", len(observed), len(captured))
	}
	t.Logf("30 publication/intent-expiry/root-reply combinations: %v; actual uncommitted intent expiry cuts, all faults consumed, exact replay and 30s recovery", observed)
}
