package sim

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSeededGraphTerminalAuditReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runGraphTerminalAudit(seed, nil)
		if err != nil {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		replayed, err := runGraphTerminalAudit(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s replay: %v", seed, path, err)
		}
		mode := generated.Decisions[0].Chosen
		if observed[mode] == 0 {
			if dir := os.Getenv("SIM_GRAPH_TERMINAL_AUDIT_ROOT"); dir != "" {
				if err := generated.Save(filepath.Join(dir, mode+".json")); err != nil {
					t.Fatal(err)
				}
			}
		}
		observed[mode]++
	}
	if len(observed) != len(terminalAuditModes) {
		t.Fatal("incomplete audit mode coverage", observed)
	}
	t.Logf("terminal projection audit modes=%v; sole scanner dispatch, exact canonical repair, live purge suppression, no handlers/foreign lease changes, repeated corruption recovery and graph drain", observed)
}

func TestGraphTerminalAuditLeaseFreeReplay(t *testing.T) {
	seed := int64(0)
	for candidate := int64(1); candidate <= 1000; candidate++ {
		s := NewScheduler(candidate)
		if err := s.SetWorkload("graph_terminal_projection_audit"); err != nil {
			t.Fatal(err)
		}
		mode, err := s.Choose(terminalAuditModes)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "lease_free_corrupt" {
			seed = candidate
			break
		}
	}
	if seed == 0 {
		t.Fatal("lease-free mode not found")
	}
	generated, err := runGraphTerminalAudit(seed, nil)
	if err != nil {
		path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
	}
	replayed, err := runGraphTerminalAudit(seed, &generated)
	if err != nil || !reflect.DeepEqual(generated, replayed) {
		t.Fatal("lease-free audit replay diverged", err)
	}
	t.Logf("lease-free canonical projection recovery seed=%d", seed)
}
