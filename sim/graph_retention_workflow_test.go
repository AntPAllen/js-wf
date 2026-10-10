package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var graphRetentionWorkflowCuts = []string{"cut0", "cut1", "cut2", "cut3", "cut3-reuse", "cut4", "cut4-reuse"}

func runGraphRetentionWorkflow(seed int64, replay *Trace) (Trace, error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_retention_workflow"); err != nil {
		return s.Trace(), err
	}
	mode, err := s.Choose(graphRetentionWorkflowCuts)
	if err != nil {
		return s.Trace(), err
	}
	cut, reuse := 0, false
	switch mode {
	case "cut1":
		cut = 1
	case "cut2":
		cut = 2
	case "cut3", "cut3-reuse":
		cut, reuse = 3, mode == "cut3-reuse"
	case "cut4", "cut4-reuse":
		cut, reuse = 4, mode == "cut4-reuse"
	}
	return runGraphRetentionWorkflowSDKCut(s, cut, reuse)
}

func TestSeededGraphRetentionWorkflowReplay(t *testing.T) {
	observed := map[string]bool{}
	fail := func(trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			directory, err := os.MkdirTemp("", "js-wf-retention-sdk-failure-")
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(directory, "trace.json")
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", trace.Seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphRetentionWorkflow(seed, nil)
		if err != nil {
			fail(generated, err)
		}
		replayed, err := runGraphRetentionWorkflow(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(generated, fmt.Errorf("SDK retention exact replay differs: %v", err))
		}
		mode := generated.Decisions[0].Chosen
		if directory := os.Getenv("SIM_GRAPH_RETENTION_WORKFLOW_ROOT"); directory != "" && !observed[mode] {
			if err := generated.Save(filepath.Join(directory, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
		observed[mode] = true
	}
	if len(observed) != len(graphRetentionWorkflowCuts) {
		t.Fatalf("retention SDK cut coverage incomplete: %v", observed)
	}
	t.Logf("retention SDK crash cuts and generation-bound reuse covered: %v", observed)
}
