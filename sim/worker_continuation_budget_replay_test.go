package sim

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestSeededWorkerContinuationBudgetReplay(t *testing.T) {
	if os.Getenv("SIM_CONTINUATION_BUDGET_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerContinuationBudget(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CONTINUATION_BUDGET_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerContinuationBudget(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-worker-continuation-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-worker-continuation.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runSeededWorkerContinuationBudget(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d worker continuation replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 8 {
		t.Fatalf("covered %d/8 worker continuation modes", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-continuation-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerContinuationBudgetReplay$")
		cmd.Env = append(os.Environ(), "SIM_CONTINUATION_BUDGET_HELPER=1", "SIM_CONTINUATION_BUDGET_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("worker continuation trace changed across processes")
	}
}
