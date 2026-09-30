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

func TestSeededPausedCoordinatorClaimReplay(t *testing.T) {
	if os.Getenv("SIM_COORDINATOR_CLAIM_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runPausedCoordinatorClaim(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_COORDINATOR_CLAIM_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runPausedCoordinatorClaim(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-coordinator-claim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-coordinator-claim.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen+"/"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runPausedCoordinatorClaim(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d coordinator claim replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 6 {
		t.Fatalf("missing coordinator cut modes: %v", modes)
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("coordinator-claim-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededPausedCoordinatorClaimReplay$")
		cmd.Env = append(os.Environ(), "SIM_COORDINATOR_CLAIM_HELPER=1", "SIM_COORDINATOR_CLAIM_OUT="+files[i], "FAULT_SEED=42")
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
		t.Fatal("membership trace changed across processes")
	}
}
