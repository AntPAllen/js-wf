package sim

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A suite can fail more than once. Keep every failing schedule rather than
// replacing the first trace with the next test's failure. FAULT_TRACE_OUT is a
// filename prefix; diagnostics report the actual replayable filename.
func saveSeedFailureTrace(test string, seed int64, trace Trace) (string, error) {
	path := os.Getenv("FAULT_TRACE_OUT")
	if path == "" {
		dir, err := os.MkdirTemp("", "js-wf-seed-failure-")
		if err != nil {
			return "", err
		}
		path = filepath.Join(dir, "trace.json")
	}
	name := strings.NewReplacer("/", "_", "\\", "_").Replace(test)
	stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	f, err := os.CreateTemp(filepath.Dir(path), fmt.Sprintf("%s.%s.seed-%d-*.json", stem, name, seed))
	if err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	return f.Name(), trace.Save(f.Name())
}

func TestSeedFailureTracesDoNotOverwrite(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "failure-trace.json")
	t.Setenv("FAULT_TRACE_OUT", prefix)
	paths := map[string]bool{}
	for i, name := range []string{"Signal/combined", "Signal/runtime", "Signal/runtime"} {
		trace := Trace{Version: TraceVersion, Seed: int64(i + 1), Workload: "failure_capture_control", StepLimit: DefaultMaxSteps}
		path, err := saveSeedFailureTrace(name, 971, trace)
		if err != nil {
			t.Fatal(err)
		}
		if paths[path] || filepath.Dir(path) != filepath.Dir(prefix) {
			t.Fatalf("trace collision or escaped directory: %s", path)
		}
		paths[path] = true
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var saved Trace
		if err = json.Unmarshal(data, &saved); err != nil || !reflect.DeepEqual(trace, saved) {
			t.Fatalf("saved trace differs: %v", err)
		}
	}
	if _, err := os.Stat(prefix); !os.IsNotExist(err) {
		t.Fatalf("shared failure filename was overwritten: %v", err)
	}
}
