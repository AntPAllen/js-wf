package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSeededGraphTerminalProjectionReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-terminal-worker-failure-")
			if e != nil {
				t.Fatal(e)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, e := runGraphTerminalProjection(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphTerminalProjection(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("terminal worker replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_TERMINAL_PROJECTION_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphTerminalProjectionModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph terminal projection repair: modes=%v; canonical ACK/NAK, no handlers/effects, unchanged foreign lease, graph parent bytes and graph drain", observed)
}
