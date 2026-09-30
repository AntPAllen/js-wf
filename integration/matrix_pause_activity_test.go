//go:build linux

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"js-wf/identity"
	"js-wf/worker"
)

func TestMatrixPauseActivityUsesCompletedDispatchRecords(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "worker")
	// Reproduce seed 7: release is recorded before SIGSTOP, but the active
	// marker still says one. A partial next event is not evidence of acquisition.
	if err := os.WriteFile(base+"-active.json", []byte(`{"active_leases":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(base + "-dispatch.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(f)
	for _, event := range []worker.DispatchEvent{
		{Type: "matrixsignal", ID: "released", Stage: "lease_acquired"},
		{Type: "matrixshort", ID: "running", Stage: "lease_acquired"},
		{Type: "matrixsignal", ID: "released", Stage: "released"},
	} {
		if err := encoder.Encode(event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.WriteString(`{"Stage":"lease_acquired"`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	keys, err := matrixWorkerActiveLeaseKeys(base + "-dispatch.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{identity.Key("matrixshort", "running")}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("active leases=%v want=%v", keys, want)
	}
	// Corrupt completed records must fail rather than manufacture idle evidence.
	if err := os.WriteFile(base+"-dispatch.jsonl", []byte("{bad}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := matrixWorkerActiveLeaseKeys(base + "-dispatch.jsonl"); err == nil {
		t.Fatal("accepted malformed completed dispatch record")
	}
}
