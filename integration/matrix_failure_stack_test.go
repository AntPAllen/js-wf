package integration_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Snapshot the parent before cleanup joins can hide or block the failure.
// Workers in the automatic-membership row run in this parent process. Other
// rows may have child processes; their stacks are not represented here.
func saveMatrixFailureStack(root string, failed bool) error {
	if !failed {
		return nil
	}
	return saveMatrixParentStack(root, "failure-goroutines", "Parent SDK process at failed-test boundary, before fixture cleanup; child process stacks excluded", time.Time{})
}

func saveMatrixParentStack(root, name, scope string, deadline time.Time) error {
	const maximum = 32 * 1024 * 1024
	buffer := make([]byte, 64*1024)
	var size int
	for {
		size = runtime.Stack(buffer, true)
		if size < len(buffer) || len(buffer) == maximum {
			break
		}
		buffer = make([]byte, min(len(buffer)*2, maximum))
	}
	observed := time.Now().UTC()
	if err := os.WriteFile(filepath.Join(root, name+".txt"), buffer[:size], 0600); err != nil {
		return err
	}
	meta := struct {
		Observed  time.Time `json:"observed"`
		PID       int       `json:"pid"`
		Bytes     int       `json:"bytes"`
		Truncated bool      `json:"truncated"`
		Scope     string    `json:"scope"`
		Deadline  time.Time `json:"deadline,omitempty"`
	}{observed, os.Getpid(), size, size == len(buffer), scope, deadline}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, name+".json"), data, 0600)
}

// An actual blocked goroutine must be represented; healthy fixtures must not
// emit failure evidence. This verifies diagnostic behavior, not native recovery.
func TestMatrixFailureStackRetainsBlockedParent(t *testing.T) {
	root := t.TempDir()
	if err := saveMatrixFailureStack(root, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "failure-goroutines.txt")); !os.IsNotExist(err) {
		t.Fatalf("healthy fixture emitted failure stack: %v", err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go matrixFailureStackBlockedParent(entered, release, done)
	<-entered
	defer func() { close(release); <-done }()
	if err := saveMatrixFailureStack(root, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "failure-goroutines.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("matrixFailureStackBlockedParent")) {
		t.Fatal("blocked parent goroutine absent from stack")
	}
	var meta struct {
		PID, Bytes int
		Truncated  bool
	}
	raw, err := os.ReadFile(filepath.Join(root, "failure-goroutines.json"))
	if err != nil || json.Unmarshal(raw, &meta) != nil || meta.PID != os.Getpid() || meta.Bytes != len(data) || meta.Truncated {
		t.Fatalf("incomplete diagnostic identity: %+v err=%v", meta, err)
	}
}

func matrixFailureStackBlockedParent(entered, release, done chan struct{}) {
	close(entered)
	<-release
	close(done)
}
