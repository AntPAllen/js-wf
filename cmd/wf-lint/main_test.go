package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandReportsAndFailsOnWorkflowClockCall(t *testing.T) {
	file := filepath.Join(t.TempDir(), "workflow.go")
	source := `package sample
import ("time"; "js-wf/wf")
func workflow(c *wf.Context) { time.Now() }
`
	if err := os.WriteFile(file, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := run([]string{file}, &output)
	if err == nil || !strings.Contains(err.Error(), "1 nondeterministic") || !strings.Contains(output.String(), file+":3:") || !strings.Contains(output.String(), "wf.Now") {
		t.Fatalf("lint output=%q err=%v", output.String(), err)
	}
}
