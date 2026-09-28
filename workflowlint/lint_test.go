package workflowlint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScanWorkflowCallsAndShadows(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "workflow.go")
	source := `package sample
import (
    "context"
    clock "time"
    mr "math/rand"
    cr "crypto/rand"
    flow "js-wf/wf"
)
func ordinary() { clock.Now() }
func workflow(c *flow.Context) {
    clock.Now()
    mr.Intn(3)
    cr.Read(nil)
    flow.Run(c, "effect", 1, func(context.Context) (int, error) { clock.Now(); mr.Intn(3); return 1, nil })
    flow.RunOnce(c, "effect", 1, func(context.Context, string) (int, error) { cr.Read(nil); return 1, nil })
    clock := struct{ Now func() }{}
    clock.Now()
}
var another = func(c *flow.Context) { clock.Now() }
`
	if err := os.WriteFile(file, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	findings, err := Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 4 {
		t.Fatalf("findings=%+v", findings)
	}
	for index, call := range []string{"clock.Now", "mr.Intn", "cr.Read", "clock.Now"} {
		if findings[index].Call != call || findings[index].Line == 0 || findings[index].Column == 0 || findings[index].File != file {
			t.Fatalf("finding %d=%+v want %s", index, findings[index], call)
		}
	}
	if !strings.Contains(findings[0].Hint, "wf.Now") || !strings.Contains(findings[1].Hint, "wf.Random") {
		t.Fatalf("remediation hints=%+v", findings)
	}
}

func TestScanSafeWorkflowAndInvalidInput(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "safe.go")
	source := `package sample
import (
    "time"
    "js-wf/wf"
)
func ordinary() { time.Now() }
func workflow(c *wf.Context) { wf.Now(c) }
`
	if err := os.WriteFile(file, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	findings, err := Scan(file, dir)
	if err != nil || len(findings) != 0 {
		t.Fatalf("safe findings=%+v err=%v", findings, err)
	}
	if _, err := Scan(filepath.Join(dir, "missing.go")); err == nil {
		t.Fatal("missing input was silently ignored")
	}
}
