package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"js-wf/worker"
)

// Opt-in observations retain the original cut deadline and dispatch decisions.
func fanoutProcessTrace(t *testing.T, marker string) []worker.Option {
	t.Helper()
	if os.Getenv("WF_FANOUT_DIAGNOSTIC_TRACE") != "1" {
		return nil
	}
	root := filepath.Dir(marker)
	file, err := os.OpenFile(filepath.Join(root, "worker-trace.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	encoder := json.NewEncoder(file)
	write := func(kind string, event any) {
		mu.Lock()
		defer mu.Unlock()
		if err := encoder.Encode(map[string]any{"kind": kind, "event": event}); err != nil {
			// A diagnostic write failure must remain visible in the child log.
			_, _ = os.Stderr.WriteString("fanout trace write: " + err.Error() + "\n")
		}
	}
	write("trace_started", map[string]any{"at": time.Now().UTC(), "pid": os.Getpid(), "predeadline_delay": "29s"})
	timer := time.AfterFunc(29*time.Second, func() {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for n == len(buf) && len(buf) < 16<<20 {
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
		if err := os.WriteFile(filepath.Join(root, "worker-predeadline-stack.txt"), buf[:n], 0600); err != nil {
			write("stack_error", map[string]any{"at": time.Now().UTC(), "error": err.Error()})
			return
		}
		write("stack_captured", map[string]any{"at": time.Now().UTC(), "bytes": n, "complete": n < len(buf)})
	})
	t.Cleanup(func() {
		timer.Stop()
		mu.Lock()
		defer mu.Unlock()
		_ = file.Close()
	})
	return []worker.Option{
		worker.WithDispatchObserver(func(event worker.DispatchEvent) { write("dispatch", event) }),
		worker.WithOperationObserver(func(event worker.OperationEvent) { write("operation", event) }),
	}
}
