package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/wf"
	"js-wf/worker"
)

// Result-cut preparation never executes parent SDK result steps. Cancellation
// releases its lease without publishing a parent terminal or acknowledging work.
func holdFanoutParent(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	<-c.Context().Done()
	return nil, c.Context().Err()
}

func prepareFanoutResultSignals(t *testing.T, ctx context.Context, js jetstream.JetStream, partition uint32, count int) {
	t.Helper()
	reached := make(chan struct{})
	var once sync.Once
	handlers := map[string]worker.Handler{"parent": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		// Worker signal draining precedes the handler. All children already
		// completed; this barrier admits their durable consumption before the
		// actual cut worker starts collecting SDK results.
		once.Do(func() { close(reached) })
		return holdFanoutParent(c, input)
	}}
	w, err := worker.New(ctx, js, "parent-signal-preparation", handlers)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	joined := false
	defer func() {
		stop()
		if !joined {
			<-done
		}
	}()
	go func() { done <- w.RunPartition(runCtx, partition) }()
	select {
	case <-reached:
	case err := <-done:
		joined = true
		t.Fatalf("signal preparation worker exited before barrier: %v", err)
	case <-ctx.Done():
		t.Fatalf("signal preparation barrier: %v", ctx.Err())
	}
	stop()
	err = <-done
	joined = true
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	records, _, err := journal.New(js).Read(ctx, "parent", "large-fanout")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{}
	consumed := map[string]bool{}
	pendingKind := ""
	for _, record := range records {
		switch record.Kind {
		case journal.StepRequested:
			var request struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			}
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				t.Fatal(err)
			}
			pendingKind = request.Kind
			if request.Kind == "call_async" {
				expected[request.Name] = true
			}
		case journal.StepCompleted:
			if pendingKind == "signal" {
				t.Fatal("parent collected an SDK result before result cut")
			}
			pendingKind = ""
		case journal.SignalConsumed:
			var signal struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(record.Payload, &signal); err != nil || signal.Name == "" || consumed[signal.Name] {
				t.Fatalf("invalid or duplicate prepared child signal: %s err=%v", signal.Name, err)
			}
			consumed[signal.Name] = true
		case journal.Completed, journal.Failed:
			t.Fatal("parent became terminal before result cut")
		}
	}
	if len(expected) != count || len(consumed) != count {
		t.Fatalf("prepared signals expected=%d consumed=%d want=%d", len(expected), len(consumed), count)
	}
	for name := range expected {
		if !consumed[name] {
			t.Fatalf("missing prepared child signal %s", name)
		}
	}
	t.Logf("prepared %d child signals before result cut without parent terminal", count)
}
