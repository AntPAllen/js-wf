//go:build linux

package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type traceWatchControl struct {
	jetstream.KeyWatcher
	updates chan jetstream.KeyValueEntry
	stops   int
}

func (w *traceWatchControl) Updates() <-chan jetstream.KeyValueEntry { return w.updates }
func (w *traceWatchControl) Stop() error                             { w.stops++; return auditTraceSentinel }

type traceWatchKVControl struct {
	jetstream.KeyValue
	expected context.Context
	watch    jetstream.KeyWatcher
	err      error
}

func (s traceWatchKVControl) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	if ctx != s.expected || len(opts) != 1 || opts[0] != nil {
		return nil, errors.New("watch context or options changed")
	}
	return s.watch, s.err
}

func TestRetainedAuditTraceWatchPreservesChannelOptionsErrorsAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	trace := &retainedAuditTrace{}
	control := &traceWatchControl{updates: make(chan jetstream.KeyValueEntry, 1)}
	kv := tracedAuditKV{KeyValue: traceWatchKVControl{expected: ctx, watch: control}, name: "WF_STATE", trace: trace}
	watch, err := kv.WatchAll(ctx, nil)
	if err != nil || watch.Updates() != control.updates {
		t.Fatalf("watch delegation changed: %v %v", watch, err)
	}
	// The nil initial-set barrier remains on the original channel. The tracer
	// neither consumes it nor forwards updates through another goroutine.
	control.updates <- nil
	if len(control.updates) != 1 || <-watch.Updates() != nil {
		t.Fatal("initial barrier changed")
	}
	for i := 0; i < 2; i++ {
		if watch.Stop() != auditTraceSentinel {
			t.Fatal("stop error identity changed")
		}
	}
	if control.stops != 2 {
		t.Fatal("stop calls changed")
	}
	kv.KeyValue = traceWatchKVControl{expected: ctx, watch: control, err: auditTraceSentinel}
	if got, err := kv.WatchAll(ctx, nil); got != control || err != auditTraceSentinel {
		t.Fatalf("failed constructor result changed: %v %v", got, err)
	}
	snapshot := trace.snapshot()
	if c := snapshot.Counts["WF_STATE.WatchAll"]; c.Started != 2 || c.Completed != 2 || c.Errors != 1 {
		t.Fatalf("creation count=%+v", c)
	}
	if c := snapshot.Counts["WF_STATE.WatchStop"]; c.Started != 2 || c.Completed != 2 || c.Errors != 2 {
		t.Fatalf("cleanup count=%+v", c)
	}
	for _, call := range snapshot.Recent {
		if call.Operation == "WF_STATE.WatchStop" && !call.Deadline.IsZero() {
			t.Fatal("invented cleanup deadline")
		}
	}
}
