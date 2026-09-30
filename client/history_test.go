package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestObservedSignalStatus(t *testing.T) {
	for _, test := range []struct {
		err       error
		attempted bool
		want      string
	}{
		{context.DeadlineExceeded, false, "not_published"},
		{context.DeadlineExceeded, true, "error"},
		{ErrSignalUnknown, true, "unknown"},
		{ErrEnqueueUnknown, true, "enqueue_unknown"},
		{ErrNotFound, false, "not_found"},
		{nil, true, "signaled"},
	} {
		if got := observedSignalStatus(test.err, test.attempted); got != test.want {
			t.Fatalf("err=%v attempted=%v status=%s want=%s", test.err, test.attempted, got, test.want)
		}
	}
}

type deadlineSignalPort struct{ SignalPort }

func (deadlineSignalPort) LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error) {
	return nil, context.DeadlineExceeded
}

type signalOperationRecorder struct{ operations []Operation }

func (r *signalOperationRecorder) Record(op Operation) { r.operations = append(r.operations, op) }

func TestSignalRecordsPrePublicationDeadline(t *testing.T) {
	recorder := &signalOperationRecorder{}
	c := &Client{signalPort: deadlineSignalPort{}, observer: recorder}
	if _, err := c.Signal(context.Background(), "test", "same", "go", nil, "key"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("signal error=%v", err)
	}
	if len(recorder.operations) != 1 {
		t.Fatalf("operations=%d", len(recorder.operations))
	}
	var output struct {
		Status   string `json:"status"`
		Sequence uint64 `json:"signal_seq"`
	}
	if err := json.Unmarshal(recorder.operations[0].Result, &output); err != nil {
		t.Fatal(err)
	}
	if output.Status != "not_published" || output.Sequence != 0 {
		t.Fatalf("output=%+v", output)
	}
}
