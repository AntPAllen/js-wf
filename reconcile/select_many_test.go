package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type selectSignalPort struct {
	SuspendedScanPort
	err error
}

func (p selectSignalPort) GetSignalAfter(context.Context, string, uint64) (*jetstream.RawStreamMsg, error) {
	return nil, p.err
}

func TestMultiSelectReadFailureDoesNotSuppressDueTimer(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	payload, _ := json.Marshal(map[string]any{"kind": "select_many", "cases": []map[string]any{{"kind": "signal", "name": "go"}, {"kind": "timer", "name": "deadline", "fire_at": now.Add(-time.Second)}}})
	records := []journal.Record{{Entry: journal.Entry{Kind: journal.StepRequested, Payload: payload}}}
	lost := errors.New("signal read unavailable")
	scanner := NewSuspendedScanWithPort(selectSignalPort{err: lost})
	scanner.Now = func() time.Time { return now }
	scanner.Grace = 0
	if ready, err := scanner.selectReady(context.Background(), "test", "one", 1, records); !ready || err != nil {
		t.Fatalf("due timer suppressed: ready=%t err=%v", ready, err)
	}
	scanner.Now = func() time.Time { return now.Add(-time.Minute) }
	if ready, err := scanner.selectReady(context.Background(), "test", "one", 1, records); ready || !errors.Is(err, lost) {
		t.Fatalf("unready read failure concealed: %t %v", ready, err)
	}
	records[0].Payload = json.RawMessage(`{"kind":"select_many","cases":[{"kind":"unknown","name":"go"}]}`)
	if _, err := scanner.selectReady(context.Background(), "test", "one", 1, records); err == nil {
		t.Fatal("invalid case accepted")
	}
}
