package journal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type noResponderBatchPort struct {
	opened  int
	closed  int
	fetches int
	data    []byte
}

func (p *noResponderBatchPort) Open(_ context.Context, _ string, _ uint64) (BatchReadCursor, error) {
	p.opened++
	return p, nil
}

func (p *noResponderBatchPort) Probe(_ context.Context, _ string, _ uint64) (bool, error) {
	return false, nil
}

func (p *noResponderBatchPort) Wait(context.Context, time.Duration) error { return nil }

func (p *noResponderBatchPort) Fetch(_ context.Context, _ int) ([]AppendTail, error) {
	p.fetches++
	if p.fetches == 1 {
		return nil, nats.ErrNoResponders
	}
	return []AppendTail{{Sequence: 2, Data: p.data}}, nil
}

func (p *noResponderBatchPort) Close(context.Context) error {
	p.closed++
	return nil
}

func TestBatchReadRetriesNoResponderWithoutSkippingEntry(t *testing.T) {
	encoded, err := json.Marshal(Entry{Epoch: 1, Index: 1, Kind: StepRequested})
	if err != nil {
		t.Fatal(err)
	}
	port := &noResponderBatchPort{data: encoded}
	first := []Record{{Entry: Entry{Epoch: 1, Index: 0, Kind: Started}, Sequence: 1}}
	records, tail, received, err := readLiveBatch(context.Background(), port, "wf.jrn.test.retry", 2, first, 1)
	if err != nil || !received || len(records) != 2 || records[1].Index != 1 || tail != 2 {
		t.Fatalf("read after no responder: records=%+v tail=%d received=%t err=%v", records, tail, received, err)
	}
	if port.fetches != 2 || port.opened != 1 || port.closed != 1 {
		t.Fatalf("consumer lifecycle: fetches=%d opened=%d closed=%d", port.fetches, port.opened, port.closed)
	}
}
