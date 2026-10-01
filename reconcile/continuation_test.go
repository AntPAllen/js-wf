package reconcile

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type continuationScanPort struct {
	SuspendedScanPort
	records []journal.Record
}

func (p continuationScanPort) ReadJournal(context.Context, string, string) ([]journal.Record, error) {
	return p.records, nil
}

func TestContinuationRepairBeforeAndAfterSuspension(t *testing.T) {
	input := &jetstream.RawStreamMsg{Subject: "wf.inv.test.continuation", Sequence: 17}
	pair := []journal.Record{
		{Entry: journal.Entry{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"checkpoint","name":"next_v1"}`)}, Sequence: 31},
		{Entry: journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result_ref":"step-result-frame"}`)}, Sequence: 32},
	}
	for _, suspended := range []bool{false, true} {
		records := append([]journal.Record(nil), pair...)
		if suspended {
			records = append(records, journal.Record{Entry: journal.Entry{Kind: journal.Suspended, Payload: json.RawMessage(`{"waiting_on":"continuation:next_v1"}`)}, Sequence: 33})
		}
		candidate, ready, err := NewSuspendedScanWithPort(continuationScanPort{records: records}).inspect(context.Background(), input)
		if err != nil || !ready || candidate.Reason != "continuation" || candidate.JournalSeq != records[len(records)-1].Sequence {
			t.Fatalf("suspended=%t candidate=%+v ready=%t err=%v", suspended, candidate, ready, err)
		}
		records = append(records, journal.Record{Entry: journal.Entry{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"run","name":"next"}`)}, Sequence: 34})
		if _, ready, err := NewSuspendedScanWithPort(continuationScanPort{records: records}).inspect(context.Background(), input); err != nil || ready {
			t.Fatalf("advanced journal re-enqueued: %t %v", ready, err)
		}
	}
}
