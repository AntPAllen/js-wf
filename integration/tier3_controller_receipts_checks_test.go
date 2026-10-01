//go:build linux

package integration_test

import (
	"context"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/worker"
)

func TestMatrixControllerJournalReceiptBoundsActualRetainedAppend(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	observer, err := startMatrixControllerReceiptObserver(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	defer observer.consume.Stop()
	entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: "controller-receipt-owner"}
	before := time.Now()
	sequence, err := journal.New(all[0]).Append(ctx, "controller-receipt", "one", entry, 0)
	if err != nil {
		t.Fatal(err)
	}
	var receipts []matrixControllerJournalReceipt
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		receipts, err = observer.snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if len(receipts) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	records := []journal.Record{{Entry: entry, Sequence: sequence}}
	times, err := matrixControllerReceiptTimes("controller-receipt", "one", records, receipts)
	if err != nil || times[sequence].IsZero() || times[sequence].Before(before) {
		t.Fatalf("receipt times=%v err=%v", times, err)
	}
	observation := worker.OperationEvent{Type: "controller-receipt", ID: "one", Worker: entry.WorkerID, Operation: "journal_append", JournalIndex: entry.Index, JournalKind: entry.Kind, At: time.Now(), Error: journal.ErrUnknown.Error()}
	observation.Duration = observation.At.Sub(before)
	bounds, err := matrixControllerAppendBounds(observation.Type, observation.ID, records, []worker.OperationEvent{observation}, times)
	if err != nil || !bounds[0].After.Equal(times[sequence]) {
		t.Fatalf("unknown append receipt bounds=%+v err=%v", bounds, err)
	}
}

func TestMatrixControllerReceiptRejectsChangedEntryAndDuplicate(t *testing.T) {
	entry := journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started, WorkerID: "owner"}
	record := journal.Record{Entry: entry, Sequence: 7}
	receipt := matrixControllerJournalReceipt{Sequence: 7, Subject: identity.JournalSubject("typ", "id"), Entry: entry, ObservedAt: time.Unix(100, 0)}
	for _, mode := range []string{"changed_epoch", "changed_payload", "duplicate", "missing_time"} {
		t.Run(mode, func(t *testing.T) {
			changed := receipt
			receipts := []matrixControllerJournalReceipt{changed}
			switch mode {
			case "changed_epoch":
				receipts[0].Entry.Epoch = 2
			case "changed_payload":
				receipts[0].Entry.Payload = []byte(`1`)
			case "duplicate":
				receipts = append(receipts, receipt)
			case "missing_time":
				receipts[0].ObservedAt = time.Time{}
			}
			if _, err := matrixControllerReceiptTimes("typ", "id", []journal.Record{record}, receipts); err == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}
