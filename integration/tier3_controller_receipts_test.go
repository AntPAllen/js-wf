//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type matrixControllerJournalReceipt struct {
	Sequence   uint64        `json:"sequence"`
	Subject    string        `json:"subject"`
	Entry      journal.Entry `json:"entry"`
	ObservedAt time.Time     `json:"observed_at"`
}

type matrixControllerReceiptObserver struct {
	mu       sync.Mutex
	receipts map[uint64]matrixControllerJournalReceipt
	invalid  error
	consume  jetstream.ConsumeContext
}

func startMatrixControllerReceiptObserver(ctx context.Context, js jetstream.JetStream) (*matrixControllerReceiptObserver, error) {
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return nil, err
	}
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{"wf.jrn.>"}, DeliverPolicy: jetstream.DeliverAllPolicy})
	if err != nil {
		return nil, err
	}
	observer := &matrixControllerReceiptObserver{receipts: make(map[uint64]matrixControllerJournalReceipt)}
	observer.consume, err = consumer.Consume(func(message jetstream.Msg) {
		at := time.Now().UTC()
		metadata, err := message.Metadata()
		var entry journal.Entry
		if err == nil {
			err = json.Unmarshal(message.Data(), &entry)
		}
		observer.mu.Lock()
		defer observer.mu.Unlock()
		if err != nil {
			observer.invalid = fmt.Errorf("controller journal receipt decode: %w", err)
			return
		}
		if metadata.Stream != "WF_JRN" || metadata.Sequence.Stream == 0 {
			observer.invalid = fmt.Errorf("controller receipt has invalid stream/sequence")
			return
		}
		receipt := matrixControllerJournalReceipt{metadata.Sequence.Stream, message.Subject(), entry, at}
		if previous, exists := observer.receipts[receipt.Sequence]; exists {
			if previous.Subject != receipt.Subject || !reflect.DeepEqual(previous.Entry, receipt.Entry) {
				observer.invalid = fmt.Errorf("controller receipt changed at sequence%d", receipt.Sequence)
			}
			return // Preserve the first actual receipt across ordered-consumer resets.
		}
		observer.receipts[receipt.Sequence] = receipt
	})
	if err != nil {
		return nil, err
	}
	return observer, nil
}

func (observer *matrixControllerReceiptObserver) snapshot() ([]matrixControllerJournalReceipt, error) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	records := make([]matrixControllerJournalReceipt, 0, len(observer.receipts))
	for _, receipt := range observer.receipts {
		records = append(records, receipt)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	return records, observer.invalid
}

// Validate receipt identity against the final retained journal before its
// unshifted read time can bound an unknown append. A sequence alone is not enough.
func matrixControllerReceiptTimes(typ, id string, records []journal.Record, receipts []matrixControllerJournalReceipt) (map[uint64]time.Time, error) {
	found := make(map[uint64]matrixControllerJournalReceipt)
	for _, receipt := range receipts {
		if receipt.Subject != identity.JournalSubject(typ, id) {
			continue
		}
		if receipt.ObservedAt.IsZero() {
			return nil, fmt.Errorf("missing controller receipt time")
		}
		if _, exists := found[receipt.Sequence]; exists {
			return nil, fmt.Errorf("duplicate controller receipt seq%d", receipt.Sequence)
		}
		found[receipt.Sequence] = receipt
	}
	times := make(map[uint64]time.Time)
	for _, record := range records {
		receipt, exists := found[record.Sequence]
		if !exists {
			continue
		} // A known successful append can independently bound it.
		if !reflect.DeepEqual(receipt.Entry, record.Entry) {
			return nil, fmt.Errorf("controller receipt differs from retained journal seq%d", record.Sequence)
		}
		times[record.Sequence] = receipt.ObservedAt
	}
	return times, nil
}
