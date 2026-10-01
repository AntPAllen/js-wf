//go:build linux

package integration_test

import (
	"fmt"
	"strings"
	"time"

	"js-wf/journal"
	"js-wf/worker"
)

// An append can commit anywhere inside this interval. It is never treated as
// an exact commit timestamp. Failed contenders can widen the lower bound, but
// cannot make the measured latency smaller.
type matrixControllerAppendBound struct {
	Sequence uint64        `json:"sequence"`
	Entry    journal.Entry `json:"entry"`
	Type     string        `json:"type"`
	ID       string        `json:"id"`
	Worker   string        `json:"worker"`
	Index    uint64        `json:"index"`
	Kind     journal.Kind  `json:"kind"`
	Before   time.Time     `json:"before"`
	After    time.Time     `json:"after"`
	Attempts int           `json:"attempts"`
}

func matrixControllerAppendBounds(typ, id string, records []journal.Record, operations []worker.OperationEvent, receipts map[uint64]time.Time) ([]matrixControllerAppendBound, error) {
	bounds := make([]matrixControllerAppendBound, 0, len(records))
	for _, record := range records {
		bound := matrixControllerAppendBound{Sequence: record.Sequence, Entry: record.Entry, Type: typ, ID: id, Worker: record.WorkerID, Index: record.Index, Kind: record.Kind}
		for _, event := range operations {
			if event.Operation != "journal_append" || event.Type != typ || event.ID != id || event.Worker != record.WorkerID || event.JournalIndex != record.Index || event.JournalKind != record.Kind {
				continue
			}
			// Successful or unknown attempts can supply a lower bound. A timeout
			// does not supply an upper bound: the server may commit after the
			// caller returns. Only a success or independent read receipt does.
			if event.Error != "" && !strings.Contains(event.Error, journal.ErrUnknown.Error()) {
				continue
			}
			if event.At.IsZero() || event.Duration < 0 {
				return nil, fmt.Errorf("invalid controller append observation for %s/%s index%d", typ, id, record.Index)
			}
			before := event.At.Add(-event.Duration)
			if bound.Before.IsZero() || before.Before(bound.Before) {
				bound.Before = before
			}
			if event.Error == "" && (bound.After.IsZero() || event.At.Before(bound.After)) {
				bound.After = event.At
			}
			bound.Attempts++
		}
		if receipt := receipts[record.Sequence]; !receipt.IsZero() && (bound.After.IsZero() || receipt.Before(bound.After)) {
			bound.After = receipt
		}
		if bound.Attempts == 0 || bound.After.IsZero() || bound.After.Before(bound.Before) {
			return nil, fmt.Errorf("missing controller append window for %s/%s index%d seq%d", typ, id, record.Index, record.Sequence)
		}
		bounds = append(bounds, bound)
	}
	return bounds, nil
}

// Upper-delay samples subtract an enabling event's lower bound from the next
// causally subsequent append's upper bound. Sequence ordering chooses progress;
// comparing receipt times would incorrectly reorder concurrent observations.
func matrixControllerProgress(typ, id, event string, enabled time.Time, afterSequence uint64, bounds []matrixControllerAppendBound) (matrixLatencySample, error) {
	if enabled.IsZero() {
		return matrixLatencySample{}, fmt.Errorf("missing controller enabling observation for %s/%s %s", typ, id, event)
	}
	for _, bound := range bounds {
		if bound.Sequence <= afterSequence {
			continue
		}
		if bound.After.Before(enabled) {
			return matrixLatencySample{}, fmt.Errorf("controller progress precedes enabling observation for %s/%s %s", typ, id, event)
		}
		return matrixLatencySample{Type: typ, ID: id, Event: event, Enabled: enabled, Observed: bound.After, ObservedLower: &bound.Before, Delay: bound.After.UTC().Sub(enabled.UTC())}, nil
	}
	return matrixLatencySample{}, fmt.Errorf("missing causal controller progress for %s/%s %s after seq%d", typ, id, event, afterSequence)
}
