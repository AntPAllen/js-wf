//go:build linux

package integration_test

import (
	"strings"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/worker"
)

func TestMatrixControllerAppendBoundsIncludeUnknownButRejectMissingEvidence(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	records := []journal.Record{{Entry: journal.Entry{Index: 2, Kind: journal.Completed, WorkerID: "owner"}, Sequence: 17}}
	observation := worker.OperationEvent{Type: "typ", ID: "id", Worker: "owner", Operation: "journal_append", JournalIndex: 2, JournalKind: journal.Completed, At: at, Duration: time.Second}
	unknown := observation
	unknown.At = at.Add(2 * time.Second)
	unknown.Duration = 4 * time.Second
	unknown.Error = journal.ErrUnknown.Error()
	bounds, err := matrixControllerAppendBounds("typ", "id", records, []worker.OperationEvent{observation, unknown}, nil)
	if err != nil || len(bounds) != 1 || bounds[0].Attempts != 2 || !bounds[0].Before.Equal(at.Add(-2*time.Second)) || !bounds[0].After.Equal(at) {
		t.Fatalf("bounds=%+v err=%v", bounds, err)
	}
	if _, err := matrixControllerAppendBounds("typ", "id", records, []worker.OperationEvent{unknown}, nil); err == nil {
		t.Fatal("unknown append return incorrectly established commit upper bound")
	}
	// A server can commit after the caller's timeout. A later independent
	// receipt bounds that commit without pretending the timeout was an ack.
	lateReceipt := at.Add(3 * time.Second)
	recovered, err := matrixControllerAppendBounds("typ", "id", records, []worker.OperationEvent{unknown}, map[uint64]time.Time{17: lateReceipt})
	if err != nil || !recovered[0].After.Equal(lateReceipt) {
		t.Fatalf("late receipt bounds=%+v err=%v", recovered, err)
	}
	for _, mode := range []string{"missing", "wrong_worker", "wrong_index", "wrong_kind", "wrong_invocation", "failed", "negative_duration", "zero_time"} {
		t.Run(mode, func(t *testing.T) {
			changed := observation
			switch mode {
			case "missing":
				changed.Operation = "journal_read"
			case "wrong_worker":
				changed.Worker = "contender"
			case "wrong_index":
				changed.JournalIndex = 3
			case "wrong_kind":
				changed.JournalKind = journal.Started
			case "wrong_invocation":
				changed.ID = "other"
			case "failed":
				changed.Error = journal.ErrStale.Error()
			case "negative_duration":
				changed.Duration = -time.Second
			case "zero_time":
				changed.At = time.Time{}
			}
			if _, err := matrixControllerAppendBounds("typ", "id", records, []worker.OperationEvent{changed}, nil); err == nil {
				t.Fatal("invalid controller evidence accepted")
			}
		})
	}
}

func TestMatrixControllerProgressUsesCausalSequenceAndConservativeWindow(t *testing.T) {
	at := time.Unix(100, 0).UTC()
	bounds := []matrixControllerAppendBound{{Sequence: 10, Before: at, After: at.Add(10 * time.Second)}, {Sequence: 20, Before: at.Add(time.Second), After: at.Add(2 * time.Second)}}
	sample, err := matrixControllerProgress("typ", "id", "child_completed", at, 10, bounds)
	if err != nil || sample.Delay != 2*time.Second || !sample.Observed.Equal(bounds[1].After) {
		t.Fatalf("sample=%+v err=%v", sample, err)
	}
	// Receipt time of the earlier sequence is later. Sequence ordering must
	// still choose the next entry, rather than that delayed earlier observation.
	if _, err := matrixControllerProgress("typ", "id", "child_completed", time.Time{}, 10, bounds); err == nil {
		t.Fatal("missing enabling evidence accepted")
	}
	if _, err := matrixControllerProgress("typ", "id", "child_completed", at, 20, bounds); err == nil || !strings.Contains(err.Error(), "missing causal") {
		t.Fatalf("missing progress err=%v", err)
	}
	if _, err := matrixControllerProgress("typ", "id", "child_completed", at.Add(3*time.Second), 10, bounds); err == nil {
		t.Fatal("reversed controller timeline accepted")
	}
}
