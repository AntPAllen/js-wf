package wf

import (
	"bytes"

	"js-wf/internal/checkpoint"
	"js-wf/internal/stepwire"
	"js-wf/journal"
)

// validateReplayRecordOrder admits the complete logical history before a plugin
// or handler can run. Physical stream sequence gaps are valid; logical index
// gaps, backwards epochs and records after a terminal are not.
func validateReplayRecordOrder(records []journal.Record, invocation uint64) error {
	if len(records) == 0 || len(records) > journal.MaxEntries {
		return ErrCorruptJournal
	}
	epochWorkers := map[uint64]string{}
	for i, record := range records {
		if record.Index != uint64(i) || record.Sequence == 0 || i == 0 && record.Kind != journal.Started {
			return ErrCorruptJournal
		}
		if i > 0 {
			prev := records[i-1]
			if record.Sequence <= prev.Sequence || record.Epoch < prev.Epoch || prev.Kind == journal.Completed || prev.Kind == journal.Failed || record.Kind == journal.Started {
				return ErrCorruptJournal
			}
		}
		// The integrity checker applies this same ownership rule. Old journals
		// without worker identifiers cannot supply that evidence; contradictory
		// identifiers on a declared nonzero epoch must fail before user code.
		if record.Epoch != 0 && record.WorkerID != "" {
			if prior := epochWorkers[record.Epoch]; prior != "" && prior != record.WorkerID {
				return ErrCorruptJournal
			}
			epochWorkers[record.Epoch] = record.WorkerID
		}
		switch record.Kind {
		case journal.SignalConsumed:
			// Legacy exports also carry runtime signal identity/provenance.
			// The encoded user payload stays opaque to this header check.
			var signal replayCheckpointSignal
			if decodeReplaySignal(record.Payload, &signal) != nil {
				return ErrCorruptJournal
			}
		case journal.StepRequested:
			var request stepwire.Request
			if stepwire.Decode(record.Payload, &request) != nil {
				return ErrCorruptJournal
			}
		case journal.Started, journal.StepCompleted, journal.Suspended, journal.Attempt, journal.Completed, journal.Failed:
		default:
			return ErrCorruptJournal
		}
	}
	last := records[len(records)-1]
	if last.Kind == journal.Completed || last.Kind == journal.Failed {
		// Historical identity-free replay can carry only a terminal marker.
		// Present terminal envelopes still need admission, even without identity.
		if invocation == 0 && len(last.Payload) == 0 {
			return nil
		}
		var outcome Outcome
		if decodeReplayTerminal(last.Payload, &outcome) != nil || invocation != 0 && (outcome.InvSeq != invocation || last.Kind == journal.Failed && outcome.Error == "") {
			return ErrCorruptJournal
		}
	}
	return nil
}

func decodeReplaySignal(raw []byte, signal *replayCheckpointSignal) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ErrCorruptJournal
	}
	return checkpoint.DecodeUnambiguous(raw, signal)
}

// Terminal fields carry runtime identity and result provenance. Result bytes
// and limit-request payload contents remain opaque; the enclosing fields and
// any typed LimitEntry headers must be unambiguous before user code runs.
func decodeReplayTerminal(raw []byte, outcome *Outcome) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ErrCorruptJournal
	}
	return checkpoint.DecodeUnambiguous(raw, outcome)
}
