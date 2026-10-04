package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"

	"js-wf/journal"
)

// This opt-in accumulator is checked differentially against the unchanged
// slice-based oracle. It retains protocol state, not the journal prefix. Errors
// are saved until sorted subject reduction so decoding/compaction/error ordering
// remains the same as the existing full audit.
type journalAudit struct {
	count               int
	previousSequence    uint64
	previousEpoch       uint64
	epochWorker         string
	terminal            []byte
	hasTerminal         bool
	outstanding         bool
	request, completion json.RawMessage
	lastSignal          uint64
	lastAttempt         int
	failure             error
}

func (s *journalAudit) add(subject string, record journal.Record) {
	if s.failure != nil {
		return
	}
	s.failure = s.advance(subject, record)
}

func (s *journalAudit) advance(subject string, record journal.Record) error {
	e := record.Entry
	i := s.count
	if record.Sequence == 0 || i > 0 && record.Sequence <= s.previousSequence {
		return fmt.Errorf("%s: invalid retained journal sequence", subject)
	}
	if e.Index != uint64(i) {
		return fmt.Errorf("%s: index %d at position %d", subject, e.Index, i)
	}
	if i == 0 && e.Kind != journal.Started {
		return fmt.Errorf("%s: missing Started", subject)
	}
	if i > 0 && (e.Epoch < s.previousEpoch || e.Kind == journal.Started) {
		return fmt.Errorf("%s: epoch or Started violation", subject)
	}
	// Epochs cannot decrease, so earlier epoch owners cannot be visited again.
	if e.Epoch > s.previousEpoch {
		s.epochWorker = ""
	}
	if e.Epoch != 0 && e.WorkerID != "" {
		if prior := s.epochWorker; prior != "" && prior != e.WorkerID {
			return fmt.Errorf("%s: epoch %d used by workers %s and %s", subject, e.Epoch, prior, e.WorkerID)
		}
		s.epochWorker = e.WorkerID
	}
	if s.hasTerminal {
		return fmt.Errorf("%s: entry after terminal", subject)
	}
	switch e.Kind {
	case journal.Started, journal.Failed:
	case journal.Completed:
		if s.outstanding {
			return fmt.Errorf("%s: successful terminal with unresolved request", subject)
		}
	case journal.StepRequested:
		if s.outstanding {
			return fmt.Errorf("%s: overlapping step requests", subject)
		}
		s.outstanding = true
		s.request, s.completion = e.Payload, nil
	case journal.StepCompleted:
		if !s.outstanding {
			return fmt.Errorf("%s: completion without request", subject)
		}
		s.outstanding = false
		s.completion = e.Payload
	case journal.Suspended:
		if !s.outstanding && !validContinuationSuspension(s.request, s.completion, e.Payload) {
			return fmt.Errorf("%s: suspension without request", subject)
		}
	case journal.SignalConsumed:
		var signal struct {
			Sequence uint64 `json:"sig_seq"`
		}
		if err := json.Unmarshal(e.Payload, &signal); err != nil || signal.Sequence <= s.lastSignal {
			return fmt.Errorf("%s: signal sequence out of order", subject)
		}
		s.lastSignal = signal.Sequence
	case journal.Attempt:
		attempt, err := journal.DecodeAttempt(e.Payload)
		if err != nil || attempt.Count != s.lastAttempt+1 {
			return fmt.Errorf("%s: handler attempt out of order", subject)
		}
		s.lastAttempt = attempt.Count
	default:
		return fmt.Errorf("%s: unknown entry kind %q", subject, e.Kind)
	}
	if e.Kind == journal.Completed || e.Kind == journal.Failed {
		s.terminal, s.hasTerminal = e.Payload, true
		// No later record can validly consume these payloads after a terminal.
		s.request, s.completion = nil, nil
	}
	s.count++
	s.previousSequence, s.previousEpoch = record.Sequence, e.Epoch
	return nil
}

func (s *journalAudit) finish(subject string, terminalValue func() ([]byte, error)) (int, bool, error) {
	if s.failure != nil {
		return 0, false, s.failure
	}
	if s.hasTerminal {
		value, err := terminalValue()
		if err != nil {
			return 0, false, fmt.Errorf("%s: terminal state missing: %w", subject, err)
		}
		if !bytes.Equal(value, s.terminal) {
			return 0, false, fmt.Errorf("%s: terminal state differs", subject)
		}
	}
	return s.count, s.hasTerminal, nil
}
