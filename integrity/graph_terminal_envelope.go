package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

// Own complete terminal wire shape. User result bytes remain opaque; retained
// rejected operations receive the prefix checks below.
type auditedGraphOutcome struct {
	InvSeq       uint64          `json:"inv_seq"`
	Result       []byte          `json:"result"`
	ResultRef    string          `json:"result_ref"`
	ResultHash   string          `json:"result_hash"`
	Error        string          `json:"error"`
	LimitRequest json.RawMessage `json:"limit_request"`
	LimitEntry   *struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	} `json:"limit_entry"`
}

// Check before advancing the terminal: advance releases the request/completion
// prefix. The configured capacity is not durable in these records.
func auditGraphLimitBoundary(entry journal.Entry, prefix *journalAudit) error {
	if entry.Kind != journal.Completed && entry.Kind != journal.Failed {
		return nil
	}
	var outcome auditedGraphOutcome
	if err := checkpoint.DecodeUnambiguous(entry.Payload, &outcome); err != nil {
		return err
	}
	if outcome.LimitRequest == nil && outcome.LimitEntry == nil {
		return nil
	}
	if entry.Kind != journal.Failed || outcome.Error != journal.ErrTooLong.Error() ||
		len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "" ||
		outcome.LimitRequest != nil && outcome.LimitEntry != nil {
		return fmt.Errorf("invalid rejected limit outcome")
	}
	if outcome.LimitRequest != nil {
		if prefix.outstanding {
			return fmt.Errorf("rejected request overlaps prior request")
		}
		if err := auditSDKStepEnvelope(journal.Entry{Kind: journal.StepRequested, Payload: outcome.LimitRequest}); err != nil {
			return err
		}
		var request auditedStepRequest
		if checkpoint.DecodeUnambiguous(outcome.LimitRequest, &request) != nil || request.Kind == "" {
			return fmt.Errorf("invalid rejected request declaration")
		}
		return nil
	}
	raw := bytes.TrimSpace(outcome.LimitEntry.Payload)
	if len(raw) == 0 || raw[0] != '{' {
		return fmt.Errorf("rejected limit payload is not an object")
	}
	switch journal.Kind(outcome.LimitEntry.Kind) {
	case journal.Attempt:
		var attempt struct {
			Count int    `json:"count"`
			Error string `json:"error"`
		}
		if checkpoint.DecodeUnambiguous(raw, &attempt) != nil || attempt.Count < 1 || attempt.Count != prefix.lastAttempt+1 || attempt.Error == "" {
			return fmt.Errorf("rejected attempt does not follow prefix")
		}
	case journal.SignalConsumed:
		var signal auditedCheckpointSignal
		if checkpoint.DecodeUnambiguous(raw, &signal) != nil || signal.Sequence <= prefix.lastSignal || signal.Name == "" {
			return fmt.Errorf("rejected signal does not follow prefix")
		}
	case journal.Suspended:
		var wait struct {
			WaitingOn string `json:"waiting_on"`
		}
		if checkpoint.DecodeUnambiguous(raw, &wait) != nil || wait.WaitingOn == "" ||
			!prefix.outstanding && !validContinuationSuspension(prefix.request, prefix.completion, raw) {
			return fmt.Errorf("rejected suspension lacks supporting request")
		}
	default:
		return fmt.Errorf("invalid rejected limit entry kind")
	}
	return nil
}
