package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"js-wf/internal/checkpoint"
	"js-wf/journal"
)

// Own wire declarations: no production stepwire decoder or SDK replay is used.
// Result remains opaque user JSON; only the runtime envelope is admitted here.
type auditedStepRequest struct {
	Kind        string    `json:"kind"`
	Name        string    `json:"name"`
	InputHash   string    `json:"input_hash"`
	Duration    int64     `json:"duration_nanos"`
	FireAt      time.Time `json:"fire_at"`
	ClockDomain string    `json:"clock_domain"`
	TimerStep   uint64    `json:"timer_step"`
	TimerName   string    `json:"timer_name"`
	ChildType   string    `json:"child_type"`
	ChildID     string    `json:"child_id"`
	Cases       []struct {
		Kind        string    `json:"kind"`
		Name        string    `json:"name"`
		TimerStep   uint64    `json:"timer_step"`
		FireAt      time.Time `json:"fire_at"`
		ClockDomain string    `json:"clock_domain"`
		ChildType   string    `json:"child_type"`
		ChildID     string    `json:"child_id"`
	} `json:"cases"`
}

type auditedStepCompletion struct {
	CaseIndex    *int            `json:"case_index"`
	Result       json.RawMessage `json:"result"`
	ResultRef    string          `json:"result_ref"`
	ResultHash   string          `json:"result_hash"`
	Error        string          `json:"error"`
	ErrorKind    string          `json:"error_kind"`
	SignalSeq    uint64          `json:"signal_seq"`
	Selected     string          `json:"selected"`
	Cancelled    bool            `json:"cancelled"`
	MetadataRef  json.RawMessage `json:"checkpoint_metadata_ref"`
	MetadataHash json.RawMessage `json:"checkpoint_metadata_hash"`
}

func auditSDKStepEnvelope(entry journal.Entry) error {
	if entry.Kind != journal.StepRequested && entry.Kind != journal.StepCompleted {
		return nil
	}
	raw := bytes.TrimSpace(entry.Payload)
	if len(raw) == 0 || raw[0] != '{' {
		return fmt.Errorf("SDK step envelope is not an object")
	}
	if entry.Kind == journal.StepRequested {
		var request auditedStepRequest
		if err := checkpoint.DecodeUnambiguous(raw, &request); err != nil {
			return fmt.Errorf("invalid SDK request envelope: %w", err)
		}
		return nil
	}
	var done auditedStepCompletion
	if err := checkpoint.DecodeUnambiguous(raw, &done); err != nil {
		return fmt.Errorf("invalid SDK completion envelope: %w", err)
	}
	for _, field := range []json.RawMessage{done.MetadataRef, done.MetadataHash} {
		if len(field) == 0 {
			continue
		}
		var value string
		if bytes.Equal(bytes.TrimSpace(field), []byte("null")) || json.Unmarshal(field, &value) != nil {
			return fmt.Errorf("invalid SDK metadata envelope field")
		}
	}
	return nil
}
