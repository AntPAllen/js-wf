// Package stepwire admits runtime step declarations without interpreting user inputs.
package stepwire

import (
	"bytes"
	"fmt"
	"time"

	"js-wf/internal/checkpoint"
)

// Request covers the SDK request variants, including ordered Select cases.
// Inputs are represented by their hash; user input bytes are not in this header.
type Request struct {
	Kind          string    `json:"kind,omitempty"`
	Name          string    `json:"name"`
	InputHash     string    `json:"input_hash"`
	DurationNanos int64     `json:"duration_nanos,omitempty"`
	FireAt        time.Time `json:"fire_at,omitempty"`
	ClockDomain   string    `json:"clock_domain,omitempty"`
	TimerStep     uint64    `json:"timer_step,omitempty"`
	TimerName     string    `json:"timer_name,omitempty"`
	ChildType     string    `json:"child_type,omitempty"`
	ChildID       string    `json:"child_id,omitempty"`
	Cases         []Case    `json:"cases,omitempty"`
}

type Case struct {
	Kind        string    `json:"kind"`
	Name        string    `json:"name"`
	TimerStep   uint64    `json:"timer_step,omitempty"`
	FireAt      time.Time `json:"fire_at,omitempty"`
	ClockDomain string    `json:"clock_domain,omitempty"`
	ChildType   string    `json:"child_type,omitempty"`
	ChildID     string    `json:"child_id,omitempty"`
}

// Decode checks the complete runtime envelope before exposing typed values.
// Kind-specific semantics and input-hash comparison remain the caller's job.
func Decode(raw []byte, target *Request) error {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("missing step declaration")
	}
	return checkpoint.DecodeUnambiguous(raw, target)
}
