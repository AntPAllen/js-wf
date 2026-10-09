// Package checkpoint defines the durable continuation frame. Worker publication
// and dispatch are not yet wired to this format.
package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"js-wf/identity"
)

const Version = 1

// MaxBytes bounds decoding and the entire serialized materialized state, not
// just user locals. Frames always belong in Object Store, never inline journals.
const MaxBytes = 16 << 20

var ErrInvalid = errors.New("invalid continuation checkpoint")

// Identity includes the immutable invocation stream sequence, so an ID reused
// after retirement cannot inherit another invocation's state.
type Identity struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	InvSeq uint64 `json:"inv_seq"`
}

// Anchor identifies the checkpoint completion. Its stream sequence lives in
// the manifest: it is unknown when the frame is serialized before publication.
type Anchor struct {
	Index uint64 `json:"index"`
	Epoch uint64 `json:"epoch"`
}

type Signal struct {
	Sequence uint64 `json:"sequence"`
	Name     string `json:"name"`
	Payload  []byte `json:"payload"`
}

type Frame struct {
	Version  int             `json:"version"`
	Identity Identity        `json:"identity"`
	Stage    string          `json:"stage"`
	Data     json.RawMessage `json:"data"`
	Anchor   Anchor          `json:"anchor"`
	// StepPosition counts SDK entries, not journal entries. It includes the
	// checkpoint pair and never resets between stages.
	StepPosition    uint64                     `json:"step_position"`
	State           map[string]json.RawMessage `json:"state"`
	SignalCursor    uint64                     `json:"signal_cursor,omitempty"`
	PendingSignals  []Signal                   `json:"pending_signals,omitempty"`
	ConsumedSignals []uint64                   `json:"consumed_signals"`
	// PromiseOutcomes retains serialized wf.Outcome payloads, including object
	// references. Derived result caches are deliberately not persisted.
	PromiseOutcomes map[string]json.RawMessage `json:"promise_outcomes"`
	CancelledTimers []uint64                   `json:"cancelled_timers"`
	PanicAttempts   uint64                     `json:"panic_attempts"`
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }

func (f Frame) validate() error {
	if f.Version != Version {
		return invalid("unsupported version")
	}
	if identity.Validate(f.Identity.Type, f.Identity.ID) != nil || f.Identity.InvSeq == 0 {
		return invalid("invocation identity")
	}
	if identity.ValidateToken(f.Stage) != nil {
		return invalid("stage")
	}
	if !json.Valid(f.Data) {
		return invalid("continuation data")
	}
	if f.Anchor.Epoch == 0 || f.StepPosition < 2 || f.StepPosition%2 != 0 || f.Anchor.Index < f.StepPosition {
		return invalid("anchor or SDK position")
	}
	for key, raw := range f.State {
		if identity.ValidateToken(key) != nil || !json.Valid(raw) {
			return invalid("state value")
		}
	}
	for name, raw := range f.PromiseOutcomes {
		if identity.ValidateToken(name) != nil {
			return invalid("promise name")
		}
		// An outcome must be an object. Strict decode rejects unknown fields so a
		// newer outcome schema cannot silently lose information in an older worker.
		var out struct {
			InvSeq       uint64          `json:"inv_seq,omitempty"`
			Result       []byte          `json:"result,omitempty"`
			ResultRef    string          `json:"result_ref,omitempty"`
			ResultHash   string          `json:"result_hash,omitempty"`
			Error        string          `json:"error,omitempty"`
			LimitRequest json.RawMessage `json:"limit_request,omitempty"`
			LimitEntry   json.RawMessage `json:"limit_entry,omitempty"`
		}
		if !json.Valid(raw) || bytes.TrimSpace(raw)[0] != '{' || decodeStrict(raw, &out) != nil {
			return invalid("promise outcome")
		}
		if out.ResultRef == "" && out.ResultHash != "" || out.ResultRef != "" && (len(out.Result) != 0 || !validHash(out.ResultHash)) {
			return invalid("promise result reference")
		}
	}
	if !sortedUnique(f.ConsumedSignals, false, 0) {
		return invalid("consumed signal identities")
	}
	var previousSignal uint64
	for _, signal := range f.PendingSignals {
		if signal.Sequence <= previousSignal || signal.Sequence > f.SignalCursor || identity.ValidateToken(signal.Name) != nil {
			return invalid("buffered signal identity")
		}
		if _, consumed := slices.BinarySearch(f.ConsumedSignals, signal.Sequence); consumed {
			return invalid("buffered signal already consumed")
		}
		previousSignal = signal.Sequence
	}
	for _, step := range f.CancelledTimers {
		if step%2 != 0 {
			return invalid("timer SDK position")
		}
	}
	if f.PanicAttempts > f.Anchor.Index {
		return invalid("panic attempts exceed journal position")
	}
	if !sortedUnique(f.ConsumedSignals, false, 0) || !sortedUnique(f.CancelledTimers, true, f.StepPosition) {
		return invalid("signal or timer identities")
	}
	return nil
}

func sortedUnique(values []uint64, allowZero bool, upper uint64) bool {
	for i, v := range values {
		if !allowZero && v == 0 || i > 0 && values[i-1] >= v || upper != 0 && v >= upper {
			return false
		}
	}
	return true
}

func validHash(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && value == hex.EncodeToString(raw)
}

func decodeStrict(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return invalid("trailing JSON")
	}
	return nil
}

// Encode validates and returns exact bytes and their content hash. The hash
// covers the bytes stored, including RawMessage formatting after JSON marshal.
func Encode(frame Frame) ([]byte, string, error) {
	if err := frame.validate(); err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(frame)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if len(raw) > MaxBytes {
		return nil, "", invalid("frame exceeds byte limit")
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

// Decode checks size and hash before parsing, then binds the frame to the
// manifest's expected generation and anchor. No partial state is returned on
// failure. Stage registration is checked separately by the worker registry.
func Decode(raw []byte, hash string, expected Identity, anchor Anchor) (Frame, error) {
	var empty Frame
	if len(raw) > MaxBytes || !validHash(hash) {
		return empty, invalid("size or content hash")
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != hash {
		return empty, invalid("content hash mismatch")
	}
	var frame Frame
	if err := decodeStrict(raw, &frame); err != nil {
		return empty, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := frame.validate(); err != nil {
		return empty, err
	}
	if frame.Identity != expected || frame.Anchor != anchor {
		return empty, invalid("generation or anchor mismatch")
	}
	return frame, nil
}
