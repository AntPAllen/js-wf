package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrStale   = errors.New("journal compare-and-swap lost")
	ErrUnknown = errors.New("journal append outcome unknown")
	ErrGap     = errors.New("journal gap or corrupt entry")
	ErrTooLong = errors.New("journal exceeds 100000 entries")
)

const MaxEntries = 100000

// A stopped server can leave a JetStream API lookup waiting for its caller's
// entire workflow lifetime. Bound one append attempt so the worker can nak
// and retry against the new leader while preserving the same CAS precondition.
const appendAttemptTimeout = 5 * time.Second

type Kind string

const (
	Started        Kind = "Started"
	StepRequested  Kind = "StepRequested"
	StepCompleted  Kind = "StepCompleted"
	Suspended      Kind = "Suspended"
	SignalConsumed Kind = "SignalConsumed"
	Attempt        Kind = "Attempt"
	Completed      Kind = "Completed"
	Failed         Kind = "Failed"
)

type Entry struct {
	Epoch    uint64          `json:"epoch"`
	Index    uint64          `json:"index"`
	Kind     Kind            `json:"kind"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	WorkerID string          `json:"worker_id,omitempty"`
}

type Record struct {
	Entry
	Sequence uint64 `json:"sequence"`
}

// AttemptPayload records a handler panic before the worker retries it.
// Count is per invocation and survives worker replacement.
type AttemptPayload struct {
	Count int    `json:"count"`
	Error string `json:"error"`
}

func DecodeAttempt(data []byte) (AttemptPayload, error) {
	var attempt AttemptPayload
	if json.Unmarshal(data, &attempt) != nil || attempt.Count < 1 || attempt.Error == "" {
		return AttemptPayload{}, ErrGap
	}
	return attempt, nil
}

type Store struct{ js jetstream.JetStream }

func New(js jetstream.JetStream) *Store { return &Store{js: js} }

// Append atomically compares the last sequence for this invocation's subject.
// A timeout is ambiguous: the caller must Read before retrying.
func (s *Store) Append(ctx context.Context, typ, id string, e Entry, expectedSeq uint64) (uint64, error) {
	if err := identity.Validate(typ, id); err != nil {
		return 0, err
	}
	if e.Index >= MaxEntries {
		return 0, ErrTooLong
	}
	if e.Kind == "" {
		return 0, fmt.Errorf("empty journal kind")
	}
	attemptCtx, stopAttempt := context.WithTimeout(ctx, appendAttemptTimeout)
	defer stopAttempt()
	stream, err := s.js.Stream(attemptCtx, "WF_JRN")
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("%w: stream lookup: %v", ErrUnknown, err)
	}
	last, err := stream.GetLastMsgForSubject(attemptCtx, identity.JournalSubject(typ, id))
	if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("%w: tail lookup: %v", ErrUnknown, err)
	}
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		if expectedSeq != 0 || e.Index != 0 || e.Kind != Started {
			return 0, ErrStale
		}
	} else {
		if last.Sequence != expectedSeq {
			return 0, ErrStale
		}
		var prev Entry
		if err := json.Unmarshal(last.Data, &prev); err != nil {
			return 0, ErrGap
		}
		if e.Index != prev.Index+1 || e.Epoch < prev.Epoch || prev.Kind == Completed || prev.Kind == Failed || e.Kind == Started {
			return 0, ErrStale
		}
	}
	data, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}
	// A replicated stream can briefly reject the correct expected sequence
	// during leader/replica state convergence. Only classify a CAS rejection
	// as stale after observing a newer subject tail. Retrying the same CAS is
	// safe: a competing writer's committed append makes it fail again.
	for attempt := 0; attempt < 3; attempt++ {
		ack, err := s.js.Publish(attemptCtx, identity.JournalSubject(typ, id), data, jetstream.WithExpectLastSequencePerSubject(expectedSeq))
		if err == nil {
			return ack.Sequence, nil
		}
		var api *jetstream.APIError
		if !errors.As(err, &api) {
			// A transport error cannot establish whether the server committed
			// the publish. The caller must reread before choosing a retry.
			return 0, fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		if api.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence && api.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequenceConstant {
			return 0, err // an explicit server rejection did not append
		}
		current, readErr := stream.GetLastMsgForSubject(attemptCtx, identity.JournalSubject(typ, id))
		if readErr != nil && !errors.Is(readErr, jetstream.ErrMsgNotFound) {
			return 0, fmt.Errorf("%w: CAS rejected: %v; tail read: %v", ErrUnknown, err, readErr)
		}
		if readErr == nil && current.Sequence > expectedSeq {
			return 0, fmt.Errorf("%w: expected subject seq %d, current %d: %v", ErrStale, expectedSeq, current.Sequence, err)
		}
		if attempt == 2 || attemptCtx.Err() != nil {
			return 0, fmt.Errorf("%w: CAS rejected while subject tail did not advance past %d: %v", ErrUnknown, expectedSeq, err)
		}
		select {
		case <-attemptCtx.Done():
			return 0, fmt.Errorf("%w: %v", ErrUnknown, attemptCtx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	return 0, ErrUnknown
}

// Read uses the server's next-message-for-subject lookup. Stream sequence
// numbers are global, so they may jump even when journal indices are contiguous.
func (s *Store) Read(ctx context.Context, typ, id string) ([]Record, uint64, error) {
	// A compactor can advance the snapshot manifest and purge the old live
	// prefix between loadSnapshot and the next-subject scan. Retry the whole
	// logical read so the new manifest supplies the prefix. Persistent gaps
	// still fail closed after a short bounded interval.
	records, tail, err := s.readOnce(ctx, typ, id)
	if !errors.Is(err, ErrGap) {
		return records, tail, err
	}
	retryUntil := time.Now().Add(2 * time.Second)
	for {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		if time.Now().After(retryUntil) {
			return nil, 0, err
		}
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		records, tail, err = s.readOnce(ctx, typ, id)
		if !errors.Is(err, ErrGap) {
			return records, tail, err
		}
	}
}

func (s *Store) readOnce(ctx context.Context, typ, id string) ([]Record, uint64, error) {
	if err := identity.Validate(typ, id); err != nil {
		return nil, 0, err
	}
	stream, err := s.js.Stream(ctx, "WF_JRN")
	if err != nil {
		return nil, 0, err
	}
	subject := identity.JournalSubject(typ, id)
	out, snap, err := s.loadSnapshot(ctx, typ, id)
	if err != nil {
		return nil, 0, err
	}
	var tail uint64
	seq := uint64(1)
	if snap != nil {
		seq = snap.LastSeq + 1
		tail = snap.LastSeq
	}
	readLive := false
	for {
		m, err := stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, 0, err
		}
		var e Entry
		if err := json.Unmarshal(m.Data, &e); err != nil {
			return nil, 0, fmt.Errorf("%w: %v", ErrGap, err)
		}
		out, err = verifyNext(out, Record{Entry: e, Sequence: m.Sequence})
		if err != nil {
			return nil, 0, err
		}
		readLive = true
		tail = m.Sequence
		if len(out) > MaxEntries {
			return nil, 0, ErrTooLong
		}
		seq = m.Sequence + 1
	}
	if snap != nil && !readLive {
		return nil, 0, ErrGap
	}
	return out, tail, nil
}

func verifyNext(out []Record, next Record) ([]Record, error) {
	if next.Index != uint64(len(out)) {
		return nil, fmt.Errorf("%w: expected index %d, got %d", ErrGap, len(out), next.Index)
	}
	if next.Sequence == 0 {
		return nil, ErrGap
	}
	if len(out) > 0 {
		prev := out[len(out)-1]
		if next.Sequence <= prev.Sequence || next.Epoch < prev.Epoch || prev.Kind == Completed || prev.Kind == Failed || next.Kind == Started {
			return nil, ErrGap
		}
	} else if next.Kind != Started {
		return nil, ErrGap
	}
	return append(out, next), nil
}
