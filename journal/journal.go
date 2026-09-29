package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
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

// Allow a replica's subject-tail read and the leader's CAS view to converge
// after an election. Repeating the same CAS cannot overwrite another writer.
const unchangedTailRetryLimit = 40

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

const MaxAttemptErrorBytes = 4096

// AttemptError is the exact error text retained for a panic attempt. JSON
// replaces malformed UTF-8, including a rune cut by the byte limit.
func AttemptError(reason string) string {
	if len(reason) > MaxAttemptErrorBytes {
		reason = reason[:MaxAttemptErrorBytes]
	}
	return string([]rune(reason))
}

func DecodeAttempt(data []byte) (AttemptPayload, error) {
	var attempt AttemptPayload
	if json.Unmarshal(data, &attempt) != nil || attempt.Count < 1 || attempt.Error == "" {
		return AttemptPayload{}, ErrGap
	}
	return attempt, nil
}

type Store struct {
	js                jetstream.JetStream
	appendPort        AppendPort
	readPort          ReadPort
	snapshotReadPort  SnapshotReadPort
	snapshotWritePort SnapshotWritePort
	mu                sync.Mutex
	stream            jetstream.Stream
	state             jetstream.KeyValue
}

func New(js jetstream.JetStream) *Store {
	port := NewSnapshotPort(js)
	return &Store{js: js, snapshotReadPort: port, snapshotWritePort: port}
}

// NewWithJetStreamSnapshotPort keeps journal I/O on JetStream while supplying
// a snapshot port, primarily for transport contracts and fault injection.
func NewWithJetStreamSnapshotPort(js jetstream.JetStream, port SnapshotWritePort) *Store {
	return &Store{js: js, snapshotReadPort: port, snapshotWritePort: port}
}

// AppendPort is the transport boundary used by the journal CAS decision path.
// It permits deterministic transport simulations without replacing the SDK's
// entire JetStream interface. Read and snapshot operations still use New.
type AppendPort interface {
	Last(context.Context, string) (AppendTail, error)
	Publish(context.Context, string, []byte, uint64) (uint64, error)
	Wait(context.Context, time.Duration) error
}

// AppendTail is the latest retained raw entry for one journal subject.
type AppendTail struct {
	Sequence uint64
	Data     []byte
}

// NewWithAppendPort builds an append-only store for deterministic transport
// tests. Read and snapshot operations require New with a real JetStream client.
func NewWithAppendPort(port AppendPort) *Store { return &Store{appendPort: port} }

// ReadPort supplies retained live journal messages to the production Read
// validator. The in-memory adapter has no snapshot objects; production Read
// continues to load snapshots through JetStream.
type ReadPort interface {
	Next(context.Context, string, uint64) (AppendTail, error)
	Wait(context.Context, time.Duration) error
}

// NewWithPorts runs Append and live Read decisions against supplied transports.
func NewWithPorts(appendPort AppendPort, readPort ReadPort) *Store {
	return &Store{appendPort: appendPort, readPort: readPort}
}

// NewWithSnapshotReadPort also loads compacted prefixes through a narrow
// manifest and object boundary while keeping live reads on ReadPort.
func NewWithSnapshotReadPort(appendPort AppendPort, readPort ReadPort, snapshotPort SnapshotReadPort) *Store {
	return &Store{appendPort: appendPort, readPort: readPort, snapshotReadPort: snapshotPort}
}

// NewWithSnapshotPort also runs snapshot creation and prefix purge decisions
// against the supplied narrow transport.
func NewWithSnapshotPort(appendPort AppendPort, readPort ReadPort, snapshotPort SnapshotWritePort) *Store {
	return &Store{appendPort: appendPort, readPort: readPort, snapshotReadPort: snapshotPort, snapshotWritePort: snapshotPort}
}

// HasSnapshotTransport reports whether worker-triggered compaction is wired.
func (s *Store) HasSnapshotTransport() bool { return s.snapshotWritePort != nil }

type jetStreamAppendPort struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func (p jetStreamAppendPort) Last(ctx context.Context, subject string) (AppendTail, error) {
	msg, err := p.stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return AppendTail{}, err
	}
	return AppendTail{Sequence: msg.Sequence, Data: msg.Data}, nil
}

func (p jetStreamAppendPort) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	ack, err := p.js.Publish(ctx, subject, data, jetstream.WithExpectLastSequencePerSubject(expected))
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

func (jetStreamAppendPort) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Cache only successful handles. Their operations still make fresh JetStream
// requests, while failed initial lookups remain retryable after provisioning
// or a temporary server outage.
func (s *Store) journalStream(ctx context.Context) (jetstream.Stream, error) {
	if s.js == nil {
		return nil, fmt.Errorf("journal stream unavailable on append-only transport")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream != nil {
		return s.stream, nil
	}
	stream, err := s.js.Stream(ctx, "WF_JRN")
	if err == nil {
		s.stream = stream
	}
	return stream, err
}

func (s *Store) stateKV(ctx context.Context) (jetstream.KeyValue, error) {
	if s.js == nil {
		return nil, fmt.Errorf("journal state unavailable on append-only transport")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != nil {
		return s.state, nil
	}
	state, err := s.js.KeyValue(ctx, "WF_STATE")
	if err == nil {
		s.state = state
	}
	return state, err
}

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
	port := s.appendPort
	if port == nil {
		stream, err := s.journalStream(attemptCtx)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, fmt.Errorf("%w: stream lookup: %v", ErrUnknown, err)
		}
		port = jetStreamAppendPort{js: s.js, stream: stream}
	}
	last, err := port.Last(attemptCtx, identity.JournalSubject(typ, id))
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
	for attempt := 0; attempt < unchangedTailRetryLimit; attempt++ {
		seq, err := port.Publish(attemptCtx, identity.JournalSubject(typ, id), data, expectedSeq)
		if err == nil {
			return seq, nil
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
		current, readErr := port.Last(attemptCtx, identity.JournalSubject(typ, id))
		if readErr != nil && !errors.Is(readErr, jetstream.ErrMsgNotFound) {
			return 0, fmt.Errorf("%w: CAS rejected: %v; tail read: %v", ErrUnknown, err, readErr)
		}
		if readErr == nil && current.Sequence > expectedSeq {
			return 0, fmt.Errorf("%w: expected subject seq %d, current %d: %v", ErrStale, expectedSeq, current.Sequence, err)
		}
		if attempt == unchangedTailRetryLimit-1 || attemptCtx.Err() != nil {
			return 0, fmt.Errorf("%w: CAS rejected while subject tail did not advance past %d: %v", ErrUnknown, expectedSeq, err)
		}
		if err := port.Wait(attemptCtx, 25*time.Millisecond); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrUnknown, err)
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
	var objectGap *snapshotObjectGap
	if errors.As(err, &objectGap) {
		return records, tail, err
	}
	if !errors.Is(err, ErrGap) {
		return records, tail, err
	}
	if s.readPort != nil {
		for attempt := 0; attempt < 80; attempt++ {
			if waitErr := s.readPort.Wait(ctx, 25*time.Millisecond); waitErr != nil {
				return nil, 0, waitErr
			}
			records, tail, err = s.readOnce(ctx, typ, id)
			if errors.As(err, &objectGap) {
				return records, tail, err
			}
			if !errors.Is(err, ErrGap) {
				return records, tail, err
			}
		}
		return nil, 0, err
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
		if errors.As(err, &objectGap) {
			return records, tail, err
		}
		if !errors.Is(err, ErrGap) {
			return records, tail, err
		}
	}
}

func (s *Store) readOnce(ctx context.Context, typ, id string) ([]Record, uint64, error) {
	if err := identity.Validate(typ, id); err != nil {
		return nil, 0, err
	}
	var stream jetstream.Stream
	if s.readPort == nil {
		var err error
		stream, err = s.journalStream(ctx)
		if err != nil {
			return nil, 0, err
		}
	}
	subject := identity.JournalSubject(typ, id)
	var out []Record
	var snap *Snapshot
	if s.readPort == nil || s.snapshotReadPort != nil {
		var err error
		out, snap, err = s.loadSnapshot(ctx, typ, id)
		if err != nil {
			return nil, 0, err
		}
	}
	var tail uint64
	seq := uint64(1)
	if snap != nil {
		seq = snap.LastSeq + 1
		tail = snap.LastSeq
	}
	readLive := false
	for {
		var m AppendTail
		var err error
		if s.readPort != nil {
			m, err = s.readPort.Next(ctx, subject, seq)
		} else {
			var raw *jetstream.RawStreamMsg
			raw, err = stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
			if err == nil {
				m = AppendTail{Sequence: raw.Sequence, Data: raw.Data}
			}
		}
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
