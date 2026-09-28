package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type AppendFault string

const (
	DropBeforeCommit   AppendFault = "drop_before_commit"
	LoseAckAfterCommit AppendFault = "lose_ack_after_commit"
	RejectUnchanged    AppendFault = "reject_unchanged_tail"
	CompetingCommit    AppendFault = "competing_commit"
)

var ErrTransportLost = errors.New("simulated transport lost")

type Fault struct {
	Kind          AppendFault
	CompetingData []byte
}

type Message struct {
	Subject  string
	Sequence uint64
	Data     []byte
}

// JournalTransport models only the stream operations required by
// journal.Append. It preserves global stream sequences and each subject tail.
// Faults are consumed in publish order; callers can choose their order through
// Scheduler rather than relying on Go goroutine timing.
type JournalTransport struct {
	mu       sync.Mutex
	schedule *Scheduler
	sequence uint64
	messages map[string][]Message
	faults   []Fault
}

var _ journal.AppendPort = (*JournalTransport)(nil)
var _ journal.ReadPort = (*JournalTransport)(nil)

func NewJournalTransport(schedule *Scheduler) *JournalTransport {
	return &JournalTransport{schedule: schedule, messages: map[string][]Message{}}
}

func (m *JournalTransport) QueueFault(f Fault) error {
	switch f.Kind {
	case DropBeforeCommit, LoseAckAfterCommit, RejectUnchanged:
		if len(f.CompetingData) != 0 {
			return fmt.Errorf("unexpected competing data for fault %s", f.Kind)
		}
	case CompetingCommit:
		if len(f.CompetingData) == 0 {
			return fmt.Errorf("competing commit has no data")
		}
	default:
		return fmt.Errorf("unknown append fault %q", f.Kind)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	f.CompetingData = append([]byte(nil), f.CompetingData...)
	m.faults = append(m.faults, f)
	return nil
}

func (m *JournalTransport) Last(ctx context.Context, subject string) (journal.AppendTail, error) {
	if err := ctx.Err(); err != nil {
		return journal.AppendTail{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	list := m.messages[subject]
	if len(list) == 0 {
		m.event(TransportEvent{Operation: "last", Subject: subject, Outcome: "not_found"})
		return journal.AppendTail{}, jetstream.ErrMsgNotFound
	}
	last := list[len(list)-1]
	m.event(TransportEvent{Operation: "last", Subject: subject, Sequence: last.Sequence, DataSHA256: digest(last.Data), Outcome: "ok"})
	return journal.AppendTail{Sequence: last.Sequence, Data: append([]byte(nil), last.Data...)}, nil
}

func (m *JournalTransport) Next(ctx context.Context, subject string, from uint64) (journal.AppendTail, error) {
	if err := ctx.Err(); err != nil {
		return journal.AppendTail{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, message := range m.messages[subject] {
		if message.Sequence < from {
			continue
		}
		m.event(TransportEvent{Operation: "next_journal", Subject: subject, Expected: from, Sequence: message.Sequence, DataSHA256: digest(message.Data), Outcome: "ok"})
		return journal.AppendTail{Sequence: message.Sequence, Data: append([]byte(nil), message.Data...)}, nil
	}
	m.event(TransportEvent{Operation: "next_journal", Subject: subject, Expected: from, Outcome: "not_found"})
	return journal.AppendTail{}, jetstream.ErrMsgNotFound
}

func (m *JournalTransport) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "publish", Subject: subject, Expected: expected, DataSHA256: digest(data)}
	var fault Fault
	if len(m.faults) != 0 {
		fault, m.faults = m.faults[0], m.faults[1:]
	}
	if fault.Kind == DropBeforeCommit {
		event.Outcome = string(fault.Kind)
		m.event(event)
		return 0, ErrTransportLost
	}
	if fault.Kind == RejectUnchanged {
		event.Outcome = string(fault.Kind)
		m.event(event)
		return 0, wrongLastSequence()
	}
	if fault.Kind == CompetingCommit {
		sequence := m.commit(subject, fault.CompetingData)
		m.event(TransportEvent{Operation: "injected_commit", Subject: subject, Sequence: sequence, DataSHA256: digest(fault.CompetingData), Outcome: string(fault.Kind)})
	}
	var current uint64
	list := m.messages[subject]
	if len(list) != 0 {
		current = list[len(list)-1].Sequence
	}
	if current != expected {
		event.Outcome = "wrong_last_sequence"
		m.event(event)
		return 0, wrongLastSequence()
	}
	seq := m.commit(subject, data)
	event.Sequence = seq
	if fault.Kind == LoseAckAfterCommit {
		event.Outcome = string(fault.Kind)
		m.event(event)
		return 0, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return seq, nil
}

func (m *JournalTransport) Wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay < 0 || delay%time.Millisecond != 0 {
		return fmt.Errorf("invalid virtual delay %s", delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.schedule.AdvanceMillis(delay.Milliseconds()); err != nil {
		return err
	}
	m.event(TransportEvent{Operation: "wait", Outcome: fmt.Sprintf("%dms", delay.Milliseconds())})
	return nil
}

func (m *JournalTransport) Messages(subject string) []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]Message(nil), m.messages[subject]...)
	for i := range out {
		out[i].Data = append([]byte(nil), out[i].Data...)
	}
	return out
}

func (m *JournalTransport) commit(subject string, data []byte) uint64 {
	m.sequence++
	m.messages[subject] = append(m.messages[subject], Message{Subject: subject, Sequence: m.sequence, Data: append([]byte(nil), data...)})
	return m.sequence
}

func (m *JournalTransport) event(event TransportEvent) {
	event.AtMillis = m.schedule.NowMillis()
	m.schedule.RecordTransport(event)
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func wrongLastSequence() error {
	return &jetstream.APIError{Code: 400, ErrorCode: jetstream.JSErrCodeStreamWrongLastSequenceConstant, Description: "wrong last sequence"}
}
