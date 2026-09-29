package sim

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type StartFault struct {
	Operation string
	Kind      string
}

type runDedupEntry struct {
	sequence uint64
	atMillis int64
}

// StartTransport models the write-once invocation stream, run enqueue, and
// input objects used by client.Start. Run messages are retained until a worker
// consumes them; this slice does not model that consumer yet.
type StartTransport struct {
	mu          sync.Mutex
	schedule    *Scheduler
	invSeq      uint64
	runSeq      uint64
	invocations map[string]jetstream.RawStreamMsg
	runs        []Message
	onRunCommit func(Message)
	runIDs      map[string]runDedupEntry
	runWindow   int64
	objects     map[string][]byte
	journals    map[string]uint64
	faults      []StartFault
}

var _ client.StartPort = (*StartTransport)(nil)
var _ reconcile.StartScanPort = (*StartTransport)(nil)

func NewStartTransport(schedule *Scheduler) *StartTransport {
	return &StartTransport{schedule: schedule, invocations: map[string]jetstream.RawStreamMsg{}, runIDs: map[string]runDedupEntry{}, runWindow: (2 * time.Minute).Milliseconds(), objects: map[string][]byte{}, journals: map[string]uint64{}}
}

// SetRunDedupWindow configures the virtual WF_RUN duplicate window. Call it
// before publishing; the default matches JetStream's two-minute window.
func (m *StartTransport) SetRunDedupWindow(window time.Duration) error {
	if window < time.Millisecond || window%time.Millisecond != 0 {
		return fmt.Errorf("invalid run dedup window %s", window)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runSeq != 0 {
		return fmt.Errorf("run dedup window cannot change after publish")
	}
	m.runWindow = window.Milliseconds()
	return nil
}

func (m *StartTransport) QueueFault(f StartFault) error {
	if f.Operation != "publish_invocation" && f.Operation != "last_invocation" && f.Operation != "enqueue_run" {
		return fmt.Errorf("invalid start fault operation %q", f.Operation)
	}
	switch f.Kind {
	case "drop_before_commit", "lose_ack_after_commit":
		if f.Operation == "last_invocation" {
			return fmt.Errorf("invalid last-invocation fault %q", f.Kind)
		}
	case "stale_read":
		if f.Operation != "last_invocation" {
			return fmt.Errorf("invalid %s fault %q", f.Operation, f.Kind)
		}
	case "bypass_subject_limit":
		if f.Operation != "publish_invocation" {
			return fmt.Errorf("invalid %s fault %q", f.Operation, f.Kind)
		}
	default:
		return fmt.Errorf("invalid start fault %q", f.Kind)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, f)
	return nil
}

func (m *StartTransport) PublishInvocation(ctx context.Context, msg *nats.Msg) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "publish_invocation", Subject: msg.Subject, DataSHA256: digest(msg.Data)}
	fault := m.takeFault("publish_invocation")
	if fault == "drop_before_commit" {
		event.Outcome = fault
		m.event(event)
		return 0, ErrTransportLost
	}
	if _, exists := m.invocations[msg.Subject]; exists {
		if fault == "bypass_subject_limit" {
			if msg.Header.Get(jetstream.ExpectedLastSubjSeqHeader) == "0" {
				event.Outcome = "cas_reject"
				m.event(event)
				return 0, &jetstream.APIError{Code: 400, Description: "wrong last sequence"}
			}
		} else {
			event.Outcome = "subject_full"
			m.event(event)
			return 0, &jetstream.APIError{Code: 400, Description: "maximum messages per subject exceeded"}
		}
	}
	m.invSeq++
	copyMsg := jetstream.RawStreamMsg{Subject: msg.Subject, Sequence: m.invSeq, Header: cloneHeader(msg.Header), Data: append([]byte(nil), msg.Data...)}
	m.invocations[msg.Subject] = copyMsg
	event.Sequence = m.invSeq
	if fault == "lose_ack_after_commit" {
		event.Outcome = fault
		m.event(event)
		return 0, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return m.invSeq, nil
}

func (m *StartTransport) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.takeFault("last_invocation") == "stale_read" {
		m.event(TransportEvent{Operation: "last_invocation", Subject: subject, Outcome: "stale_read"})
		return nil, jetstream.ErrMsgNotFound
	}
	msg, exists := m.invocations[subject]
	if !exists {
		m.event(TransportEvent{Operation: "last_invocation", Subject: subject, Outcome: "not_found"})
		return nil, jetstream.ErrMsgNotFound
	}
	m.event(TransportEvent{Operation: "last_invocation", Subject: subject, Sequence: msg.Sequence, DataSHA256: digest(msg.Data), Outcome: "ok"})
	msg.Header = cloneHeader(msg.Header)
	msg.Data = append([]byte(nil), msg.Data...)
	return &msg, nil
}

func (m *StartTransport) PutInput(ctx context.Context, key string, input []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), input...)
	m.event(TransportEvent{Operation: "put_input", Subject: key, DataSHA256: digest(input), Outcome: "ok"})
	return nil
}

func (m *StartTransport) EnqueueRun(ctx context.Context, subject string, data []byte, messageID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "enqueue_run", Subject: subject, DataSHA256: digest(data)}
	fault := m.takeFault("enqueue_run")
	if fault == "drop_before_commit" {
		event.Outcome = fault
		m.event(event)
		return ErrTransportLost
	}
	if messageID != "" {
		if previous, duplicate := m.runIDs[messageID]; duplicate && m.schedule.NowMillis()-previous.atMillis < m.runWindow {
			event.Sequence = previous.sequence
			event.Outcome = "duplicate"
			m.event(event)
			return nil
		}
	}
	m.runSeq++
	if messageID != "" {
		m.runIDs[messageID] = runDedupEntry{sequence: m.runSeq, atMillis: m.schedule.NowMillis()}
	}
	run := Message{Subject: subject, Sequence: m.runSeq, Data: append([]byte(nil), data...)}
	m.runs = append(m.runs, run)
	if m.onRunCommit != nil {
		m.onRunCommit(run)
	}
	event.Sequence = m.runSeq
	if fault == "lose_ack_after_commit" {
		event.Outcome = fault
		m.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return nil
}

// OnRunCommit connects the retained WF_RUN stream to a modeled consumer.
// Install it before actors begin publishing.
func (m *StartTransport) OnRunCommit(callback func(Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onRunCommit = callback
}

func (m *StartTransport) Wait(ctx context.Context, delay time.Duration) error {
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

func (m *StartTransport) Invocation(subject string) (*jetstream.RawStreamMsg, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msg, exists := m.invocations[subject]
	if !exists {
		return nil, false
	}
	msg.Header = cloneHeader(msg.Header)
	msg.Data = append([]byte(nil), msg.Data...)
	return &msg, true
}

func (m *StartTransport) Runs() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]Message(nil), m.runs...)
	for i := range out {
		out[i].Data = append([]byte(nil), out[i].Data...)
	}
	return out
}

func (m *StartTransport) Input(key string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.objects[key]...)
}

func (m *StartTransport) InputBlob(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.objects[key]
	if !exists {
		m.event(TransportEvent{Operation: "get_input_blob", Subject: key, Outcome: "not_found"})
		return nil, jetstream.ErrObjectNotFound
	}
	m.event(TransportEvent{Operation: "get_input_blob", Subject: key, DataSHA256: digest(value), Outcome: "ok"})
	return append([]byte(nil), value...), nil
}

func (m *StartTransport) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.invocations {
		if entry.Sequence == sequence {
			entry.Header = cloneHeader(entry.Header)
			entry.Data = append([]byte(nil), entry.Data...)
			m.event(TransportEvent{Operation: "get_invocation", Subject: entry.Subject, Sequence: sequence, Outcome: "ok"})
			return &entry, nil
		}
	}
	m.event(TransportEvent{Operation: "get_invocation", Sequence: sequence, Outcome: "not_found"})
	return nil, jetstream.ErrMsgNotFound
}

func (m *StartTransport) LastInvocationSequence(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.event(TransportEvent{Operation: "invocation_stream_info", Sequence: m.invSeq, Outcome: "ok"})
	return m.invSeq, nil
}

func (m *StartTransport) JournalExists(ctx context.Context, subject string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	exists := m.journals[subject] != 0
	outcome := "not_found"
	if exists {
		outcome = "ok"
	}
	m.event(TransportEvent{Operation: "journal_exists", Subject: subject, Outcome: outcome})
	return exists, nil
}

func (m *StartTransport) EnqueueStart(ctx context.Context, typ, id string, sequence uint64) error {
	key := identity.Key(typ, id)
	return m.EnqueueRun(ctx, identity.RunSubject(typ, id, provision.Partitions), []byte(key), "start:"+key+":"+strconv.FormatUint(sequence, 10))
}

// MarkJournal records that a worker has started this invocation. Repeated
// reconciliation must then skip it even if the run message has been consumed.
func (m *StartTransport) MarkJournal(typ, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subject := identity.JournalSubject(typ, id)
	invocation := m.invocations[identity.InvocationSubject(typ, id)]
	m.journals[subject] = invocation.Sequence
	m.event(TransportEvent{Operation: "mark_journal", Subject: subject, Outcome: "ok"})
}

// PurgeJournal models the journal stage of retirement before invocation purge.
func (m *StartTransport) PurgeJournal(typ, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subject := identity.JournalSubject(typ, id)
	delete(m.journals, subject)
	m.event(TransportEvent{Operation: "purge_journal", Subject: subject, Outcome: "ok"})
}

// PurgeInvocation leaves a sequence hole, as a real stream purge does.
func (m *StartTransport) PurgeInvocation(subject string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.invocations, subject)
	m.event(TransportEvent{Operation: "purge_invocation", Subject: subject, Outcome: "ok"})
}

func (m *StartTransport) takeFault(operation string) string {
	for i, fault := range m.faults {
		if fault.Operation == operation {
			m.faults = append(m.faults[:i], m.faults[i+1:]...)
			return fault.Kind
		}
	}
	return ""
}

func (m *StartTransport) event(event TransportEvent) {
	event.AtMillis = m.schedule.NowMillis()
	m.schedule.RecordTransport(event)
}

func cloneHeader(header nats.Header) nats.Header {
	copyHeader := nats.Header{}
	for key, values := range header {
		copyHeader[key] = append([]string(nil), values...)
	}
	return copyHeader
}
