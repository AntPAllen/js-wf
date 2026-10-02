package sim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type DispatchFault struct {
	Operation string
	Kind      string
}

type dispatchRecord struct {
	subject        string
	data           []byte
	header         nats.Header
	timestamp      time.Time
	sequence       uint64
	deliveries     uint64
	deadline       int64
	storedDeadline int64
	acked          bool
	retained       bool
}

// DispatchTransport models the durable pull/ack subset used by
// worker.RunPartition. It has virtual AckWait, explicit ack/nak/progress, and
// injected consumer-leader errors; it does not model the handler's journal.
type DispatchTransport struct {
	mu                  sync.Mutex
	schedule            *Scheduler
	ackWait             int64
	consumerClockOffset int64
	storedPendingClock  bool
	records             []*dispatchRecord
	faults              []DispatchFault
	creates             int
	sequence            uint64
	onDrained           func()
	onNextAck           func()
	onNextNak           func()
	onNextProgress      func()
}

var _ worker.DispatchPort = (*DispatchTransport)(nil)

func NewDispatchTransport(schedule *Scheduler, ackWait time.Duration) *DispatchTransport {
	return &DispatchTransport{schedule: schedule, ackWait: ackWait.Milliseconds()}
}

// SetConsumerClockOffset models persisted delivery deadlines interpreted by a
// different leader clock. This is an explicit transport assumption; it does
// not assert NATS's election or pending-state timestamp conversion behavior.
func (m *DispatchTransport) SetConsumerClockOffset(offset time.Duration) error {
	if offset%time.Millisecond != 0 {
		return fmt.Errorf("invalid consumer clock offset %s", offset)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumerClockOffset = offset.Milliseconds()
	m.event(TransportEvent{Operation: "consumer_clock", Outcome: offset.String()})
	return nil
}

func (m *DispatchTransport) consumerNowMillis() int64 {
	return m.schedule.NowMillis() + m.consumerClockOffset
}

// EnableStoredPendingClock models the replicated pending timestamps verified
// against pinned NATS: initial delivery persists the stored message timestamp;
// progress and delayed NAK replace it with the consumer leader's timestamp.
// This must be selected before publishing. Legacy hypothesis traces retain
// their previous semantics unless explicitly enabled.
func (m *DispatchTransport) EnableStoredPendingClock() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.records) != 0 || m.creates != 0 {
		return fmt.Errorf("pending clock contract must precede transport use")
	}
	m.storedPendingClock = true
	m.event(TransportEvent{Operation: "consumer_pending_clock", Outcome: "stored_message_then_consumer_updates"})
	return nil
}

func (m *DispatchTransport) TransferConsumerLeadership(offset time.Duration) error {
	if offset%time.Millisecond != 0 {
		return fmt.Errorf("invalid consumer clock offset %s", offset)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.storedPendingClock {
		return fmt.Errorf("pending clock contract is not enabled")
	}
	m.consumerClockOffset = offset.Milliseconds()
	for _, record := range m.records {
		if record.deliveries != 0 && !record.acked {
			record.deadline = record.storedDeadline
		}
	}
	m.event(TransportEvent{Operation: "consumer_leader_restore", Outcome: offset.String()})
	return nil
}

func (m *DispatchTransport) QueueFault(f DispatchFault) error {
	switch f.Operation {
	case "consumer", "fetch":
		if f.Kind != "leader_changed" {
			return fmt.Errorf("invalid %s fault on %s", f.Kind, f.Operation)
		}
	case "retention":
		if f.Kind != "hold_after_ack" {
			return fmt.Errorf("invalid %s fault on %s", f.Kind, f.Operation)
		}
	case "ack":
		if f.Kind != "lose_ack_after_commit" && f.Kind != "drop_before_commit" {
			return fmt.Errorf("invalid %s fault on %s", f.Kind, f.Operation)
		}
	case "progress":
		if f.Kind != "drop_before_commit" {
			return fmt.Errorf("invalid %s fault on %s", f.Kind, f.Operation)
		}
	case "nak":
		if f.Kind != "drop_before_commit" {
			return fmt.Errorf("invalid %s fault on %s", f.Kind, f.Operation)
		}
	default:
		return fmt.Errorf("invalid dispatch fault operation %q", f.Operation)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, f)
	return nil
}

func (m *DispatchTransport) PublishRun(subject string, data []byte) uint64 {
	return m.PublishRunMessage(subject, data, nil, time.Time{})
}

// PublishRunMessage retains the headers and server timestamp needed by timer wakeups.
func (m *DispatchTransport) PublishRunMessage(subject string, data []byte, header nats.Header, timestamp time.Time) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sequence++
	if m.storedPendingClock && timestamp.IsZero() {
		timestamp = time.UnixMilli(m.schedule.NowMillis())
	}
	m.records = append(m.records, &dispatchRecord{subject: subject, data: append([]byte(nil), data...), header: cloneHeader(header), timestamp: timestamp, sequence: m.sequence, retained: true})
	m.event(TransportEvent{Operation: "run_publish", Subject: subject, Sequence: m.sequence, DataSHA256: digest(data), Outcome: "ok"})
	return m.sequence
}

func (m *DispatchTransport) Consumer(ctx context.Context, partition uint32) (worker.DispatchConsumer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	subject := "wf.run." + strconv.FormatUint(uint64(partition), 10)
	if m.takeFault("consumer") == "leader_changed" {
		m.event(TransportEvent{Operation: "consumer_create", Subject: subject, Outcome: "leader_changed"})
		return nil, jetstream.ErrConsumerLeadershipChanged
	}
	m.creates++
	m.event(TransportEvent{Operation: "consumer_create", Subject: subject, Sequence: uint64(m.creates), Outcome: "ok"})
	return dispatchConsumer{model: m, subject: subject}, nil
}

func (m *DispatchTransport) Wait(ctx context.Context, delay time.Duration) error {
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
	m.event(TransportEvent{Operation: "dispatch_wait", Outcome: fmt.Sprintf("%dms", delay.Milliseconds())})
	return nil
}

func (m *DispatchTransport) Creates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.creates
}

func (m *DispatchTransport) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := 0
	for _, record := range m.records {
		if !record.acked {
			pending++
		}
	}
	return pending
}

// RetainedSequences reports physical stream retention, independently of
// delivery eligibility. A committed consumer ack does not imply removal.
func (m *DispatchTransport) RetainedSequences() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var sequences []uint64
	for _, record := range m.records {
		if record.retained {
			sequences = append(sequences, record.sequence)
		}
	}
	return sequences
}

var ErrRunQueueRetained = errors.New("run queue retains messages")

// CheckDrained applies the real matrix's stream-level drain requirement. The
// consumer may have zero pending deliveries while retained records remain.
func (m *DispatchTransport) CheckDrained() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	retained, pending := 0, 0
	for _, record := range m.records {
		if record.retained {
			retained++
		}
		if !record.acked {
			pending++
		}
	}
	m.event(TransportEvent{Operation: "queue_drain_check", Outcome: fmt.Sprintf("retained_%d_pending_%d", retained, pending)})
	if retained != 0 || pending != 0 {
		return fmt.Errorf("%w: retained=%d deliverable=%d", ErrRunQueueRetained, retained, pending)
	}
	return nil
}

// CommitRetention explicitly commits a deferred stream removal. It is a
// transport completion, not an assumed automatic repair of mixed-version NATS.
func (m *DispatchTransport) CommitRetention(sequence uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, record := range m.records {
		if record.sequence != sequence {
			continue
		}
		if !record.acked {
			modelEvent := TransportEvent{Operation: "stream_retention", Subject: record.subject, Sequence: sequence, Outcome: "before_ack_rejected"}
			m.event(modelEvent)
			return fmt.Errorf("retention before ack for sequence %d", sequence)
		}
		outcome := "duplicate_commit"
		if record.retained {
			record.retained = false
			outcome = "committed"
			m.stopIfDrained()
		}
		m.event(TransportEvent{Operation: "stream_retention", Subject: record.subject, Sequence: sequence, Outcome: outcome})
		return nil
	}
	m.event(TransportEvent{Operation: "stream_retention", Sequence: sequence, Outcome: "sequence_not_found"})
	return fmt.Errorf("retention sequence %d not found", sequence)
}

// PendingSubjects exposes enabled run partitions to cooperative workloads.
// It does not change durable delivery order within a partition.
func (m *DispatchTransport) PendingSubjects() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	for _, record := range m.records {
		if !record.acked {
			seen[record.subject] = true
		}
	}
	subjects := make([]string, 0, len(seen))
	for subject := range seen {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	return subjects
}

// StopWhenDrained ends a modeled partition loop after every published run has
// been acknowledged and removed from the stream. Install it after publishing the workload's messages.
func (m *DispatchTransport) StopWhenDrained(stop func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onDrained = stop
}

// StopAfterNextAck ends a modeled loop after its next committed acknowledgment.
// This lets a test hand off to a different partition while published child or
// parent wakeups remain pending.
func (m *DispatchTransport) StopAfterNextAck(stop func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNextAck = stop
}

// StopAfterNextNak ends one modeled worker loop at its failed delivery so a
// successor can receive the retained run message.
func (m *DispatchTransport) StopAfterNextNak(stop func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNextNak = stop
}

// OnNextProgress observes one committed heartbeat without reading the trace
// concurrently with the worker's handler goroutine.
func (m *DispatchTransport) OnNextProgress(notify func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onNextProgress = notify
}

type dispatchConsumer struct {
	model   *DispatchTransport
	subject string
}

var _ worker.DispatchConsumer = dispatchConsumer{}

func (c dispatchConsumer) FetchOne(ctx context.Context) (worker.DispatchBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m := c.model
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.takeFault("fetch") == "leader_changed" {
		m.event(TransportEvent{Operation: "consumer_fetch", Subject: c.subject, Outcome: "leader_changed"})
		return nil, jetstream.ErrConsumerLeadershipChanged
	}
	for _, record := range m.records {
		if record.subject != c.subject || record.acked || record.deliveries > 0 && m.consumerNowMillis() < record.deadline {
			continue
		}
		record.deliveries++
		record.deadline = m.consumerNowMillis() + m.ackWait
		if m.storedPendingClock {
			record.storedDeadline = record.timestamp.UnixMilli() + m.ackWait
		}
		m.event(TransportEvent{Operation: "consumer_fetch", Subject: c.subject, Sequence: record.sequence, Outcome: fmt.Sprintf("delivery_%d", record.deliveries)})
		channel := make(chan jetstream.Msg, 1)
		channel <- &dispatchMsg{model: m, record: record, delivery: record.deliveries}
		close(channel)
		return dispatchBatch{messages: channel}, nil
	}
	if err := m.schedule.AdvanceMillis(1000); err != nil {
		return nil, err
	}
	m.event(TransportEvent{Operation: "consumer_fetch", Subject: c.subject, Outcome: "no_messages"})
	return nil, jetstream.ErrNoMessages
}

func (c dispatchConsumer) Info(ctx context.Context) (uint64, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	m := c.model
	m.mu.Lock()
	defer m.mu.Unlock()
	var pending uint64
	ackPending := 0
	for _, record := range m.records {
		if record.subject != c.subject || record.acked {
			continue
		}
		if record.deliveries == 0 {
			pending++
		} else {
			ackPending++
		}
	}
	m.event(TransportEvent{Operation: "consumer_info", Subject: c.subject, Outcome: fmt.Sprintf("pending_%d_ack_%d", pending, ackPending)})
	return pending, ackPending, nil
}

type dispatchBatch struct{ messages <-chan jetstream.Msg }

func (b dispatchBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (dispatchBatch) Error() error                     { return nil }

type dispatchMsg struct {
	model    *DispatchTransport
	record   *dispatchRecord
	delivery uint64
}

var _ jetstream.Msg = (*dispatchMsg)(nil)

func (m *dispatchMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.delivery, Sequence: jetstream.SequencePair{Stream: m.record.sequence, Consumer: m.delivery}, Timestamp: m.record.timestamp}, nil
}
func (m *dispatchMsg) Data() []byte         { return append([]byte(nil), m.record.data...) }
func (m *dispatchMsg) Headers() nats.Header { return cloneHeader(m.record.header) }
func (m *dispatchMsg) Subject() string      { return m.record.subject }
func (m *dispatchMsg) Reply() string        { return "" }
func (m *dispatchMsg) Ack() error           { return m.finish("ack", 0) }
func (m *dispatchMsg) DoubleAck(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.Ack()
}
func (m *dispatchMsg) Nak() error { return m.finish("nak", 0) }
func (m *dispatchMsg) NakWithDelay(delay time.Duration) error {
	return m.finish("nak", delay)
}
func (m *dispatchMsg) InProgress() error             { return m.finish("progress", 0) }
func (m *dispatchMsg) Term() error                   { return m.finish("term", 0) }
func (m *dispatchMsg) TermWithReason(_ string) error { return m.Term() }

func (m *dispatchMsg) finish(operation string, delay time.Duration) error {
	model := m.model
	model.mu.Lock()
	defer model.mu.Unlock()
	event := TransportEvent{Operation: "consumer_" + operation, Subject: m.record.subject, Sequence: m.record.sequence}
	if operation == "ack" {
		if m.record.acked {
			event.Outcome = "duplicate_ack"
			model.event(event)
			return nil
		}
		fault := model.takeFault("ack")
		if fault == "drop_before_commit" {
			event.Outcome = fault
			model.event(event)
			return ErrTransportLost
		}
		// JetStream accepts a late ack from an earlier delivery even after
		// another client has received the redelivery. The first committed ack
		// commits consumer progress; stream removal is a separate state.
		m.record.acked = true
		if model.takeFault("retention") == "hold_after_ack" {
			model.event(TransportEvent{Operation: "stream_retention", Subject: m.record.subject, Sequence: m.record.sequence, Outcome: "held_after_ack"})
		} else {
			m.record.retained = false
		}
		if model.onNextAck != nil {
			stop := model.onNextAck
			model.onNextAck = nil
			stop()
		}
		if fault == "lose_ack_after_commit" {
			event.Outcome = "lose_ack_after_commit"
			model.event(event)
			model.stopIfDrained()
			return ErrTransportLost
		}
		event.Outcome = "ok"
		model.event(event)
		model.stopIfDrained()
		return nil
	}
	if m.record.acked || m.delivery != m.record.deliveries {
		return fmt.Errorf("stale dispatch delivery")
	}
	if operation == "progress" && model.takeFault("progress") == "drop_before_commit" {
		event.Outcome = "drop_before_commit"
		model.event(event)
		return ErrTransportLost
	}
	if operation == "nak" && model.takeFault("nak") == "drop_before_commit" {
		event.Outcome = "drop_before_commit"
		model.event(event)
		if model.onNextNak != nil {
			stop := model.onNextNak
			model.onNextNak = nil
			stop()
		}
		return ErrTransportLost
	}
	switch operation {
	case "term":
		m.record.acked = true
		m.record.retained = false
	case "nak":
		m.record.deadline = model.consumerNowMillis() + delay.Milliseconds()
	case "progress":
		m.record.deadline = model.consumerNowMillis() + model.ackWait
	default:
		return fmt.Errorf("invalid dispatch response %q", operation)
	}
	if model.storedPendingClock && (operation == "nak" || operation == "progress") {
		m.record.storedDeadline = m.record.deadline
	}
	event.Outcome = "ok"
	model.event(event)
	if operation == "nak" && model.onNextNak != nil {
		stop := model.onNextNak
		model.onNextNak = nil
		stop()
	}
	if operation == "progress" && model.onNextProgress != nil {
		notify := model.onNextProgress
		model.onNextProgress = nil
		notify()
	}
	return nil
}

func (m *DispatchTransport) stopIfDrained() {
	if m.onDrained == nil || len(m.records) == 0 {
		return
	}
	for _, record := range m.records {
		if record.retained {
			return
		}
	}
	m.onDrained()
}

func (m *DispatchTransport) takeFault(operation string) string {
	for i, fault := range m.faults {
		if fault.Operation == operation {
			m.faults = append(m.faults[:i], m.faults[i+1:]...)
			return fault.Kind
		}
	}
	return ""
}

func (m *DispatchTransport) event(event TransportEvent) {
	event.AtMillis = m.schedule.NowMillis()
	m.schedule.RecordTransport(event)
}
