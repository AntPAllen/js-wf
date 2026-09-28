package sim

import (
	"context"
	"fmt"
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
	subject    string
	data       []byte
	sequence   uint64
	deliveries uint64
	deadline   int64
	acked      bool
}

// DispatchTransport models the durable pull/ack subset used by
// worker.RunPartition. It has virtual AckWait, explicit ack/nak/progress, and
// injected consumer-leader errors; it does not model the handler's journal.
type DispatchTransport struct {
	mu       sync.Mutex
	schedule *Scheduler
	ackWait  int64
	records  []*dispatchRecord
	faults   []DispatchFault
	creates  int
	sequence uint64
}

var _ worker.DispatchPort = (*DispatchTransport)(nil)

func NewDispatchTransport(schedule *Scheduler, ackWait time.Duration) *DispatchTransport {
	return &DispatchTransport{schedule: schedule, ackWait: ackWait.Milliseconds()}
}

func (m *DispatchTransport) QueueFault(f DispatchFault) error {
	if f.Operation != "consumer" && f.Operation != "fetch" && f.Operation != "ack" {
		return fmt.Errorf("invalid dispatch fault operation %q", f.Operation)
	}
	if f.Kind != "leader_changed" && f.Kind != "lose_ack_after_commit" {
		return fmt.Errorf("invalid dispatch fault %q", f.Kind)
	}
	if f.Kind == "lose_ack_after_commit" && f.Operation != "ack" || f.Kind == "leader_changed" && f.Operation == "ack" {
		return fmt.Errorf("invalid %s fault on %s", f.Kind, f.Operation)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, f)
	return nil
}

func (m *DispatchTransport) PublishRun(subject string, data []byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sequence++
	m.records = append(m.records, &dispatchRecord{subject: subject, data: append([]byte(nil), data...), sequence: m.sequence})
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
		if record.subject != c.subject || record.acked || record.deliveries > 0 && m.schedule.NowMillis() < record.deadline {
			continue
		}
		record.deliveries++
		record.deadline = m.schedule.NowMillis() + m.ackWait
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
	return &jetstream.MsgMetadata{NumDelivered: m.delivery, Sequence: jetstream.SequencePair{Stream: m.record.sequence, Consumer: m.delivery}}, nil
}
func (m *dispatchMsg) Data() []byte                    { return append([]byte(nil), m.record.data...) }
func (m *dispatchMsg) Headers() nats.Header            { return nats.Header{} }
func (m *dispatchMsg) Subject() string                 { return m.record.subject }
func (m *dispatchMsg) Reply() string                   { return "" }
func (m *dispatchMsg) Ack() error                      { return m.finish("ack", 0) }
func (m *dispatchMsg) DoubleAck(context.Context) error { return m.Ack() }
func (m *dispatchMsg) Nak() error                      { return m.finish("nak", 0) }
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
		// JetStream accepts a late ack from an earlier delivery even after
		// another client has received the redelivery. The first committed ack
		// retires the stream message; later acks are idempotent.
		m.record.acked = true
		if model.takeFault("ack") == "lose_ack_after_commit" {
			event.Outcome = "lose_ack_after_commit"
			model.event(event)
			return ErrTransportLost
		}
		event.Outcome = "ok"
		model.event(event)
		return nil
	}
	if m.record.acked || m.delivery != m.record.deliveries {
		return fmt.Errorf("stale dispatch delivery")
	}
	switch operation {
	case "term":
		m.record.acked = true
	case "nak":
		m.record.deadline = model.schedule.NowMillis() + delay.Milliseconds()
	case "progress":
		m.record.deadline = model.schedule.NowMillis() + model.ackWait
	default:
		return fmt.Errorf("invalid dispatch response %q", operation)
	}
	event.Outcome = "ok"
	model.event(event)
	return nil
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
	m.schedule.trace.Transport = append(m.schedule.trace.Transport, event)
}
