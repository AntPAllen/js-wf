package sim

import (
	"context"
	"fmt"

	"js-wf/journal"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// BatchReadFault applies to the next modeled pull. After messages can be
// delivered before the error, matching a real partial JetStream batch.
type BatchReadFault struct {
	Kind  string
	After int
}

func (m *JournalTransport) QueueBatchFault(fault BatchReadFault) error {
	if fault.After < 0 || fault.Kind != "no_responders" && fault.Kind != "consumer_deleted" {
		return fmt.Errorf("invalid journal batch fault %+v", fault)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batchFaults = append(m.batchFaults, fault)
	return nil
}

func (m *JournalTransport) Open(ctx context.Context, subject string, sequence uint64) (journal.BatchReadCursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.event(TransportEvent{Operation: "open_journal_batch", Subject: subject, Sequence: sequence, Outcome: "ok"})
	return &journalBatchCursor{transport: m, subject: subject, next: sequence}, nil
}

func (m *JournalTransport) Probe(ctx context.Context, subject string, sequence uint64) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, message := range m.messages[subject] {
		if message.Sequence >= sequence {
			m.event(TransportEvent{Operation: "probe_journal_batch", Subject: subject, Expected: sequence, Sequence: message.Sequence, Outcome: "found"})
			return true, nil
		}
	}
	m.event(TransportEvent{Operation: "probe_journal_batch", Subject: subject, Expected: sequence, Outcome: "not_found"})
	return false, nil
}

type journalBatchCursor struct {
	transport *JournalTransport
	subject   string
	next      uint64
	closed    bool
}

func (c *journalBatchCursor) Fetch(ctx context.Context, limit int) ([]journal.AppendTail, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m := c.transport
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.closed || limit < 1 {
		return nil, fmt.Errorf("invalid journal batch cursor")
	}
	var fault BatchReadFault
	if len(m.batchFaults) > 0 {
		fault, m.batchFaults = m.batchFaults[0], m.batchFaults[1:]
	}
	count := limit
	if fault.Kind != "" && fault.After < count {
		count = fault.After
	}
	messages := make([]journal.AppendTail, 0, count)
	if count > 0 {
		for _, message := range m.messages[c.subject] {
			if message.Sequence < c.next {
				continue
			}
			messages = append(messages, journal.AppendTail{Sequence: message.Sequence, Data: append([]byte(nil), message.Data...)})
			c.next = message.Sequence + 1
			if len(messages) == count {
				break
			}
		}
	}
	event := TransportEvent{Operation: "fetch_journal_batch", Subject: c.subject, Expected: uint64(limit), Sequence: c.next, Outcome: "ok"}
	if fault.Kind != "" {
		event.Outcome = fault.Kind
		m.event(event)
		if fault.Kind == "no_responders" {
			return messages, nats.ErrNoResponders
		}
		return messages, jetstream.ErrConsumerDeleted
	}
	if len(messages) == 0 {
		event.Outcome = "not_found"
		m.event(event)
		return nil, jetstream.ErrNoMessages
	}
	m.event(event)
	return messages, nil
}

func (c *journalBatchCursor) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m := c.transport
	m.mu.Lock()
	defer m.mu.Unlock()
	c.closed = true
	m.event(TransportEvent{Operation: "close_journal_batch", Subject: c.subject, Sequence: c.next, Outcome: "ok"})
	return nil
}
