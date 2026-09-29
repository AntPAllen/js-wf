package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/retention"

	"github.com/nats-io/nats.go/jetstream"
)

type PurgeFault struct {
	Operation string
	Kind      AppendFault
}

// PurgeTransport shares retained streams, WF_STATE, and objects with the blob
// sweep model while production retention acquires a modeled KV lease.
type PurgeTransport struct {
	Blobs          *BlobSweepTransport
	leasing        *lease.Store
	schedule       *Scheduler
	fallbackTimers bool
	faults         []PurgeFault
	purgeIDs       map[string]uint64
}

var _ retention.PurgePort = (*PurgeTransport)(nil)

func NewPurgeTransport(schedule *Scheduler) *PurgeTransport {
	blobs := NewBlobSweepTransport(schedule)
	return &PurgeTransport{Blobs: blobs, leasing: lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second)), schedule: schedule, purgeIDs: map[string]uint64{}}
}

func (m *PurgeTransport) EnableFallbackTimers() { m.fallbackTimers = true }

func (m *PurgeTransport) QueueFault(fault PurgeFault) error {
	switch fault.Operation {
	case "purge_WF_INV", "purge_WF_SIG", "purge_WF_JRN", "purge_WF_TIMER", "publish_purge":
	default:
		return fmt.Errorf("invalid purge fault operation %q", fault.Operation)
	}
	if fault.Kind != DropBeforeCommit && fault.Kind != LoseAckAfterCommit {
		return fmt.Errorf("invalid purge fault kind %q", fault.Kind)
	}
	m.faults = append(m.faults, fault)
	return nil
}

func (m *PurgeTransport) takeFault(operation string) AppendFault {
	for i, fault := range m.faults {
		if fault.Operation == operation {
			m.faults = append(m.faults[:i], m.faults[i+1:]...)
			return fault.Kind
		}
	}
	return ""
}

func (m *PurgeTransport) Acquire(ctx context.Context, typ, id string) (retention.PurgeLease, error) {
	return m.leasing.Acquire(ctx, typ, id, "retention")
}

func (m *PurgeTransport) Invocation(ctx context.Context, subject string) (retention.PurgeInvocation, error) {
	if err := ctx.Err(); err != nil {
		return retention.PurgeInvocation{}, err
	}
	m.Blobs.mu.Lock()
	defer m.Blobs.mu.Unlock()
	stream := m.Blobs.streams["WF_INV"]
	var newest uint64
	var header retention.BlobSweepMessage
	for sequence, message := range stream.messages {
		if message.Subject == subject && sequence > newest {
			newest, header = sequence, message
		}
	}
	if newest == 0 {
		m.Blobs.event(TransportEvent{Operation: "purge_invocation_get", Subject: subject, Outcome: "not_found"})
		return retention.PurgeInvocation{}, jetstream.ErrMsgNotFound
	}
	m.Blobs.event(TransportEvent{Operation: "purge_invocation_get", Subject: subject, Sequence: newest, Outcome: "ok"})
	return retention.PurgeInvocation{Sequence: newest, Header: cloneHeader(header.Header)}, nil
}

func (m *PurgeTransport) State(ctx context.Context, key string) (retention.PurgeState, error) {
	entry, err := m.Blobs.State().Get(ctx, key)
	if err != nil {
		return retention.PurgeState{}, err
	}
	return retention.PurgeState{Value: entry.Value, Revision: entry.Revision}, nil
}

func (m *PurgeTransport) PutState(ctx context.Context, key string, value []byte) error {
	_, err := m.Blobs.State().Put(ctx, key, value)
	return err
}

func (m *PurgeTransport) UpdateState(ctx context.Context, key string, value []byte, revision uint64) error {
	_, err := m.Blobs.State().Update(ctx, key, value, revision)
	return err
}

func (m *PurgeTransport) DeleteState(ctx context.Context, key string, revision uint64) error {
	return m.Blobs.State().Delete(ctx, key, revision)
}

func (m *PurgeTransport) Journal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.Blobs.mu.Lock()
	defer m.Blobs.mu.Unlock()
	stream := m.Blobs.streams["WF_JRN"]
	subject := identity.JournalSubject(typ, id)
	sequences := make([]uint64, 0, len(stream.messages))
	for sequence, message := range stream.messages {
		if message.Subject == subject {
			sequences = append(sequences, sequence)
		}
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	records := make([]journal.Record, 0, len(sequences))
	for _, sequence := range sequences {
		var entry journal.Entry
		if err := json.Unmarshal(stream.messages[sequence].Data, &entry); err != nil {
			return nil, err
		}
		records = append(records, journal.Record{Entry: entry, Sequence: sequence})
	}
	m.Blobs.event(TransportEvent{Operation: "purge_journal_read", Subject: subject, Sequence: uint64(len(records)), Outcome: "ok"})
	return records, nil
}

func (m *PurgeTransport) HasFallbackTimers(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	m.schedule.RecordTransport(TransportEvent{Operation: "purge_has_timers", Outcome: strconv.FormatBool(m.fallbackTimers), AtMillis: m.schedule.NowMillis()})
	return m.fallbackTimers, nil
}

func (m *PurgeTransport) PurgeSubject(ctx context.Context, streamName, subject string, before uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if streamName == "WF_TIMER" && !m.fallbackTimers {
		return jetstream.ErrStreamNotFound
	}
	m.Blobs.mu.Lock()
	defer m.Blobs.mu.Unlock()
	stream := m.Blobs.streams[streamName]
	if stream == nil {
		return jetstream.ErrStreamNotFound
	}
	fault := m.takeFault("purge_" + streamName)
	event := TransportEvent{Operation: "purge_subject", Subject: streamName + ":" + subject, Expected: before}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.Blobs.event(event)
		return ErrTransportLost
	}
	var removed uint64
	for sequence, message := range stream.messages {
		if before != 0 && sequence >= before {
			continue
		}
		if subject == message.Subject || strings.HasSuffix(subject, "*") && strings.HasPrefix(message.Subject, strings.TrimSuffix(subject, "*")) {
			delete(stream.messages, sequence)
			removed++
		}
	}
	event.Sequence = removed
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.Blobs.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.Blobs.event(event)
	return nil
}

func (m *PurgeTransport) PublishPurge(ctx context.Context, typ, id string, invSeq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if invSeq == 0 {
		return fmt.Errorf("purge event requires an invocation sequence")
	}
	subject := "wf.purge." + typ + "." + id
	dedup := fmt.Sprintf("purge:%s:%s:%d", typ, id, invSeq)
	m.Blobs.mu.Lock()
	defer m.Blobs.mu.Unlock()
	fault := m.takeFault("publish_purge")
	event := TransportEvent{Operation: "purge_event_publish", Subject: subject, DataSHA256: digest([]byte(strconv.FormatUint(invSeq, 10)))}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.Blobs.event(event)
		return ErrTransportLost
	}
	if prior := m.purgeIDs[dedup]; prior != 0 {
		event.Sequence = prior
		event.Outcome = "duplicate"
		m.Blobs.event(event)
		return nil
	}
	stream := m.Blobs.streams["WF_PURGE"]
	stream.last++
	stream.messages[stream.last] = retention.BlobSweepMessage{Subject: subject, Data: []byte(strconv.FormatUint(invSeq, 10))}
	m.purgeIDs[dedup] = stream.last
	event.Sequence = stream.last
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.Blobs.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.Blobs.event(event)
	return nil
}

func (m *PurgeTransport) Now() time.Time { return time.UnixMilli(m.schedule.NowMillis()).UTC() }

func (m *PurgeTransport) PurgeEventCount() int {
	m.Blobs.mu.Lock()
	defer m.Blobs.mu.Unlock()
	return len(m.Blobs.streams["WF_PURGE"].messages)
}
