package sim

import (
	"context"
	"fmt"
	"sync"
	"time"

	"js-wf/lease"

	"github.com/nats-io/nats.go/jetstream"
)

type KVFaultKind string

const (
	KVDropBeforeCommit   KVFaultKind = "drop_before_commit"
	KVLoseAckAfterCommit KVFaultKind = "lose_ack_after_commit"
)

type KVFault struct {
	Operation string
	Kind      KVFaultKind
}

type kvItem struct {
	value    []byte
	revision uint64
	created  time.Time
}

// KVTransport models the revision CAS and expiry behavior used by workflow
// leases. Its revisions are global across keys, including delete markers.
type KVTransport struct {
	mu       sync.Mutex
	schedule *Scheduler
	ttl      time.Duration
	revision uint64
	items    map[string]kvItem
	faults   []KVFault
}

var _ lease.KVPort = (*KVTransport)(nil)

func NewKVTransport(schedule *Scheduler, ttl time.Duration) *KVTransport {
	return &KVTransport{schedule: schedule, ttl: ttl, items: map[string]kvItem{}}
}

func (m *KVTransport) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now()
}

func (m *KVTransport) QueueFault(f KVFault) error {
	if f.Operation != "create" && f.Operation != "update" && f.Operation != "delete" {
		return fmt.Errorf("invalid KV fault operation %q", f.Operation)
	}
	if f.Kind != KVDropBeforeCommit && f.Kind != KVLoseAckAfterCommit {
		return fmt.Errorf("invalid KV fault kind %q", f.Kind)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, f)
	return nil
}

func (m *KVTransport) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "kv_create", Subject: key, DataSHA256: digest(value)}
	fault := m.takeFault("create")
	if fault == KVDropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return 0, ErrTransportLost
	}
	if _, exists := m.current(key); exists {
		event.Outcome = "key_exists"
		m.event(event)
		return 0, jetstream.ErrKeyExists
	}
	rev := m.put(key, value)
	event.Sequence = rev
	if fault == KVLoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return 0, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return rev, nil
}

func (m *KVTransport) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	if err := ctx.Err(); err != nil {
		return lease.KVEntry{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	item, exists := m.current(key)
	if !exists {
		m.event(TransportEvent{Operation: "kv_get", Subject: key, Outcome: "not_found"})
		return lease.KVEntry{}, jetstream.ErrKeyNotFound
	}
	m.event(TransportEvent{Operation: "kv_get", Subject: key, Sequence: item.revision, DataSHA256: digest(item.value), Outcome: "ok"})
	return lease.KVEntry{Value: append([]byte(nil), item.value...), Revision: item.revision, Created: item.created}, nil
}

func (m *KVTransport) Update(ctx context.Context, key string, value []byte, expected uint64) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "kv_update", Subject: key, Expected: expected, DataSHA256: digest(value)}
	fault := m.takeFault("update")
	if fault == KVDropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return 0, ErrTransportLost
	}
	item, exists := m.current(key)
	if !exists || item.revision != expected {
		event.Outcome = "revision_mismatch"
		m.event(event)
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	rev := m.put(key, value)
	event.Sequence = rev
	if fault == KVLoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return 0, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return rev, nil
}

func (m *KVTransport) Delete(ctx context.Context, key string, expected uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "kv_delete", Subject: key, Expected: expected}
	fault := m.takeFault("delete")
	if fault == KVDropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	item, exists := m.current(key)
	if !exists || item.revision != expected {
		event.Outcome = "revision_mismatch"
		m.event(event)
		return jetstream.ErrKeyRevisionMismatch
	}
	m.revision++
	delete(m.items, key)
	event.Sequence = m.revision
	if fault == KVLoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return nil
}

func (m *KVTransport) current(key string) (kvItem, bool) {
	item, exists := m.items[key]
	if !exists {
		return kvItem{}, false
	}
	if m.ttl > 0 && !m.now().Before(item.created.Add(m.ttl)) {
		delete(m.items, key)
		return kvItem{}, false
	}
	return item, true
}

func (m *KVTransport) put(key string, value []byte) uint64 {
	m.revision++
	m.items[key] = kvItem{value: append([]byte(nil), value...), revision: m.revision, created: m.now()}
	return m.revision
}

func (m *KVTransport) takeFault(operation string) KVFaultKind {
	for i, fault := range m.faults {
		if fault.Operation == operation {
			m.faults = append(m.faults[:i], m.faults[i+1:]...)
			return fault.Kind
		}
	}
	return ""
}

func (m *KVTransport) now() time.Time {
	return time.Unix(0, 0).Add(time.Duration(m.schedule.NowMillis()) * time.Millisecond)
}

func (m *KVTransport) event(event TransportEvent) {
	event.AtMillis = m.schedule.NowMillis()
	m.schedule.trace.Transport = append(m.schedule.trace.Transport, event)
}
