package sim

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// ResultBlobTransport models the Object Store calls used by production worker
// step and terminal result decisions.
type ResultBlobTransport struct {
	mu              sync.Mutex
	schedule        *Scheduler
	objects         map[string][]byte
	faults          []AppendFault
	namedPrefix     string
	namedFault      AppendFault
	readFaultPrefix string
}

// QueueReadFault makes the next matching object read temporarily unavailable.
func (m *ResultBlobTransport) QueueReadFault(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("empty result blob read fault prefix")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readFaultPrefix = prefix
	return nil
}

// QueueNamedFault faults the next write whose content-addressed name has prefix.
func (m *ResultBlobTransport) QueueNamedFault(prefix string, fault AppendFault) error {
	if prefix == "" || fault != DropBeforeCommit && fault != LoseAckAfterCommit {
		return fmt.Errorf("invalid named result blob fault")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.namedPrefix, m.namedFault = prefix, fault
	return nil
}

var _ worker.ResultBlobPort = (*ResultBlobTransport)(nil)

func NewResultBlobTransport(schedule *Scheduler) *ResultBlobTransport {
	return &ResultBlobTransport{schedule: schedule, objects: make(map[string][]byte)}
}

func (m *ResultBlobTransport) QueueFault(fault AppendFault) error {
	if fault != DropBeforeCommit && fault != LoseAckAfterCommit {
		return fmt.Errorf("invalid result blob fault %q", fault)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, fault)
	return nil
}

func (m *ResultBlobTransport) PutBytes(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var fault AppendFault
	if m.namedFault != "" && strings.HasPrefix(name, m.namedPrefix) {
		fault = m.namedFault
		m.namedFault, m.namedPrefix = "", ""
	} else if len(m.faults) > 0 {
		fault, m.faults = m.faults[0], m.faults[1:]
	}
	event := TransportEvent{Operation: "put_result_blob", Subject: name, DataSHA256: digest(data), AtMillis: m.schedule.NowMillis()}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.schedule.RecordTransport(event)
		return fmt.Errorf("%w: %w", worker.ErrResultBlobUnknown, ErrTransportLost)
	}
	m.objects[name] = append([]byte(nil), data...)
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.schedule.RecordTransport(event)
		return fmt.Errorf("%w: %w", worker.ErrResultBlobUnknown, ErrTransportLost)
	}
	event.Outcome = "ok"
	m.schedule.RecordTransport(event)
	return nil
}

func (m *ResultBlobTransport) GetBytes(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "get_result_blob", Subject: name, AtMillis: m.schedule.NowMillis()}
	if m.readFaultPrefix != "" && strings.HasPrefix(name, m.readFaultPrefix) {
		m.readFaultPrefix = ""
		event.Outcome = "transient_unavailable"
		m.schedule.RecordTransport(event)
		return nil, fmt.Errorf("%w: %w", worker.ErrResultBlobUnavailable, ErrTransportLost)
	}
	data, ok := m.objects[name]
	if !ok {
		event.Outcome = "not_found"
		m.schedule.RecordTransport(event)
		return nil, fmt.Errorf("%w: %w", worker.ErrResultBlobUnavailable, jetstream.ErrObjectNotFound)
	}
	event.Outcome = "ok"
	event.DataSHA256 = digest(data)
	m.schedule.RecordTransport(event)
	return append([]byte(nil), data...), nil
}

func (m *ResultBlobTransport) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.objects)
}
