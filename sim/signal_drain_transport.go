package sim

import (
	"context"
	"strings"

	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

var _ worker.SignalDrainPort = (*SignalTransport)(nil)

func (m *SignalTransport) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := strings.TrimSuffix(subject, "*")
	for sequence := from; sequence <= m.sequence && sequence != 0; sequence++ {
		message, exists := m.signals[sequence]
		if !exists || !strings.HasPrefix(message.Subject, prefix) {
			continue
		}
		suffix := message.Subject[len(prefix):]
		if suffix == "" || strings.Contains(suffix, ".") {
			continue
		}
		m.event(TransportEvent{Operation: "next_signal", Subject: subject, Expected: from, Sequence: sequence, DataSHA256: digest(message.Data), Outcome: "ok"})
		message.Header = cloneHeader(message.Header)
		message.Data = append([]byte(nil), message.Data...)
		return &message, nil
	}
	m.event(TransportEvent{Operation: "next_signal", Subject: subject, Expected: from, Outcome: "not_found"})
	return nil, jetstream.ErrMsgNotFound
}

func (m *SignalTransport) SignalBlob(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.StartTransport.mu.Lock()
	defer m.StartTransport.mu.Unlock()
	value, exists := m.objects[key]
	if !exists {
		m.event(TransportEvent{Operation: "get_signal_blob", Subject: key, Outcome: "not_found"})
		return nil, jetstream.ErrObjectNotFound
	}
	m.event(TransportEvent{Operation: "get_signal_blob", Subject: key, DataSHA256: digest(value), Outcome: "ok"})
	return append([]byte(nil), value...), nil
}
