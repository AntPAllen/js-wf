package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"js-wf/client"
	"js-wf/identity"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SignalFault string

const (
	SignalDropBeforeCommit   SignalFault = "drop_before_commit"
	SignalLoseAckAfterCommit SignalFault = "lose_ack_after_commit"
)

type signalDedupEntry struct {
	sequence uint64
	atMillis int64
}

func (m *SignalTransport) QueueSignalFault(fault SignalFault) error {
	if fault != SignalDropBeforeCommit && fault != SignalLoseAckAfterCommit {
		return fmt.Errorf("invalid signal fault %q", fault)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, fault)
	return nil
}

func (m *SignalTransport) takeSignalFault() SignalFault {
	if len(m.faults) == 0 {
		return ""
	}
	fault := m.faults[0]
	m.faults = m.faults[1:]
	return fault
}

func (m *SignalTransport) SetState(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[key] = append([]byte(nil), value...)
	m.event(TransportEvent{Operation: "set_state", Subject: key, DataSHA256: digest(value), Outcome: "ok"})
}

func (m *SignalTransport) StateValue(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.state[key]
	if !exists {
		m.event(TransportEvent{Operation: "state_get", Subject: key, Outcome: "not_found"})
		return nil, jetstream.ErrKeyNotFound
	}
	m.event(TransportEvent{Operation: "state_get", Subject: key, DataSHA256: digest(value), Outcome: "ok"})
	return append([]byte(nil), value...), nil
}

func (m *SignalTransport) LastJournal(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := subject
	if len(subject) >= len("wf.jrn.") && subject[:len("wf.jrn.")] == "wf.jrn." {
		key = subject[len("wf.jrn."):]
	}
	records := m.journals[key]
	if len(records) == 0 {
		m.event(TransportEvent{Operation: "last_journal", Subject: subject, Outcome: "not_found"})
		return nil, jetstream.ErrMsgNotFound
	}
	data, err := json.Marshal(records[len(records)-1].Entry)
	if err != nil {
		return nil, err
	}
	sequence := records[len(records)-1].Sequence
	if sequence == 0 {
		sequence = uint64(len(records))
	}
	m.event(TransportEvent{Operation: "last_journal", Subject: subject, Sequence: sequence, DataSHA256: digest(data), Outcome: "ok"})
	return &jetstream.RawStreamMsg{Subject: subject, Sequence: sequence, Data: data}, nil
}

func (m *SignalTransport) PutSignalBlob(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.StartTransport.mu.Lock()
	defer m.StartTransport.mu.Unlock()
	m.objects[key] = append([]byte(nil), data...)
	m.event(TransportEvent{Operation: "put_signal_blob", Subject: key, DataSHA256: digest(data), Outcome: "ok"})
	return nil
}

func (m *SignalTransport) PublishSignal(ctx context.Context, message *nats.Msg, messageID string) (client.SignalPublishAck, error) {
	if err := ctx.Err(); err != nil {
		return client.SignalPublishAck{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "publish_signal", Subject: message.Subject, DataSHA256: digest(message.Data)}
	fault := m.takeSignalFault()
	if fault == SignalDropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return client.SignalPublishAck{}, ErrTransportLost
	}
	if previous, exists := m.dedup[messageID]; exists && m.schedule.NowMillis()-previous.atMillis < m.window {
		ack := client.SignalPublishAck{Sequence: previous.sequence, Duplicate: true}
		event.Sequence = ack.Sequence
		if fault == SignalLoseAckAfterCommit {
			event.Outcome = string(fault)
			m.event(event)
			return client.SignalPublishAck{}, ErrTransportLost
		}
		event.Outcome = "duplicate"
		m.event(event)
		return ack, nil
	}
	sequence := m.commitSignal(message)
	m.dedup[messageID] = signalDedupEntry{sequence: sequence, atMillis: m.schedule.NowMillis()}
	event.Sequence = sequence
	if fault == SignalLoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return client.SignalPublishAck{}, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return client.SignalPublishAck{Sequence: sequence}, nil
}

func (m *SignalTransport) SignalBySequence(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	return m.GetSignal(ctx, sequence)
}

// A retained signal is tied to the invocation generation in its header.
func (m *SignalTransport) SignalGeneration(sequence uint64) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.signals[sequence].Header.Get("Wf-Inv-Seq")
}

func (m *SignalTransport) SignalFor(typ, id, name string) []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	subject := "wf.sig." + identity.Key(typ, id) + "." + name
	out := []Message{}
	for sequence, message := range m.signals {
		if message.Subject == subject {
			out = append(out, Message{Subject: subject, Sequence: sequence, Data: append([]byte(nil), message.Data...)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out
}
