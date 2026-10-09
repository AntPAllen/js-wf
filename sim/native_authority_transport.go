package sim

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
)

// NativeAuthorityTransport models the SDK boundary used by NativeAuthority.
// Conditional subject sequences are physical; logical revisions live in opaque
// stored bytes and are interpreted by the production adapter. This is a single
// durable store model, not a model of NATS replication, routing or fsync.
// Embedded interfaces leave unsupported SDK calls outside this narrow model.
type NativeAuthorityTransport struct {
	jetstream.JetStream
	mu               sync.Mutex
	schedule         *Scheduler
	config           jetstream.StreamConfig
	messages         map[string]*jetstream.RawStreamMsg
	sequence         uint64
	beforeMutation   func(context.Context) error
	mutationFault    AppendFault
	mutationAttempts int
}

type nativeAuthorityStream struct {
	jetstream.Stream
	model *NativeAuthorityTransport
}

func NewNativeAuthorityTransport(schedule *Scheduler) *NativeAuthorityTransport {
	return &NativeAuthorityTransport{schedule: schedule, config: graphpublication.AuthorityStreamConfig("MODEL_AUTHORITY", "model.authority", 1), messages: map[string]*jetstream.RawStreamMsg{}}
}

func (m *NativeAuthorityTransport) Open(ctx context.Context) (*graphpublication.NativeAuthority, error) {
	return graphpublication.OpenNativeAuthority(ctx, m, m.config.Name, "model.authority")
}

func (m *NativeAuthorityTransport) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name != m.config.Name {
		return nil, jetstream.ErrStreamNotFound
	}
	return &nativeAuthorityStream{model: m}, nil
}

func (s *nativeAuthorityStream) Info(ctx context.Context, _ ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config := s.model.config
	config.Subjects = append([]string(nil), config.Subjects...)
	return &jetstream.StreamInfo{Config: config}, nil
}

func (s *nativeAuthorityStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m := s.model
	m.mu.Lock()
	defer m.mu.Unlock()
	message := m.messages[subject]
	if message == nil {
		m.event("get", subject, 0, 0, nil, "absent")
		return nil, jetstream.ErrMsgNotFound
	}
	copyMessage := *message
	copyMessage.Data = append([]byte(nil), message.Data...)
	copyMessage.Header = cloneHeader(message.Header)
	m.event("get", subject, 0, message.Sequence, message.Data, "ok")
	return &copyMessage, nil
}

// BeforeMutation runs a single callback at the publication boundary, before
// the conditional write. The callback may install another boundary callback.
func (m *NativeAuthorityTransport) BeforeMutation(callback func(context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beforeMutation = callback
}

func (m *NativeAuthorityTransport) FaultNextMutation(fault AppendFault) error {
	if fault != DropBeforeCommit && fault != LoseAckAfterCommit {
		return fmt.Errorf("unsupported native mutation fault %q", fault)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mutationFault != "" {
		return fmt.Errorf("native mutation fault already queued")
	}
	m.mutationFault = fault
	return nil
}

func (m *NativeAuthorityTransport) MutationAttempts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mutationAttempts
}

func (m *NativeAuthorityTransport) PublishMsg(ctx context.Context, message *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	expected, err := strconv.ParseUint(message.Header.Get(jetstream.ExpectedLastSubjSeqHeader), 10, 64)
	if err != nil || message.Header.Get(jetstream.ExpectedStreamHeader) != m.config.Name {
		return nil, fmt.Errorf("invalid modeled conditional authority publication")
	}
	witness := message.Header.Get("Wf-Authority-Read-Witness") == "1"
	m.mu.Lock()
	var callback func(context.Context) error
	if !witness {
		m.mutationAttempts++
		callback = m.beforeMutation
		m.beforeMutation = nil
	}
	m.mu.Unlock()
	if callback != nil {
		if err := callback(ctx); err != nil {
			return nil, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	operation := "mutation"
	if witness {
		operation = "witness"
	}
	fault := AppendFault("")
	if !witness {
		fault = m.mutationFault
		m.mutationFault = ""
	}
	if fault == DropBeforeCommit {
		m.event(operation, message.Subject, expected, 0, message.Data, "dropped")
		return nil, nats.ErrTimeout
	}
	actual := uint64(0)
	if previous := m.messages[message.Subject]; previous != nil {
		actual = previous.Sequence
	}
	if actual != expected {
		m.event(operation, message.Subject, expected, actual, message.Data, "conflict")
		return nil, &jetstream.APIError{Code: 400, ErrorCode: jetstream.JSErrCodeStreamWrongLastSequence, Description: "wrong last subject sequence"}
	}
	m.sequence++
	m.messages[message.Subject] = &jetstream.RawStreamMsg{Subject: message.Subject, Sequence: m.sequence, Data: append([]byte(nil), message.Data...), Header: cloneHeader(message.Header), Time: time.UnixMilli(m.schedule.NowMillis()).UTC()}
	if fault == LoseAckAfterCommit {
		m.event(operation, message.Subject, expected, m.sequence, message.Data, "committed_unknown")
		return nil, nats.ErrTimeout
	}
	m.event(operation, message.Subject, expected, m.sequence, message.Data, "ok")
	return &jetstream.PubAck{Stream: m.config.Name, Sequence: m.sequence}, nil
}

func (m *NativeAuthorityTransport) event(operation, subject string, expected, sequence uint64, data []byte, outcome string) {
	digest := ""
	if data != nil {
		digest = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	m.schedule.RecordTransport(TransportEvent{AtMillis: m.schedule.NowMillis(), Operation: "native_authority_" + operation, Subject: subject, Expected: expected, Sequence: sequence, DataSHA256: digest, Outcome: outcome})
}
