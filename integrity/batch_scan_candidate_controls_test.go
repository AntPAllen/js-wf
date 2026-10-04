package integrity

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type candidateControlMsg struct {
	jetstream.Msg
	seq    uint64
	stream string
}

func (m candidateControlMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Stream: m.stream, Sequence: jetstream.SequencePair{Stream: m.seq}}, nil
}
func (m candidateControlMsg) Subject() string      { return "candidate.subject" }
func (m candidateControlMsg) Data() []byte         { return []byte("candidate-data") }
func (m candidateControlMsg) Headers() nats.Header { return nil }

type candidateControlBatch struct {
	messages chan jetstream.Msg
	err      error
}

func (b candidateControlBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b candidateControlBatch) Error() error                   { return b.err }

type candidateControlConsumer struct {
	jetstream.Consumer
	sequences []uint64
	stream    string
	err       error
}

func (c candidateControlConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{Name: "control"}
}
func (c candidateControlConsumer) Fetch(_ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	messages := make(chan jetstream.Msg, len(c.sequences))
	for _, seq := range c.sequences {
		messages <- candidateControlMsg{seq: seq, stream: c.stream}
	}
	close(messages)
	return candidateControlBatch{messages: messages, err: c.err}, nil
}

type candidateControlStream struct {
	jetstream.Stream
	consumer        candidateControlConsumer
	requests        []uint64
	deleted         bool
	readError       error
	wrongSequence   bool
	createNames     []string
	deleteName      string
	createLostReply bool
	createError     error
}

func (s *candidateControlStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: "CONTROL", Replicas: 3}, State: jetstream.StreamState{FirstSeq: 1, LastSeq: 6}}, nil
}
func (s *candidateControlStream) CreateConsumer(_ context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.createNames = append(s.createNames, cfg.Name)
	if !strings.HasPrefix(cfg.Name, "wf-audit-") {
		return nil, errors.New("missing stable audit consumer identity")
	}
	if s.createError != nil {
		return nil, s.createError
	}
	if s.createLostReply && len(s.createNames) == 1 {
		return nil, nats.ErrTimeout
	}
	if cfg.DeliverPolicy != jetstream.DeliverByStartSequencePolicy || cfg.OptStartSeq != 1 || cfg.AckPolicy != jetstream.AckNonePolicy || !cfg.MemoryStorage || cfg.Replicas != 3 || cfg.InactiveThreshold <= 0 {
		return nil, errors.New("incorrect consumer configuration")
	}
	return s.consumer, nil
}
func (s *candidateControlStream) DeleteConsumer(_ context.Context, name string) error {
	s.deleteName = name
	s.deleted = true
	return nil
}
func (s *candidateControlStream) GetMsg(_ context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.requests = append(s.requests, seq)
	if s.readError != nil {
		return nil, s.readError
	}
	if len(opts) != 1 {
		return nil, errors.New("missing next-message option")
	}
	if seq == 4 {
		seq = 5
	}
	if s.wrongSequence {
		return &jetstream.RawStreamMsg{Sequence: 0}, nil
	}
	return &jetstream.RawStreamMsg{Sequence: seq}, nil
}
func TestAuditBatchScanCandidateResolvesConsumerOmissionsAndTail(t *testing.T) {
	s := &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 3, 5}, stream: "CONTROL"}}
	var visited []uint64
	err := candidateBatchScan(context.Background(), s, nil, func(msg *jetstream.RawStreamMsg) error { visited = append(visited, msg.Sequence); return nil })
	if err != nil || !reflect.DeepEqual(visited, []uint64{1, 2, 3, 5, 6}) || !reflect.DeepEqual(s.requests, []uint64{2, 4, 6}) || !s.deleted {
		t.Fatalf("visited=%v reads=%v cleanup=%v err=%v", visited, s.requests, s.deleted, err)
	}
}
func TestAuditBatchScanCandidateRejectsOrderSourceAndSemanticFailures(t *testing.T) {
	sentinel := errors.New("retained invariant failed")
	for _, tc := range []struct {
		name         string
		stream       *candidateControlStream
		visitorError error
	}{
		{"duplicate", &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 1}, stream: "CONTROL"}}, nil},
		{"wrong stream", &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1}, stream: "OTHER"}}, nil},
		{"gap semantic error", &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 3}, stream: "CONTROL"}, readError: sentinel}, nil},
		{"wrong gap sequence", &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 3}, stream: "CONTROL"}, wrongSequence: true}, nil},
		{"batch semantic error", &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1}, stream: "CONTROL", err: sentinel}}, nil},
		{"visitor error", &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 2, 3}, stream: "CONTROL"}}, sentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var visited []uint64
			err := candidateBatchScan(context.Background(), tc.stream, nil, func(msg *jetstream.RawStreamMsg) error {
				visited = append(visited, msg.Sequence)
				return tc.visitorError
			})
			if err == nil || !tc.stream.deleted {
				t.Fatalf("err=%v cleanup=%v", err, tc.stream.deleted)
			}
			if tc.visitorError != nil && (!errors.Is(err, sentinel) || !reflect.DeepEqual(visited, []uint64{1})) {
				t.Fatalf("masked visitor error or continued: %v %v", visited, err)
			}
			if tc.stream.readError != nil && (len(tc.stream.requests) != 1 || !errors.Is(err, sentinel)) {
				t.Fatalf("retried/masked semantic read: %v %v", tc.stream.requests, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := &candidateControlStream{}
	if err := candidateBatchScan(ctx, s, nil, func(*jetstream.RawStreamMsg) error { t.Fatal("visit after cancel"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAuditBatchScanCandidateCreateRetryKeepsIdentityAndCleansUncertainCreate(t *testing.T) {
	s := &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 2, 3, 5, 6}, stream: "CONTROL"}, createLostReply: true}
	if err := candidateBatchScan(context.Background(), s, nil, func(*jetstream.RawStreamMsg) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(s.createNames) != 2 || s.createNames[0] != s.createNames[1] || s.deleteName != s.createNames[0] {
		t.Fatalf("create identities=%v cleanup=%s", s.createNames, s.deleteName)
	}
	s = &candidateControlStream{createError: nats.ErrTimeout}
	if err := candidateBatchScan(context.Background(), s, nil, func(*jetstream.RawStreamMsg) error { t.Fatal("visit after failed create"); return nil }); !errors.Is(err, nats.ErrTimeout) {
		t.Fatal(err)
	}
	if len(s.createNames) != 3 || s.createNames[0] != s.createNames[1] || s.createNames[1] != s.createNames[2] || s.deleteName != s.createNames[0] {
		t.Fatalf("uncertain create identities=%v cleanup=%s", s.createNames, s.deleteName)
	}
}

func TestAuditBatchScanCandidateTransportFallbackResumesUnvisitedTail(t *testing.T) {
	for _, interrupted := range []error{nats.ErrNoResponders, nats.ErrTimeout, jetstream.ErrNoStreamResponse, context.DeadlineExceeded, &jetstream.APIError{ErrorCode: 10008}} {
		s := &candidateControlStream{consumer: candidateControlConsumer{sequences: []uint64{1, 2}, stream: "CONTROL", err: interrupted}}
		var visited []uint64
		if err := candidateBatchScan(context.Background(), s, nil, func(msg *jetstream.RawStreamMsg) error { visited = append(visited, msg.Sequence); return nil }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(visited, []uint64{1, 2, 3, 5, 6}) || !reflect.DeepEqual(s.requests, []uint64{3, 4, 6}) || !s.deleted {
			t.Fatalf("tail not recovered: visited=%v reads=%v cleanup=%v", visited, s.requests, s.deleted)
		}
	}
}
