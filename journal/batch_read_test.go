package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type noResponderReadStream struct {
	jetstream.Stream
	consumer *noResponderReadConsumer
	created  int
	deleted  int
}

func (s *noResponderReadStream) CreateConsumer(_ context.Context, _ jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.created++
	return s.consumer, nil
}

func (s *noResponderReadStream) DeleteConsumer(_ context.Context, _ string) error {
	s.deleted++
	return nil
}

func (s *noResponderReadStream) GetMsg(_ context.Context, _ uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return nil, jetstream.ErrMsgNotFound
}

type noResponderReadConsumer struct {
	jetstream.Consumer
	fetches int
	msg     jetstream.Msg
}

func (c *noResponderReadConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{Name: "read-test"}
}

func (c *noResponderReadConsumer) Fetch(_ int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.fetches++
	if c.fetches == 1 {
		return readBatch{err: nats.ErrNoResponders}, nil
	}
	messages := make(chan jetstream.Msg, 1)
	messages <- c.msg
	close(messages)
	return readBatch{messages: messages}, nil
}

type readBatch struct {
	messages <-chan jetstream.Msg
	err      error
}

func (b readBatch) Messages() <-chan jetstream.Msg {
	if b.messages != nil {
		return b.messages
	}
	empty := make(chan jetstream.Msg)
	close(empty)
	return empty
}

func (b readBatch) Error() error { return b.err }

type readMsg struct {
	jetstream.Msg
	data []byte
}

func (m readMsg) Data() []byte { return m.data }
func (m readMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 2}}, nil
}

func TestBatchReadRetriesNoResponderWithoutSkippingEntry(t *testing.T) {
	encoded, err := json.Marshal(Entry{Epoch: 1, Index: 1, Kind: StepRequested})
	if err != nil {
		t.Fatal(err)
	}
	consumer := &noResponderReadConsumer{msg: readMsg{data: encoded}}
	stream := &noResponderReadStream{consumer: consumer}
	first := []Record{{Entry: Entry{Epoch: 1, Index: 0, Kind: Started}, Sequence: 1}}
	records, tail, received, err := readLiveBatch(context.Background(), stream, "wf.jrn.test.retry", 2, first, 1)
	if err != nil || !received || len(records) != 2 || records[1].Index != 1 || tail != 2 {
		t.Fatalf("read after no responder: records=%+v tail=%d received=%t err=%v", records, tail, received, err)
	}
	if consumer.fetches != 2 || stream.created != 1 || stream.deleted != 1 {
		t.Fatalf("consumer lifecycle: fetches=%d created=%d deleted=%d", consumer.fetches, stream.created, stream.deleted)
	}
}
