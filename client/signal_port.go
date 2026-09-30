package client

import (
	"context"

	"js-wf/journal"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type SignalPublishAck struct {
	Sequence  uint64
	Duplicate bool
}

// SignalPort is the durable boundary used by Signal, Cancel, and their
// generation-aware variants. Production uses JetStream; simulations implement
// only these operations rather than the whole JetStream client interface.
type SignalPort interface {
	LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error)
	StateValue(context.Context, string) ([]byte, error)
	LastJournal(context.Context, string) (*jetstream.RawStreamMsg, error)
	PutSignalBlob(context.Context, string, []byte) error
	PublishSignal(context.Context, *nats.Msg, string) (SignalPublishAck, error)
	SignalBySequence(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	ReadJournal(context.Context, string, string) ([]journal.Record, error)
	EnqueueRun(context.Context, string, []byte, string) error
}

type jetStreamSignalPort struct{ js jetstream.JetStream }

func (p jetStreamSignalPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}

func (p jetStreamSignalPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return nil, err
	}
	entry, err := state.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (p jetStreamSignalPort) LastJournal(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_JRN")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}

func (p jetStreamSignalPort) PutSignalBlob(ctx context.Context, key string, data []byte) error {
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return err
	}
	_, err = objects.PutBytes(ctx, key, data)
	return err
}

func (p jetStreamSignalPort) PublishSignal(ctx context.Context, msg *nats.Msg, messageID string) (SignalPublishAck, error) {
	ack, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(messageID))
	if err != nil {
		return SignalPublishAck{}, err
	}
	return SignalPublishAck{Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

func (p jetStreamSignalPort) SignalBySequence(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence)
}

func (p jetStreamSignalPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	records, _, err := journal.New(p.js).Read(ctx, typ, id)
	return records, err
}

func (p jetStreamSignalPort) EnqueueRun(ctx context.Context, subject string, data []byte, messageID string) error {
	return (&jetStreamStartPort{js: p.js}).EnqueueRun(ctx, subject, data, messageID)
}
