package worker

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// SignalDrainPort contains the retained reads used while a worker journals
// pending signals. NextSignal returns the first matching message at or after
// from, matching JetStream's subject-filtered next-message lookup.
type SignalDrainPort interface {
	LastSignalSequence(context.Context) (uint64, error)
	NextSignal(context.Context, uint64, string) (*jetstream.RawStreamMsg, error)
	SignalBlob(context.Context, string) ([]byte, error)
}

type jetStreamSignalDrainPort struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func NewSignalDrainPort(js jetstream.JetStream) SignalDrainPort {
	return &jetStreamSignalDrainPort{js: js}
}

func (p *jetStreamSignalDrainPort) signalStream(ctx context.Context) (jetstream.Stream, error) {
	if p.stream != nil {
		return p.stream, nil
	}
	stream, err := p.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return nil, err
	}
	p.stream = stream
	return stream, nil
}

func (p *jetStreamSignalDrainPort) LastSignalSequence(ctx context.Context) (uint64, error) {
	stream, err := p.signalStream(ctx)
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (p *jetStreamSignalDrainPort) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.signalStream(ctx)
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, from, jetstream.WithGetMsgSubject(subject))
}

func (p *jetStreamSignalDrainPort) SignalBlob(ctx context.Context, key string) ([]byte, error) {
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return nil, err
	}
	return objects.GetBytes(ctx, key)
}
