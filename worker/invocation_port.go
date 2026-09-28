package worker

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// InvocationPort reads a retained invocation and its optional large input.
type InvocationPort interface {
	LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error)
	InputBlob(context.Context, string) ([]byte, error)
}

type jetStreamInvocationPort struct{ js jetstream.JetStream }

func (p jetStreamInvocationPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}

func (p jetStreamInvocationPort) InputBlob(ctx context.Context, key string) ([]byte, error) {
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return nil, err
	}
	return objects.GetBytes(ctx, key)
}
