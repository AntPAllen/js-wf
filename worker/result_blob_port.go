package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// ErrResultBlobUnknown means a result object write may have committed and
// should be retried with the same content-addressed name.
var ErrResultBlobUnknown = errors.New("result blob write outcome unknown")

// ErrResultBlobUnavailable means a recorded result object could not be read.
// The worker retries the delivery without altering the completed step.
var ErrResultBlobUnavailable = errors.New("recorded result blob unavailable")

// ResultBlobPort contains the Object Store calls used for step and terminal
// results. The worker selects the object name and verifies its content hash.
type ResultBlobPort interface {
	PutBytes(context.Context, string, []byte) error
	GetBytes(context.Context, string) ([]byte, error)
}

// WithResultBlobPort supplies the object transport for step and terminal results.
// Nil is rejected. Journal and runtime-frame reads use their separate transport.
func WithResultBlobPort(port ResultBlobPort) Option {
	return func(w *Worker) error {
		if port == nil {
			return fmt.Errorf("nil result blob port")
		}
		w.resultBlobPort = port
		return nil
	}
}

type jetStreamResultBlobPort struct{ js jetstream.JetStream }

func NewResultBlobPort(js jetstream.JetStream) ResultBlobPort {
	return jetStreamResultBlobPort{js: js}
}

func (p jetStreamResultBlobPort) PutBytes(ctx context.Context, name string, data []byte) error {
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return fmt.Errorf("%w: open object store: %v", ErrResultBlobUnknown, err)
	}
	if _, err := objects.PutBytes(ctx, name, data); err != nil {
		return fmt.Errorf("%w: put %s: %v", ErrResultBlobUnknown, name, err)
	}
	return nil
}

func (p jetStreamResultBlobPort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return nil, fmt.Errorf("%w: open object store: %w", ErrResultBlobUnavailable, err)
	}
	data, err := objects.GetBytes(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("%w: get %s: %w", ErrResultBlobUnavailable, name, err)
	}
	return data, nil
}
