package retention

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
)

func graphPurgeRetryable(err error) bool {
	return errors.Is(err, journal.ErrUnknown) || errors.Is(err, graphpublication.ErrConflict) || errors.Is(err, graphpublication.ErrRevoked) || errors.Is(err, lease.ErrLost) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionReconnecting) || errors.Is(err, nats.ErrNoServers) || errors.Is(err, context.DeadlineExceeded)
}
