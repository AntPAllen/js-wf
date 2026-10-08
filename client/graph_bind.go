package client

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

// WithGraphJournal copies this client's configuration and binds graph decisions
// to store, preserving its transports and observer. The original is unchanged.
// Workers use this to ensure parent notifications match their graph mode.
func (c *Client) WithGraphJournal(store *journal.GraphStore) (*Client, error) {
	if c == nil || store == nil || c.startPort == nil || c.js == nil && c.signalPort == nil {
		return nil, fmt.Errorf("invalid graph client binding")
	}
	bound := *c
	bound.graphJournal = store
	if bound.graphResultPort == nil {
		bound.graphResultPort = graphBoundResultPort{signal: c.signalOperations(), wait: c.startOperations().Wait}
	}
	return &bound, nil
}

type graphBoundResultPort struct {
	signal SignalPort
	wait   func(context.Context, time.Duration) error
}

func (p graphBoundResultPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	return p.signal.LastInvocation(ctx, subject)
}
func (p graphBoundResultPort) State(ctx context.Context, key string) ([]byte, error) {
	return p.signal.StateValue(ctx, key)
}
func (p graphBoundResultPort) Wait(ctx context.Context, delay time.Duration) error {
	return p.wait(ctx, delay)
}
