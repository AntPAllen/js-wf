package sim

import (
	"context"
	"fmt"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type ClientSignalActor struct {
	Name string
	Run  func(context.Context, client.StartPort, client.SignalPort, reconcile.SignalScanPort) error
}

type yieldingClientSignalPort struct {
	yield     YieldFunc
	transport client.SignalPort
}

var _ client.SignalPort = yieldingClientSignalPort{}

func (p yieldingClientSignalPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "signal_last_invocation", func() { message, operationErr = p.transport.LastInvocation(ctx, subject) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingClientSignalPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	var operationErr error
	if err := p.yield(ctx, "signal_state_get", func() { value, operationErr = p.transport.StateValue(ctx, key) }); err != nil {
		return nil, err
	}
	return value, operationErr
}

func (p yieldingClientSignalPort) LastJournal(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "signal_last_journal", func() { message, operationErr = p.transport.LastJournal(ctx, subject) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingClientSignalPort) PutSignalBlob(ctx context.Context, key string, data []byte) error {
	copyData := append([]byte(nil), data...)
	var operationErr error
	if err := p.yield(ctx, "put_signal_blob", func() { operationErr = p.transport.PutSignalBlob(ctx, key, copyData) }); err != nil {
		return err
	}
	return operationErr
}

func (p yieldingClientSignalPort) PublishSignal(ctx context.Context, msg *nats.Msg, messageID string) (client.SignalPublishAck, error) {
	copyMsg := &nats.Msg{Subject: msg.Subject, Header: cloneHeader(msg.Header), Data: append([]byte(nil), msg.Data...)}
	var ack client.SignalPublishAck
	var operationErr error
	if err := p.yield(ctx, "publish_signal", func() { ack, operationErr = p.transport.PublishSignal(ctx, copyMsg, messageID) }); err != nil {
		return client.SignalPublishAck{}, err
	}
	return ack, operationErr
}

func (p yieldingClientSignalPort) SignalBySequence(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "signal_by_sequence", func() { message, operationErr = p.transport.SignalBySequence(ctx, sequence) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingClientSignalPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	var records []journal.Record
	var operationErr error
	if err := p.yield(ctx, "signal_read_journal", func() { records, operationErr = p.transport.ReadJournal(ctx, typ, id) }); err != nil {
		return nil, err
	}
	return records, operationErr
}

func (p yieldingClientSignalPort) EnqueueRun(ctx context.Context, subject string, data []byte, messageID string) error {
	copyData := append([]byte(nil), data...)
	var operationErr error
	if err := p.yield(ctx, "signal_enqueue_run", func() { operationErr = p.transport.EnqueueRun(ctx, subject, copyData, messageID) }); err != nil {
		return err
	}
	return operationErr
}

// RunClientSignalActors interleaves production client and reconciler transport
// calls against one model. Each actor uses a single goroutine between yields.
func RunClientSignalActors(ctx context.Context, schedule *Scheduler, transport *SignalTransport, actors []ClientSignalActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs a signal transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx,
				yieldingStartPort{yield: yield, transport: transport},
				yieldingClientSignalPort{yield: yield, transport: transport},
				yieldingSignalScanPort{yield: yield, transport: transport})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
