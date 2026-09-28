package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/client"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type StartActor struct {
	Name string
	Run  func(context.Context, client.StartPort) error
}

type yieldingStartPort struct {
	yield     YieldFunc
	transport client.StartPort
}

var _ client.StartPort = yieldingStartPort{}

func (p yieldingStartPort) PublishInvocation(ctx context.Context, msg *nats.Msg) (uint64, error) {
	var sequence uint64
	var operationErr error
	copyMsg := &nats.Msg{Subject: msg.Subject, Header: cloneHeader(msg.Header), Data: append([]byte(nil), msg.Data...)}
	if err := p.yield(ctx, "publish_invocation", func() { sequence, operationErr = p.transport.PublishInvocation(ctx, copyMsg) }); err != nil {
		return 0, err
	}
	return sequence, operationErr
}

func (p yieldingStartPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "last_invocation", func() { message, operationErr = p.transport.LastInvocation(ctx, subject) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingStartPort) PutInput(ctx context.Context, key string, input []byte) error {
	var operationErr error
	data := append([]byte(nil), input...)
	if err := p.yield(ctx, "put_input", func() { operationErr = p.transport.PutInput(ctx, key, data) }); err != nil {
		return err
	}
	return operationErr
}

func (p yieldingStartPort) EnqueueRun(ctx context.Context, subject string, data []byte, dedupID string) error {
	var operationErr error
	copyData := append([]byte(nil), data...)
	if err := p.yield(ctx, "enqueue_run", func() { operationErr = p.transport.EnqueueRun(ctx, subject, copyData, dedupID) }); err != nil {
		return err
	}
	return operationErr
}

func (p yieldingStartPort) Wait(ctx context.Context, delay time.Duration) error {
	var operationErr error
	if err := p.yield(ctx, "wait", func() { operationErr = p.transport.Wait(ctx, delay) }); err != nil {
		return err
	}
	return operationErr
}

// RunStartActors schedules production client starts at each transport call.
func RunStartActors(ctx context.Context, schedule *Scheduler, transport client.StartPort, actors []StartActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs a start transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, yieldingStartPort{yield: yield, transport: transport})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}

type StartAndScanActor struct {
	Name string
	Run  func(context.Context, client.StartPort, reconcile.StartScanPort) error
}

type yieldingStartScanPort struct {
	yield     YieldFunc
	transport reconcile.StartScanPort
}

var _ reconcile.StartScanPort = yieldingStartScanPort{}

func (p yieldingStartScanPort) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	var message *jetstream.RawStreamMsg
	var operationErr error
	if err := p.yield(ctx, "get_invocation", func() { message, operationErr = p.transport.GetInvocation(ctx, sequence) }); err != nil {
		return nil, err
	}
	return message, operationErr
}

func (p yieldingStartScanPort) LastInvocationSequence(ctx context.Context) (uint64, error) {
	var sequence uint64
	var operationErr error
	if err := p.yield(ctx, "invocation_stream_info", func() { sequence, operationErr = p.transport.LastInvocationSequence(ctx) }); err != nil {
		return 0, err
	}
	return sequence, operationErr
}

func (p yieldingStartScanPort) JournalExists(ctx context.Context, subject string) (bool, error) {
	var exists bool
	var operationErr error
	if err := p.yield(ctx, "journal_exists", func() { exists, operationErr = p.transport.JournalExists(ctx, subject) }); err != nil {
		return false, err
	}
	return exists, operationErr
}

func (p yieldingStartScanPort) EnqueueStart(ctx context.Context, typ, id string, sequence uint64) error {
	var operationErr error
	if err := p.yield(ctx, "enqueue_start", func() { operationErr = p.transport.EnqueueStart(ctx, typ, id, sequence) }); err != nil {
		return err
	}
	return operationErr
}

// RunStartAndScanActors interleaves client and reconciler transport calls.
func RunStartAndScanActors(ctx context.Context, schedule *Scheduler, transport *StartTransport, actors []StartAndScanActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs a start transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, yieldingStartPort{yield: yield, transport: transport}, yieldingStartScanPort{yield: yield, transport: transport})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
