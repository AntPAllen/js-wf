package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type DispatchActor struct {
	Name string
	Run  func(context.Context, worker.DispatchPort) error
}

type yieldingDispatchPort struct {
	actorCtx  context.Context
	yield     YieldFunc
	transport worker.DispatchPort
}

var _ worker.DispatchPort = yieldingDispatchPort{}

func (p yieldingDispatchPort) Consumer(ctx context.Context, partition uint32) (worker.DispatchConsumer, error) {
	var consumer worker.DispatchConsumer
	var operationErr error
	if err := p.yield(ctx, "consumer_create", func() { consumer, operationErr = p.transport.Consumer(ctx, partition) }); err != nil {
		return nil, err
	}
	if operationErr != nil {
		return nil, operationErr
	}
	return yieldingDispatchConsumer{actorCtx: p.actorCtx, yield: p.yield, consumer: consumer}, nil
}

func (p yieldingDispatchPort) Wait(ctx context.Context, delay time.Duration) error {
	var operationErr error
	if err := p.yield(ctx, "dispatch_wait", func() { operationErr = p.transport.Wait(ctx, delay) }); err != nil {
		return err
	}
	return operationErr
}

type yieldingDispatchConsumer struct {
	actorCtx context.Context
	yield    YieldFunc
	consumer worker.DispatchConsumer
}

var _ worker.DispatchConsumer = yieldingDispatchConsumer{}

func (c yieldingDispatchConsumer) FetchOne(ctx context.Context) (worker.DispatchBatch, error) {
	var batch worker.DispatchBatch
	var operationErr error
	if err := c.yield(ctx, "consumer_fetch", func() { batch, operationErr = c.consumer.FetchOne(ctx) }); err != nil {
		return nil, err
	}
	if operationErr != nil {
		return nil, operationErr
	}
	messages := make([]jetstream.Msg, 0, 1)
	for message := range batch.Messages() {
		messages = append(messages, yieldingDispatchMsg{Msg: message, ctx: c.actorCtx, yield: c.yield})
	}
	channel := make(chan jetstream.Msg, len(messages))
	for _, message := range messages {
		channel <- message
	}
	close(channel)
	return yieldingDispatchBatch{messages: channel, err: batch.Error()}, nil
}

func (c yieldingDispatchConsumer) Info(ctx context.Context) (uint64, int, error) {
	var pending uint64
	var ackPending int
	var operationErr error
	if err := c.yield(ctx, "consumer_info", func() { pending, ackPending, operationErr = c.consumer.Info(ctx) }); err != nil {
		return 0, 0, err
	}
	return pending, ackPending, operationErr
}

type yieldingDispatchBatch struct {
	messages <-chan jetstream.Msg
	err      error
}

func (b yieldingDispatchBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b yieldingDispatchBatch) Error() error                   { return b.err }

type yieldingDispatchMsg struct {
	jetstream.Msg
	ctx   context.Context
	yield YieldFunc
}

func (m yieldingDispatchMsg) respond(action string, work func() error) error {
	var operationErr error
	if err := m.yield(m.ctx, action, func() { operationErr = work() }); err != nil {
		return err
	}
	return operationErr
}

func (m yieldingDispatchMsg) Ack() error { return m.respond("consumer_ack", m.Msg.Ack) }
func (m yieldingDispatchMsg) DoubleAck(ctx context.Context) error {
	return m.respond("consumer_double_ack", func() error { return m.Msg.DoubleAck(ctx) })
}
func (m yieldingDispatchMsg) Nak() error { return m.respond("consumer_nak", m.Msg.Nak) }
func (m yieldingDispatchMsg) NakWithDelay(delay time.Duration) error {
	return m.respond("consumer_nak_delay", func() error { return m.Msg.NakWithDelay(delay) })
}
func (m yieldingDispatchMsg) InProgress() error {
	return m.respond("consumer_progress", m.Msg.InProgress)
}
func (m yieldingDispatchMsg) Term() error { return m.respond("consumer_term", m.Msg.Term) }
func (m yieldingDispatchMsg) TermWithReason(reason string) error {
	return m.respond("consumer_term_reason", func() error { return m.Msg.TermWithReason(reason) })
}

// RunDispatchActors schedules production partition loops at consumer calls,
// waits, and message responses. It is intended for concurrency-one loops;
// a loop that starts additional handler goroutines needs separate actor IDs.
func RunDispatchActors(ctx context.Context, schedule *Scheduler, transport worker.DispatchPort, actors []DispatchActor) (map[string]error, error) {
	if transport == nil {
		return nil, fmt.Errorf("simulation needs a dispatch transport")
	}
	cooperative := make([]CooperativeActor, 0, len(actors))
	for _, actor := range actors {
		if actor.Run == nil {
			return nil, fmt.Errorf("simulation actor %q has no run function", actor.Name)
		}
		actor := actor
		cooperative = append(cooperative, CooperativeActor{Name: actor.Name, Run: func(ctx context.Context, yield YieldFunc) error {
			return actor.Run(ctx, yieldingDispatchPort{actorCtx: ctx, yield: yield, transport: transport})
		}})
	}
	return RunCooperative(ctx, schedule, cooperative)
}
