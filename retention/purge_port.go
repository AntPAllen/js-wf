package retention

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"js-wf/journal"
	"js-wf/lease"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type PurgeInvocation struct {
	Sequence uint64
	Header   nats.Header
}

type PurgeState struct {
	Value    []byte
	Revision uint64
}

type PurgeLease interface {
	Renew(context.Context) error
	Release(context.Context) error
}

// PurgePort contains the durable operations used by production retention.
// The model can inject faults at these edges without reimplementing purge
// ordering, generation checks, or crash recovery.
type PurgePort interface {
	Acquire(context.Context, string, string) (PurgeLease, error)
	Invocation(context.Context, string) (PurgeInvocation, error)
	State(context.Context, string) (PurgeState, error)
	PutState(context.Context, string, []byte) error
	UpdateState(context.Context, string, []byte, uint64) error
	DeleteState(context.Context, string, uint64) error
	Journal(context.Context, string, string) ([]journal.Record, error)
	HasFallbackTimers(context.Context) (bool, error)
	PurgeSubject(context.Context, string, string, uint64) error
	PublishPurge(context.Context, string, string, uint64) error
	Now() time.Time
}

type jetStreamPurgePort struct {
	js      jetstream.JetStream
	leasing *lease.Store
	state   jetstream.KeyValue
	streams map[string]jetstream.Stream
}

func (p *jetStreamPurgePort) Acquire(ctx context.Context, typ, id string) (PurgeLease, error) {
	return p.leasing.Acquire(ctx, typ, id, "retention")
}

func (p *jetStreamPurgePort) stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if stream := p.streams[name]; stream != nil {
		return stream, nil
	}
	stream, err := p.js.Stream(ctx, name)
	if err == nil {
		p.streams[name] = stream
	}
	return stream, err
}

func (p *jetStreamPurgePort) stateKV(ctx context.Context) (jetstream.KeyValue, error) {
	if p.state != nil {
		return p.state, nil
	}
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err == nil {
		p.state = state
	}
	return state, err
}

func (p *jetStreamPurgePort) Invocation(ctx context.Context, subject string) (PurgeInvocation, error) {
	stream, err := p.stream(ctx, "WF_INV")
	if err != nil {
		return PurgeInvocation{}, err
	}
	message, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return PurgeInvocation{}, err
	}
	return PurgeInvocation{Sequence: message.Sequence, Header: message.Header}, nil
}

func (p *jetStreamPurgePort) State(ctx context.Context, key string) (PurgeState, error) {
	state, err := p.stateKV(ctx)
	if err != nil {
		return PurgeState{}, err
	}
	entry, err := state.Get(ctx, key)
	if err != nil {
		return PurgeState{}, err
	}
	return PurgeState{Value: entry.Value(), Revision: entry.Revision()}, nil
}

func (p *jetStreamPurgePort) PutState(ctx context.Context, key string, value []byte) error {
	state, err := p.stateKV(ctx)
	if err != nil {
		return err
	}
	_, err = state.Put(ctx, key, value)
	return err
}

func (p *jetStreamPurgePort) UpdateState(ctx context.Context, key string, value []byte, revision uint64) error {
	state, err := p.stateKV(ctx)
	if err != nil {
		return err
	}
	_, err = state.Update(ctx, key, value, revision)
	return err
}

func (p *jetStreamPurgePort) DeleteState(ctx context.Context, key string, revision uint64) error {
	state, err := p.stateKV(ctx)
	if err != nil {
		return err
	}
	return state.Delete(ctx, key, jetstream.LastRevision(revision))
}

func (p *jetStreamPurgePort) Journal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	records, _, err := journal.New(p.js).Read(ctx, typ, id)
	return records, err
}

func (p *jetStreamPurgePort) HasFallbackTimers(ctx context.Context) (bool, error) {
	_, err := p.stream(ctx, "WF_TIMER")
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (p *jetStreamPurgePort) PurgeSubject(ctx context.Context, streamName, subject string, before uint64) error {
	stream, err := p.stream(ctx, streamName)
	if err != nil {
		return err
	}
	options := []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject(subject)}
	if before != 0 {
		options = append(options, jetstream.WithPurgeSequence(before))
	}
	return stream.Purge(ctx, options...)
}

func (p *jetStreamPurgePort) PublishPurge(ctx context.Context, typ, id string, invSeq uint64) error {
	// Publish before deleting WF_INV so a committed retirement has a durable
	// visibility event. Retry uses a generation-scoped message ID.
	if invSeq == 0 {
		return fmt.Errorf("purge event requires an invocation sequence")
	}
	_, err := p.js.Publish(ctx, "wf.purge."+typ+"."+id, []byte(strconv.FormatUint(invSeq, 10)),
		jetstream.WithMsgID(fmt.Sprintf("purge:%s:%s:%d", typ, id, invSeq)))
	return err
}

func (*jetStreamPurgePort) Now() time.Time { return time.Now().UTC() }
