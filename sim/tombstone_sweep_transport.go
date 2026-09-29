package sim

import (
	"context"

	"js-wf/retention"
)

// TombstoneSweepTransport connects production retention decisions to modeled
// invocation lookup and revision-CAS deletion.
type TombstoneSweepTransport struct {
	Invocations *StartTransport
	State       *KVTransport
}

var _ retention.TombstoneSweepPort = TombstoneSweepTransport{}

func (m TombstoneSweepTransport) StateKeys(ctx context.Context) ([]string, error) {
	return m.State.Keys(ctx)
}

func (m TombstoneSweepTransport) StateValue(ctx context.Context, key string) ([]byte, uint64, error) {
	entry, err := m.State.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return entry.Value, entry.Revision, nil
}

func (m TombstoneSweepTransport) CurrentInvocation(ctx context.Context, subject string) (uint64, error) {
	message, err := m.Invocations.LastInvocation(ctx, subject)
	if err != nil {
		return 0, err
	}
	return message.Sequence, nil
}

func (m TombstoneSweepTransport) DeleteState(ctx context.Context, key string, revision uint64) error {
	return m.State.Delete(ctx, key, revision)
}
