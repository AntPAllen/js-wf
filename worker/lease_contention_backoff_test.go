package worker

import (
	"context"
	"testing"
	"time"

	"js-wf/lease"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type heldLeasePort struct{}

func (heldLeasePort) Create(context.Context, string, []byte) (uint64, error) {
	return 0, jetstream.ErrKeyExists
}
func (heldLeasePort) Get(context.Context, string) (lease.KVEntry, error) {
	return lease.KVEntry{Value: []byte(`{"worker":"other","epoch":1}`), Revision: 2, Created: time.Now()}, nil
}
func (heldLeasePort) Update(context.Context, string, []byte, uint64) (uint64, error) {
	panic("held lease must not be updated")
}
func (heldLeasePort) Delete(context.Context, string, uint64) error {
	panic("held lease must not be deleted")
}
func (heldLeasePort) Now() time.Time { return time.Now() }

type heldLeaseMessage struct {
	jetstream.Msg
	naks  int
	delay time.Duration
}

func (*heldLeaseMessage) Data() []byte         { return []byte("test.one") }
func (*heldLeaseMessage) Headers() nats.Header { return nats.Header{} }
func (*heldLeaseMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, nil
}
func (m *heldLeaseMessage) NakWithDelay(delay time.Duration) error {
	m.naks++
	m.delay = delay
	return nil
}

func TestHeldLeaseRunUsesBoundedRedeliveryDelay(t *testing.T) {
	w := &Worker{ID: "contender", leases: lease.NewWithKVPort(heldLeasePort{})}
	msg := &heldLeaseMessage{}
	w.handle(context.Background(), msg)
	if msg.naks != 1 || msg.delay != heldLeaseNakDelay || w.Metrics().LeaseContentions != 1 {
		t.Fatalf("held lease delivery: naks=%d delay=%s contentions=%d", msg.naks, msg.delay, w.Metrics().LeaseContentions)
	}
}
