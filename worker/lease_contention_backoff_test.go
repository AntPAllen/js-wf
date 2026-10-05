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

type observedHeldLeasePort struct {
	heldLeasePort
	creates, gets int
	created       time.Time
}

func (p *observedHeldLeasePort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	p.creates++
	return p.heldLeasePort.Create(ctx, key, value)
}
func (p *observedHeldLeasePort) Get(context.Context, string) (lease.KVEntry, error) {
	p.gets++
	return lease.KVEntry{Value: []byte(`{"worker":"other","epoch":1}`), Revision: 2, Created: p.created}, nil
}

func TestHeldLeaseDispatchReportsExistingEntryWithoutExtraRead(t *testing.T) {
	for _, observe := range []bool{false, true} {
		port := &observedHeldLeasePort{created: time.Unix(20, 0)}
		w := &Worker{ID: "contender", leases: lease.NewWithKVPort(port)}
		var events []DispatchEvent
		if observe {
			w.dispatchObserver = func(e DispatchEvent) { events = append(events, e) }
		}
		msg := &heldLeaseMessage{}
		w.handle(context.Background(), msg)
		if port.creates != 1 || port.gets != 1 || msg.naks != 1 || msg.delay != heldLeaseNakDelay {
			t.Fatalf("observe=%t requests=%d/%d naks=%d delay=%s", observe, port.creates, port.gets, msg.naks, msg.delay)
		}
		if !observe {
			continue
		}
		held := 0
		for _, e := range events {
			if e.Stage != "lease_held" {
				if e.HeldLease != nil {
					t.Fatalf("metadata on unrelated event: %+v", e)
				}
				continue
			}
			held++
			o := e.HeldLease
			if o == nil || o.Key != "test.one" || o.Reason != "held_entry" || !o.EntryObserved || !o.ValueValid || o.Revision != 2 || o.Epoch != 1 || o.Worker != "other" || !o.Created.Equal(port.created) || o.ObservedAt.IsZero() {
				t.Fatalf("incorrect held event: %+v", e)
			}
		}
		if held != 1 {
			t.Fatalf("held events=%d", held)
		}
	}
}
