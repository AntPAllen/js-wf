package integrity

// Rejected buffered-delivery experiment retained only for diagnostics. Native
// measurements did not establish improvement over the unbuffered callback path.
import (
	"context"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"sync"
	"time"
)

type callbackDiagnosticReader interface {
	next(context.Context) (jetstream.Msg, error)
	stop()
	stopAndJoin(context.Context) error
}

type bufferedCallbackDelivery struct {
	callbackDelivery
	budgetMu                                   sync.Mutex
	budgetChanged                              *sync.Cond
	queuedPayload, queuedRecords, payloadLimit int
}

func newCallbackDeliveryWithBuffer(c jetstream.Consumer, records, payloadBytes int) (*bufferedCallbackDelivery, error) {
	if records < 0 || records > 256 || records == 0 && payloadBytes != 0 || records > 0 && payloadBytes < 1 {
		return nil, errors.New("retained callback scan: invalid queue bounds")
	}
	d := &bufferedCallbackDelivery{callbackDelivery: callbackDelivery{messages: make(chan jetstream.Msg, records), errors: make(chan error, 1), stopped: make(chan struct{})}, payloadLimit: payloadBytes}
	d.budgetChanged = sync.NewCond(&d.budgetMu)
	cc, err := c.Consume(func(msg jetstream.Msg) {
		if !d.reserve(msg) {
			return
		}
		// Stop unblocks both queue-byte reservation and a channel send before
		// unsubscribing, so shutdown cannot strand the handler.
		select {
		case d.messages <- msg:
		case <-d.stopped:
			d.release(msg)
		}
	}, jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		select {
		case d.errors <- err:
		default:
		}
	}))
	if err != nil {
		return nil, err
	}
	d.consume, d.closed = cc, cc.Closed()
	return d, nil
}

func (d *bufferedCallbackDelivery) reserve(msg jetstream.Msg) bool {
	if d.payloadLimit == 0 {
		return true
	}
	size := len(msg.Data())
	d.budgetMu.Lock()
	defer d.budgetMu.Unlock()
	for {
		select {
		case <-d.stopped:
			return false
		default:
		}
		// A single oversized payload remains deliverable. No second record
		// is admitted until that record leaves the queue.
		if d.queuedRecords == 0 || d.queuedPayload+size <= d.payloadLimit {
			break
		}
		d.budgetChanged.Wait()
	}
	d.queuedPayload += size
	d.queuedRecords++
	return true
}

func (d *bufferedCallbackDelivery) release(msg jetstream.Msg) {
	if d.payloadLimit == 0 {
		return
	}
	d.budgetMu.Lock()
	d.queuedPayload -= len(msg.Data())
	d.queuedRecords--
	d.budgetChanged.Broadcast()
	d.budgetMu.Unlock()
}

func (d *bufferedCallbackDelivery) stop() {
	d.once.Do(func() {
		close(d.stopped)
		if d.budgetChanged != nil {
			d.budgetMu.Lock()
			d.budgetChanged.Broadcast()
			d.budgetMu.Unlock()
		}
		d.consume.Stop()
	})
}

func (d *bufferedCallbackDelivery) next(ctx context.Context) (jetstream.Msg, error) {
	msg, err := d.callbackDelivery.next(ctx)
	if err == nil {
		d.release(msg)
	}
	return msg, err
}
func (d *bufferedCallbackDelivery) stopAndJoin(ctx context.Context) error {
	d.stop()
	return d.callbackDelivery.stopAndJoin(ctx)
}
