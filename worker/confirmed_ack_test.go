package worker

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"testing"
	"time"
)

type confirmedAckProbe struct {
	jetstream.Msg
	confirm func(context.Context) error
}

func (m confirmedAckProbe) Ack() error                          { panic("asynchronous ACK used") }
func (m confirmedAckProbe) DoubleAck(ctx context.Context) error { return m.confirm(ctx) }

func TestConfirmedDispatchAcknowledgementRespectsDeadline(t *testing.T) {
	for _, mode := range []string{"success", "parent_deadline", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			parent := context.Background()
			stop := func() {}
			if mode == "parent_deadline" {
				parent, stop = context.WithTimeout(parent, 20*time.Millisecond)
			}
			if mode == "cancelled" {
				var cancel context.CancelFunc
				parent, cancel = context.WithCancel(parent)
				cancel()
			}
			defer stop()
			var observed []OperationEvent
			w := &Worker{ID: "ack-worker", operationObserver: func(e OperationEvent) { observed = append(observed, e) }}
			ops := w.deliveryOperations("test", "ack", 17, 2)
			msg := confirmedAckProbe{confirm: func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 2*time.Second {
					t.Fatal("ACK budget missing or too long")
				}
				if mode == "parent_deadline" {
					want, _ := parent.Deadline()
					if !deadline.Equal(want) {
						t.Fatal("parent deadline extended")
					}
					<-ctx.Done()
					return ctx.Err()
				}
				if mode == "cancelled" {
					return ctx.Err()
				}
				return nil
			}}
			err := w.acknowledgeDispatch(parent, msg, ops)
			if mode == "success" && err != nil || mode == "parent_deadline" && !errors.Is(err, context.DeadlineExceeded) || mode == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if len(observed) != 1 || observed[0].Operation != "dispatch_ack_confirmed" || observed[0].RunSequence != 17 || observed[0].Delivery != 2 || observed[0].Duration < 0 {
				t.Fatal("missing ACK provenance", observed)
			}
			if (observed[0].Error == "") != (err == nil) {
				t.Fatal("ACK error provenance", observed, err)
			}
		})
	}
}
