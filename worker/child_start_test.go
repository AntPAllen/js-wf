package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/wf"
)

type interruptedChildStart struct {
	mode     string
	healed   bool
	stored   *jetstream.RawStreamMsg
	enqueues int
}

func (p *interruptedChildStart) PublishInvocation(_ context.Context, m *nats.Msg) (uint64, error) {
	if p.stored != nil || !p.healed && p.mode == "create_request_lost" {
		return 0, nats.ErrTimeout
	}
	p.stored = &jetstream.RawStreamMsg{Subject: m.Subject, Header: m.Header, Data: append([]byte(nil), m.Data...), Sequence: 1}
	return 1, nil
}
func (p *interruptedChildStart) LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error) {
	if p.stored == nil {
		return nil, jetstream.ErrMsgNotFound
	}
	return p.stored, nil
}
func (*interruptedChildStart) PutInput(context.Context, string, []byte) error {
	return errors.New("unexpected spill")
}
func (p *interruptedChildStart) EnqueueRun(ctx context.Context, _ string, _ []byte, _ string) error {
	if !p.healed {
		<-ctx.Done()
		return ctx.Err()
	}
	p.enqueues++
	return nil
}
func (*interruptedChildStart) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func TestChildStartWholeBudgetPreservesUnfinishedRequestAndReplay(t *testing.T) {
	for _, mode := range []string{"create_request_lost", "enqueue_reply_lost"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			port := &interruptedChildStart{mode: mode}
			w := &Worker{client: client.NewWithStartPort(port)}
			var entries []wf.Entry
			makeContext := func() *wf.Context {
				c := wf.NewContext(ctx, append([]wf.Entry(nil), entries...), func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
					entries = append(entries, wf.Entry{Index: uint64(len(entries)), Kind: kind, Payload: append(json.RawMessage(nil), payload...)})
					return nil
				})
				c.SetChildSupport("parent", "one", 42, func(ctx context.Context, typ, id string, input []byte, signal string) error {
					return w.startChild(ctx, typ, id, input, "parent", "one", 42, signal, nil)
				})
				return c
			}
			started := time.Now()
			if _, err := wf.CallAsync(makeContext(), "child", []byte(`null`)); !errors.Is(err, wf.ErrChildStart) {
				t.Fatalf("uncertain start: %v", err)
			}
			elapsed := time.Since(started)
			if elapsed < childStartBudget-100*time.Millisecond || elapsed > childStartBudget+2*time.Second || ctx.Err() != nil {
				t.Fatalf("whole budget elapsed=%s parent=%v", elapsed, ctx.Err())
			}
			if len(entries) != 1 || entries[0].Kind != wf.StepRequested {
				t.Fatalf("uncertain start changed request: %+v", entries)
			}
			prefix := append(json.RawMessage(nil), entries[0].Payload...)
			port.healed = true
			promise, err := wf.CallAsync(makeContext(), "child", []byte(`null`))
			if err != nil || promise.ChildID == "" || port.enqueues != 1 || port.stored == nil || port.stored.Sequence != 1 {
				t.Fatalf("replay promise=%+v enqueues=%d error=%v", promise, port.enqueues, err)
			}
			if len(entries) != 2 || entries[1].Kind != wf.StepCompleted || string(entries[0].Payload) != string(prefix) {
				t.Fatalf("replay changed durable prefix: %+v", entries)
			}
			if _, err := wf.CallAsync(makeContext(), "child", []byte(`null`)); err != nil || port.enqueues != 1 {
				t.Fatalf("completed replay started another child: %v", err)
			}
		})
	}
}
