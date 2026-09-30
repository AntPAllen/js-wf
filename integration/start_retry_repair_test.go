package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
)

type droppedStartEnqueueJS struct {
	jetstream.JetStream
	remaining atomic.Int32
}

func (j *droppedStartEnqueueJS) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if strings.HasPrefix(subject, "wf.run.") && j.remaining.Add(-1) >= 0 {
		return nil, jetstream.ErrNoStreamResponse
	}
	return j.JetStream.Publish(ctx, subject, data, opts...)
}

func TestMatchingStartRetryRepairsDroppedEnqueueWithoutScanner(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	dropped := &droppedStartEnqueueJS{JetStream: all[0]}
	dropped.remaining.Store(2)
	c := client.New(dropped)
	first, err := c.Start(ctx, "retryrepair", "one", []byte(`1`))
	if !errors.Is(err, client.ErrEnqueueUnknown) || first.InvSeq == 0 {
		t.Fatalf("initial enqueue failure: %+v %v", first, err)
	}
	// A changed input must neither enqueue nor consume the next injected failure.
	if _, err := c.Start(ctx, "retryrepair", "one", []byte(`2`)); !errors.Is(err, client.ErrInputMismatch) {
		t.Fatal(err)
	}
	second, err := c.Start(ctx, "retryrepair", "one", []byte(`1`))
	if !errors.Is(err, client.ErrEnqueueUnknown) || second.InvSeq != first.InvSeq {
		t.Fatalf("persistent enqueue failure concealed: %+v %v", second, err)
	}
	third, err := c.Start(ctx, "retryrepair", "one", []byte(`1`))
	if !errors.Is(err, client.ErrAlreadyStarted) || third.InvSeq != first.InvSeq {
		t.Fatalf("matching retry repair: %+v %v", third, err)
	}
	fourth, err := client.New(all[1]).Start(ctx, "retryrepair", "one", []byte(`1`))
	if !errors.Is(err, client.ErrAlreadyStarted) || fourth.InvSeq != first.InvSeq {
		t.Fatalf("cross-node duplicate: %+v %v", fourth, err)
	}
	for _, name := range []string{"WF_INV", "WF_RUN"} {
		stream, err := all[2].Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		if err != nil || info.State.Msgs != 1 {
			t.Fatalf("%s info=%+v err=%v", name, info, err)
		}
	}
}
