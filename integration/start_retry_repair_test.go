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
	blocked  atomic.Bool
	attempts atomic.Int32
}

func (j *droppedStartEnqueueJS) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if strings.HasPrefix(subject, "wf.run.") && j.blocked.Load() {
		j.attempts.Add(1)
		return nil, jetstream.ErrNoStreamResponse
	}
	return j.JetStream.Publish(ctx, subject, data, opts...)
}

func TestMatchingStartRetryRepairsDroppedEnqueueWithoutScanner(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	dropped := &droppedStartEnqueueJS{JetStream: all[0]}
	dropped.blocked.Store(true)
	c := client.New(dropped)
	first, err := c.Start(ctx, "retryrepair", "one", []byte(`1`))
	if !errors.Is(err, client.ErrEnqueueUnknown) || first.InvSeq == 0 {
		t.Fatalf("initial enqueue failure: %+v %v", first, err)
	}
	beforeMismatch := dropped.attempts.Load()
	if beforeMismatch < 2 {
		t.Fatal("persistent failure did not retry")
	}
	// A changed input must not enqueue even while the transport remains blocked.
	if _, err := c.Start(ctx, "retryrepair", "one", []byte(`2`)); !errors.Is(err, client.ErrInputMismatch) {
		t.Fatal(err)
	}
	if dropped.attempts.Load() != beforeMismatch {
		t.Fatal("input mismatch attempted enqueue")
	}
	second, err := c.Start(ctx, "retryrepair", "one", []byte(`1`))
	if !errors.Is(err, client.ErrEnqueueUnknown) || second.InvSeq != first.InvSeq {
		t.Fatalf("persistent enqueue failure concealed: %+v %v", second, err)
	}
	if dropped.attempts.Load() <= beforeMismatch {
		t.Fatal("matching retry did not attempt repair")
	}
	runs, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := runs.Info(ctx)
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("dropped enqueues retained runs: %+v %v", info, err)
	}
	dropped.blocked.Store(false)
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
