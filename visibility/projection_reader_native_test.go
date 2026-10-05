package visibility

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

type projectionObservedStream struct {
	jetstream.Stream
	created chan jetstream.Consumer
}

func (s projectionObservedStream) OrderedConsumer(ctx context.Context, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	c, err := s.Stream.OrderedConsumer(ctx, cfg)
	if err == nil {
		s.created <- c
	}
	return c, err
}
func TestProjectionReaderNativeConsumerDeletion(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Start returns pinned clients before JetStream's account/metadata leader
	// is necessarily ready. Admit the source with bounded requests, as the
	// integration fixtures do, under this same original control deadline.
	var stream jetstream.Stream
	for ctx.Err() == nil {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		_, err = js.AccountInfo(call)
		if err == nil {
			stream, err = js.CreateOrUpdateStream(call, jetstream.StreamConfig{Name: "PROJECTION_READER", Subjects: []string{"reader.>"}, Storage: jetstream.FileStorage, Replicas: 3, MaxMsgsPerSubject: 1})
		}
		stop()
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || ctx.Err() != nil {
		t.Fatalf("projection reader source admission: %v / %v", err, ctx.Err())
	}
	var wanted []uint64
	// Overwrite each retained subject once, creating real sequence holes.
	for i := 0; i < 600; i++ {
		for j := 0; j < 2; j++ {
			ack, err := js.Publish(ctx, fmt.Sprintf("reader.%d", i), []byte("input"))
			if err != nil {
				t.Fatal(err)
			}
			if j == 1 {
				wanted = append(wanted, ack.Sequence)
			}
		}
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 600 {
		t.Fatalf("retained input: %v %v", info, err)
	}
	created := make(chan jetstream.Consumer, 1)
	p := &Projection{inv: projectionObservedStream{Stream: stream, created: created}}
	jobs := make(chan uint64)
	done := make(chan error, 1)
	go func() { done <- p.enqueueRetainedInvocations(ctx, info.State.LastSeq, jobs); close(jobs) }()
	consumer := <-created
	oldName := consumer.CachedInfo().Name
	count := 0
	for seq := range jobs {
		if count >= len(wanted) || seq != wanted[count] {
			t.Fatalf("retained sequence%d=%d", count, seq)
		}
		count++
		if count == 37 {
			if err := stream.DeleteConsumer(ctx, oldName); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Consumer(ctx, oldName); !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatalf("consumer removal unconfirmed: %v", err)
			}
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count != len(wanted) || consumer.CachedInfo().Name == oldName {
		t.Fatalf("count=%d expected=%d consumer recovery absent", count, len(wanted))
	}
	t.Logf("R3 file source: %d retained inputs across sequence holes; consumer %s deleted and recovered as %s", count, oldName, consumer.CachedInfo().Name)
}
