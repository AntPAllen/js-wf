//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type latencyMetadataStream struct {
	jetstream.Stream
	reads atomic.Uint64
}

func (s *latencyMetadataStream) GetMsg(ctx context.Context, sequence uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n := s.reads.Add(1)
	return &jetstream.RawStreamMsg{Sequence: sequence, Data: []byte(fmt.Sprint(n)), Time: time.Unix(0, int64(n))}, nil
}

type latencyMetadataProvider struct {
	jetstream.JetStream
	stream    *latencyMetadataStream
	failFirst atomic.Bool
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (p *latencyMetadataProvider) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if p.failFirst.CompareAndSwap(true, false) {
		return nil, jetstream.ErrNoStreamResponse
	}
	if name == "WF_JRN" && p.entered != nil {
		p.once.Do(func() { close(p.entered) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.release:
		}
	}
	return p.stream, nil
}

func (p *latencyMetadataProvider) KeyValue(context.Context, string) (jetstream.KeyValue, error) {
	return struct{ jetstream.KeyValue }{}, nil
}

func (p *latencyMetadataProvider) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return struct{ jetstream.ObjectStore }{}, nil
}

func TestMatrixLatencyMetadataCoalescesHandlesKeepsFreshRecords(t *testing.T) {
	ctx := context.Background()
	provider := &latencyMetadataProvider{stream: &latencyMetadataStream{}}
	j := &matrixLatencyMetadataJS{JetStream: provider}
	var joined sync.WaitGroup
	values := make(chan string, 32)
	for i := 0; i < 32; i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for _, name := range []string{"WF_JRN", "WF_SIG"} {
				s, err := j.Stream(ctx, name)
				if err != nil {
					t.Error(err)
					return
				}
				if name == "WF_JRN" {
					msg, err := s.GetMsg(ctx, 1)
					if err != nil {
						t.Error(err)
						return
					}
					values <- string(msg.Data)
				}
			}
			if _, err := j.KeyValue(ctx, "WF_STATE"); err != nil {
				t.Error(err)
			}
			if _, err := j.ObjectStore(ctx, "WF_BLOB"); err != nil {
				t.Error(err)
			}
		}()
	}
	joined.Wait()
	close(values)
	unique := map[string]bool{}
	for value := range values {
		unique[value] = true
	}
	if len(unique) != 32 || provider.stream.reads.Load() != 32 {
		t.Fatalf("records were cached: unique=%d reads=%d", len(unique), provider.stream.reads.Load())
	}
	for resource, count := range j.lookupCounts() {
		if count != 1 {
			t.Fatalf("%s looked up %d times", resource, count)
		}
	}
}

func TestMatrixLatencyMetadataFailureRetryAndCancelledWaiter(t *testing.T) {
	provider := &latencyMetadataProvider{stream: &latencyMetadataStream{}}
	provider.failFirst.Store(true)
	j := &matrixLatencyMetadataJS{JetStream: provider}
	if _, err := j.Stream(context.Background(), "WF_JRN"); !errors.Is(err, jetstream.ErrNoStreamResponse) {
		t.Fatalf("failure changed: %v", err)
	}
	if _, err := j.Stream(context.Background(), "WF_JRN"); err != nil || j.journalLookups.Load() != 2 {
		t.Fatalf("failed handle cached: %v lookups=%d", err, j.journalLookups.Load())
	}
	provider = &latencyMetadataProvider{stream: &latencyMetadataStream{}, entered: make(chan struct{}), release: make(chan struct{})}
	j = &matrixLatencyMetadataJS{JetStream: provider}
	leadCtx, stopLead := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopLead()
	lead := make(chan error, 1)
	go func() { _, err := j.Stream(leadCtx, "WF_JRN"); lead <- err }()
	<-provider.entered
	waiterCtx, stopWaiter := context.WithCancel(context.Background())
	stopWaiter()
	if _, err := j.Stream(waiterCtx, "WF_JRN"); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter cannot cancel: %v", err)
	}
	// A blocked journal lookup cannot serialize independent signal metadata.
	if _, err := j.Stream(leadCtx, "WF_SIG"); err != nil {
		t.Fatal(err)
	}
	close(provider.release)
	if err := <-lead; err != nil || j.journalLookups.Load() != 1 {
		t.Fatalf("lead disrupted: %v", err)
	}
}
