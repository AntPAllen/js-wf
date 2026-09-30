package integration_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/testcluster"
)

type heldSerialReadJS struct {
	jetstream.JetStream
	proxy    *testcluster.ClientProxy
	armed    atomic.Bool
	requests atomic.Int32
	retried  chan struct{}
}

func (h *heldSerialReadJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	stream, err := h.JetStream.Stream(ctx, name)
	if err != nil || name != "WF_JRN" {
		return stream, err
	}
	return &heldSerialReadStream{Stream: stream, owner: h}, nil
}

type heldSerialReadStream struct {
	jetstream.Stream
	owner *heldSerialReadJS
}

func (s *heldSerialReadStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.owner.holdReadResponse()
	return s.Stream.GetMsg(ctx, seq, opts...)
}

func (h *heldSerialReadJS) holdReadResponse() {
	if h.armed.Load() {
		switch h.requests.Add(1) {
		case 1:
			h.proxy.HoldResponses()
		case 2:
			close(h.retried)
		}
	}
}

type heldManifestReadPort struct {
	journal.SnapshotWritePort
	owner *heldSerialReadJS
}

func (p *heldManifestReadPort) GetManifest(ctx context.Context, key string) ([]byte, error) {
	p.owner.holdReadResponse()
	return p.SnapshotWritePort.GetManifest(ctx, key)
}

func TestSerialJournalReadRecoversHeldNetworkResponse(t *testing.T) {
	testJournalReadRecoversHeldNetworkResponse(t, false)
}
func TestManifestReadRecoversHeldNetworkResponse(t *testing.T) {
	testJournalReadRecoversHeldNetworkResponse(t, true)
}
func testJournalReadRecoversHeldNetworkResponse(t *testing.T, manifest bool) {
	t.Helper()
	all, cluster := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	store := journal.New(all[0])
	var tail uint64
	for index, kind := range []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Completed} {
		var err error
		tail, err = store.Append(ctx, "test", "held-serial-read", journal.Entry{Index: uint64(index), Kind: kind}, tail)
		if err != nil {
			t.Fatal(err)
		}
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	hooked := &heldSerialReadJS{JetStream: js, proxy: proxy, retried: make(chan struct{})}
	reader := journal.New(hooked)
	minimumRequests := int32(5)
	if manifest {
		reader = journal.NewWithJetStreamSnapshotPort(js, &heldManifestReadPort{SnapshotWritePort: journal.NewSnapshotPort(js), owner: hooked})
		minimumRequests = 2
	}
	before, _, err := reader.Read(ctx, "test", "held-serial-read")
	if err != nil {
		t.Fatal(err)
	}
	hooked.armed.Store(true)
	result := make(chan error, 1)
	go func() {
		after, observed, err := reader.Read(ctx, "test", "held-serial-read")
		if err == nil && (!reflect.DeepEqual(before, after) || observed != tail) {
			result <- journal.ErrGap
			return
		}
		result <- err
	}()
	defer proxy.ResumeResponses()
	select {
	case <-hooked.retried:
	case <-time.After(4 * time.Second):
		t.Fatal("lost journal read response occupied the full read deadline without a retry")
	}
	proxy.ResumeResponses()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if hooked.requests.Load() < minimumRequests {
		t.Fatalf("journal read requests=%d", hooked.requests.Load())
	}
}
