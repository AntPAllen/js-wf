package integrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestByteBoundedScanNativePayloadRefillHolesCutoffAndCancel(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native byte-bounded transport control")
	}
	js, ctx, _ := batchedAuditClusterWithServers(t)
	stream := candidateNativeStream(t, js, ctx)
	// Almost 30 MiB crosses the 8 MiB client buffer several times, while
	// each record fits the standard server payload limit. Sequence holes and
	// a deleted cutoff force the leader absence oracle as well as delivery.
	for seq := 1; seq <= 120; seq++ {
		payload := bytes.Repeat([]byte{byte(seq)}, 256<<10)
		_, err := js.PublishMsg(ctx, &nats.Msg{Subject: "audit.fault.payload", Header: nats.Header{"Proof": []string{fmt.Sprint(seq)}}, Data: payload})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, seq := range []uint64{1, 7, 119} {
		if err := stream.DeleteMsg(ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := uint64(119)
	point, bulk := sha256.New(), sha256.New()
	pointCount, bulkCount := 0, 0
	if err := scanThrough(ctx, stream, &cutoff, func(m *jetstream.RawStreamMsg) error {
		pointCount++
		return candidateDigest(point, m)
	}); err != nil {
		t.Fatal(err)
	}
	call, stop := context.WithTimeout(ctx, 20*time.Second)
	err := scanByteBoundedThrough(call, stream, &cutoff, func(m *jetstream.RawStreamMsg) error {
		bulkCount++
		return candidateDigest(bulk, m)
	})
	stop()
	if err != nil || pointCount != 116 || bulkCount != pointCount || !bytes.Equal(point.Sum(nil), bulk.Sum(nil)) {
		t.Fatalf("point=%d bulk=%d err=%v digests=%x/%x", pointCount, bulkCount, err, point.Sum(nil), bulk.Sum(nil))
	}
	call, stop = context.WithCancel(ctx)
	visits := 0
	err = scanByteBoundedThrough(call, stream, nil, func(*jetstream.RawStreamMsg) error {
		visits++
		stop()
		return context.Canceled
	})
	stop()
	if !errors.Is(err, context.Canceled) || visits != 1 {
		t.Fatalf("cancel visits=%d err=%v", visits, err)
	}
	callbackDigest := sha256.New()
	callbackCount := 0
	call, stop = context.WithTimeout(ctx, 20*time.Second)
	err = scanConsumeByteBoundedThrough(call, stream, &cutoff, func(m *jetstream.RawStreamMsg) error {
		callbackCount++
		return candidateDigest(callbackDigest, m)
	})
	stop()
	if err != nil || callbackCount != pointCount || !bytes.Equal(point.Sum(nil), callbackDigest.Sum(nil)) {
		t.Fatalf("callback holes/cutoff: count=%d err=%v digest=%x/%x", callbackCount, err, point.Sum(nil), callbackDigest.Sum(nil))
	}
	call, stop = context.WithCancel(ctx)
	callbackVisits := 0
	err = scanConsumeByteBoundedThrough(call, stream, nil, func(*jetstream.RawStreamMsg) error {
		callbackVisits++
		stop()
		return context.Canceled
	})
	stop()
	if !errors.Is(err, context.Canceled) || callbackVisits != 1 {
		t.Fatalf("callback cancel visits=%d err=%v", callbackVisits, err)
	}
	directDigest := sha256.New()
	directCount := 0
	call, stop = context.WithTimeout(ctx, 20*time.Second)
	err = scanConsumeDirectWindowsThrough(call, stream, &cutoff, func(m *jetstream.RawStreamMsg) error {
		directCount++
		return candidateDigest(directDigest, m)
	})
	stop()
	if err != nil || directCount != pointCount || !bytes.Equal(point.Sum(nil), directDigest.Sum(nil)) {
		t.Fatalf("direct holes/cutoff: count=%d err=%v digest=%x/%x", directCount, err, point.Sum(nil), directDigest.Sum(nil))
	}
	call, stop = context.WithCancel(ctx)
	directVisits := 0
	err = scanConsumeDirectWindowsThrough(call, stream, nil, func(*jetstream.RawStreamMsg) error {
		directVisits++
		stop()
		return context.Canceled
	})
	stop()
	if !errors.Is(err, context.Canceled) || directVisits != 1 {
		t.Fatalf("direct cancel visits=%d err=%v", directVisits, err)
	}
	// With no caller deadline cleanup gets its independent two-second budget.
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 {
		t.Fatalf("cursor cleanup: info=%+v err=%v", info, err)
	}
	t.Logf("retained_records=%d payload_bytes=%d digest=%x cancellation_visits=%d consumers=%d", bulkCount, bulkCount*(256<<10), bulk.Sum(nil), visits, info.State.Consumers)
}

// A healthy iterator must cross the production byte window without invoking
// transport recovery. The batch reader, not SDK StopAfter, owns its record cap.
func TestByteBoundedFetchNativeHealthyRefill(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native healthy refill regression")
	}
	js, ctx, _ := batchedAuditClusterWithServers(t)
	stream := candidateNativeStream(t, js, ctx)
	payload := bytes.Repeat([]byte("r"), 256<<10)
	for n := 0; n < 64; n++ {
		if _, err := js.Publish(ctx, "audit.fault.payload", payload); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "healthy-refill", AckPolicy: jetstream.AckNonePolicy, Replicas: 3, MemoryStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	call, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	batch, err := fetchByteBounded(call, consumer, 48, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for msg := range batch.Messages() {
		count++
		meta, err := msg.Metadata()
		if err != nil || meta.Sequence.Stream != uint64(count) || !bytes.Equal(msg.Data(), payload) {
			t.Fatalf("record=%d meta=%+v err=%v", count, meta, err)
		}
	}
	if err := batch.Error(); err != nil || count != 48 {
		t.Fatalf("healthy refill records=%d expected=48 err=%v", count, err)
	}
	if err := stream.DeleteConsumer(ctx, "healthy-refill"); err != nil {
		t.Fatal(err)
	}
	t.Logf("healthy_refill records=48 bytes=%d published=64 byte_window=%d recovery_errors=0", count*len(payload), 8<<20)
}

type healthyByteStream struct {
	jetstream.Stream
	creates, gaps int
}

func (s *healthyByteStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.creates++
	return s.Stream.CreateConsumer(ctx, cfg)
}
func (s *healthyByteStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.gaps++
	return s.Stream.GetMsg(ctx, seq, opts...)
}
func TestByteBoundedScanNativeRetainsPrefetchAcrossWindows(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native continuous byte iterator regression")
	}
	js, ctx, _ := batchedAuditClusterWithServers(t)
	stream := candidateNativeStream(t, js, ctx)
	payload := bytes.Repeat([]byte("w"), 4096)
	for n := 0; n < 4100; n++ {
		if _, err := js.Publish(ctx, "audit.fault.payload", payload); err != nil {
			t.Fatal(err)
		}
	}
	observed := &healthyByteStream{Stream: stream}
	count := 0
	call, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	err := scanByteBoundedThrough(call, observed, nil, func(m *jetstream.RawStreamMsg) error {
		count++
		if m.Sequence != uint64(count) || !bytes.Equal(m.Data, payload) {
			return fmt.Errorf("wrong record at %d", count)
		}
		return nil
	})
	if err != nil || count != 4100 || observed.creates != 1 || observed.gaps != 0 {
		t.Fatalf("records=%d creates=%d gaps=%d err=%v", count, observed.creates, observed.gaps, err)
	}
	// StreamInfo's consumer count can lag the acknowledged delete. Observe
	// convergence within the same two-second cleanup budget; do not repair it.
	cleanup, done := context.WithTimeout(ctx, 2*time.Second)
	defer done()
	for {
		info, err := stream.Info(cleanup)
		if err == nil && info.State.Consumers == 0 {
			break
		}
		if cleanup.Err() != nil {
			t.Fatalf("cleanup=%+v err=%v", info, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("records=4100 bytes=%d record_window=4096 byte_window=%d consumers_created=1 leader_gap_reads=0 remaining_consumers=0", count*len(payload), 8<<20)
}
