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
	// With no caller deadline cleanup gets its independent two-second budget.
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 {
		t.Fatalf("cursor cleanup: info=%+v err=%v", info, err)
	}
	t.Logf("retained_records=%d payload_bytes=%d digest=%x cancellation_visits=%d consumers=%d", bulkCount, bulkCount*(256<<10), bulk.Sum(nil), visits, info.State.Consumers)
}
