package integrity

// Native comparison of the opt-in bulk reader against exact point reads.
import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"os"
	"testing"
	"time"

	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func candidateDigest(h hash.Hash, msg *jetstream.RawStreamMsg) error {
	_, err := fmt.Fprintf(h, "%d|%d|%s|%s|%s\n", msg.Sequence, msg.Time.UnixNano(), msg.Subject, msg.Header.Get("Proof"), msg.Data)
	return err
}

func TestAuditBatchScanCandidateNativeHighWaterHolesAndReadback(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("set WF_AUDIT_BATCH_CANDIDATE=1 for the 35k real-cluster candidate comparison")
	}
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var stream jetstream.Stream
	for {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		_, err = js.AccountInfo(call)
		if err == nil {
			stream, err = js.CreateStream(call, jetstream.StreamConfig{Name: "AUDIT_BATCH", Subjects: []string{"audit.batch.*"}, Replicas: 3, Storage: jetstream.FileStorage})
		}
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	const records = 35000
	for seq := 1; seq <= records; seq++ {
		_, err = js.PublishMsg(ctx, &nats.Msg{Subject: fmt.Sprintf("audit.batch.%d", seq%17), Header: nats.Header{"Proof": []string{fmt.Sprint(seq)}}, Data: []byte(fmt.Sprintf("retained-record-%d", seq))})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Interior holes, a missing captured tail, and later writes exercise the
	// difference between sequence bounds and number of delivered messages.
	for _, seq := range []uint64{1, 7, 511, 512, 513, 17000, records} {
		if err := stream.DeleteMsg(ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	cutoff := uint64(records)
	for i := 0; i < 20; i++ {
		if _, err := js.Publish(ctx, "audit.batch.later", []byte("excluded")); err != nil {
			t.Fatal(err)
		}
	}
	pointDigest, batchDigest := sha256.New(), sha256.New()
	pointStarted := time.Now()
	pointCtx, pointStop := context.WithTimeout(ctx, 20*time.Second)
	pointCount := 0
	err = scanThrough(pointCtx, stream, &cutoff, func(msg *jetstream.RawStreamMsg) error { pointCount++; return candidateDigest(pointDigest, msg) })
	pointStop()
	if err != nil {
		t.Fatalf("point baseline: %v", err)
	}
	pointElapsed := time.Since(pointStarted)
	batchStarted := time.Now()
	batchCtx, batchStop := context.WithTimeout(ctx, 20*time.Second)
	batchCount := 0
	err = scanBatchThrough(batchCtx, stream, &cutoff, func(msg *jetstream.RawStreamMsg) error { batchCount++; return candidateDigest(batchDigest, msg) })
	batchStop()
	if err != nil {
		t.Fatalf("batch candidate: %v", err)
	}
	batchElapsed := time.Since(batchStarted)
	if pointCount != records-7 || pointCount != batchCount || fmt.Sprintf("%x", pointDigest.Sum(nil)) != fmt.Sprintf("%x", batchDigest.Sum(nil)) {
		t.Fatalf("incomplete/different scan: point=%d batch=%d", pointCount, batchCount)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 {
		t.Fatalf("consumer cleanup info=%+v err=%v", info, err)
	}
	t.Logf("records=%d point=%s batch=%s digest=%x consumers=%d", batchCount, pointElapsed, batchElapsed, batchDigest.Sum(nil), info.State.Consumers)
	// A fresh audit must observe a change made after the first audit. Neither
	// consumer delivery nor prior audit results may be cached as proof.
	if err := stream.DeleteMsg(ctx, 12345); err != nil {
		t.Fatal(err)
	}
	freshCount := 0
	freshCtx, freshStop := context.WithTimeout(ctx, 20*time.Second)
	err = scanBatchThrough(freshCtx, stream, &cutoff, func(msg *jetstream.RawStreamMsg) error { freshCount++; return nil })
	freshStop()
	if err != nil || freshCount != batchCount-1 {
		t.Fatalf("fresh read count=%d err=%v", freshCount, err)
	}
}
