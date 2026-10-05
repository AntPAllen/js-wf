package integrity

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestCompactAckCoordinatesDifferential(t *testing.T) {
	cases := []string{
		"$JS.ACK.WF_JRN.c.1.123.456.1700000000000000000.0",
		"$JS.ACK._.hash.WF_JRN.c.1.123.456.1700000000000000000.0.token",
		"$JS.ACK.domain.hash.WF_INV.c.1.123.456.1700000000000000000.0",
		"$JS.ACK.domain.hash.WF_INV.c.1.123.456.1700000000000000000.0.token.extra",
		"$JS.ACK.WF_JRN.c.1.18446744073709551615.2.3.0",
		"$JS.ACK.WF_JRN.c.1.18446744073709551616.2.3.0",
		"$JS.ACK.WF_JRN.c.1.+1.2.3.0",
		"", "$JS.ACK.WF_JRN.c.1.2.3.4", "$JS.BAD.WF_JRN.c.1.2.3.4.5",
	}
	rng := rand.New(rand.NewSource(19355))
	for i := 0; i < 20000; i++ {
		subject := fmt.Sprintf("$JS.ACK._.hash.WF_JRN.c.%d.%d.%d.%d.%d.token", rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64())
		if i%2 == 0 {
			parts := strings.Split(subject, ".")
			pos := rng.Intn(len(parts))
			parts[pos] = []string{"", "_", "1", "+1", "-1", "bad", "18446744073709551616", "01", "1_0", "١"}[rng.Intn(10)]
			subject = strings.Join(parts, ".")
		}
		cases = append(cases, subject)
	}
	accepted := 0
	for _, reply := range cases {
		stream, seq, timestamp, ok := compactAckCoordinates(reply)
		oracle, err := (&nats.Msg{Sub: &nats.Subscription{}, Reply: reply}).Metadata()
		// Legacy and modern Metadata use the same pinned internal parser. A
		// conservative miss delegates to modern Metadata in the actual scanner.
		if ok {
			accepted++
			if err != nil || oracle.Stream != stream || oracle.Sequence.Stream != seq || !oracle.Timestamp.Equal(timestamp) {
				t.Fatalf("reply=%q compact=%s/%d SDK=%+v/%v", reply, stream, seq, oracle, err)
			}
		}
	}
	if accepted < 10000 {
		t.Fatalf("insufficient ordinary syntax coverage: %d", accepted)
	}
}

func TestCompactCoordinatesKeepWrapperMetadata(t *testing.T) {
	// Reply would panic through the nil embedded interface; the wrapper's
	// Metadata override must be honored before considering any raw reply.
	msg := candidateControlMsg{stream: "override", seq: 71}
	stream, seq, timestamp, err := compactMessageCoordinates(msg)
	if err != nil || stream != "override" || seq != 71 || !timestamp.IsZero() {
		t.Fatalf("override lost: %s/%d/%v", stream, seq, err)
	}
}

func TestCompactAckCoordinatesNoAllocation(t *testing.T) {
	reply := "$JS.ACK._.hash.WF_JRN.c.1.123.456.1700000000000000000.0.token"
	var stream string
	var seq uint64
	var timestamp time.Time
	var ok bool
	allocated := testing.AllocsPerRun(1000, func() { stream, seq, timestamp, ok = compactAckCoordinates(reply) })
	if allocated != 0 || stream != "WF_JRN" || seq != 123 || !ok || timestamp.UnixNano() != 1700000000000000000 {
		t.Fatalf("allocated=%g coordinates=%s/%d/%v", allocated, stream, seq, ok)
	}
}

func BenchmarkCompactAckCoordinates(b *testing.B) {
	reply := "$JS.ACK._.hash.WF_JRN.c.1.123.456.1700000000000000000.0.token"
	b.Run("SDK", func(b *testing.B) {
		b.ReportAllocs()
		msg := &nats.Msg{Sub: &nats.Subscription{}, Reply: reply}
		for i := 0; i < b.N; i++ {
			m, e := msg.Metadata()
			if e != nil || m.Sequence.Stream != 123 {
				b.Fatal(e)
			}
		}
	})
	b.Run("Compact", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, seq, _, ok := compactAckCoordinates(reply)
			if !ok || seq != 123 {
				b.Fatal("coordinate parse")
			}
		}
	})
}

func TestCompactCoordinatesNativeMatchesSDK(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native bound SDK delivery metadata contract")
	}
	js, ctx := batchedAuditCluster(t)
	const count = 64
	for i := 0; i < count; i++ {
		if _, err := js.Publish(ctx, "wf.jrn.audit.coordinates", []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	c, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "compact-coordinate-control", MemoryStorage: true, Replicas: 3, AckPolicy: jetstream.AckNonePolicy, DeliverPolicy: jetstream.DeliverAllPolicy})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		call, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := stream.DeleteConsumer(call, c.CachedInfo().Name); err != nil {
			t.Error(err)
		}
	}()
	batch, err := c.Fetch(count, jetstream.FetchContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	visited := 0
	for msg := range batch.Messages() {
		stream, seq, timestamp, ok := compactAckCoordinates(msg.Reply())
		if !ok {
			t.Fatalf("ordinary native reply not recognized: %q", msg.Reply())
		}
		oracle, err := msg.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		actualStream, actualSeq, actualTime, err := compactMessageCoordinates(msg)
		if err != nil || stream != oracle.Stream || seq != oracle.Sequence.Stream || !timestamp.Equal(oracle.Timestamp) || actualStream != stream || actualSeq != seq || !actualTime.Equal(timestamp) {
			t.Fatalf("native mismatch compact=%s/%d/%s SDK=%+v candidate=%s/%d/%s/%v", stream, seq, timestamp, oracle, actualStream, actualSeq, actualTime, err)
		}
		if visited == 0 {
			allocated := testing.AllocsPerRun(1000, func() {
				_, _, _, failure := compactMessageCoordinates(msg)
				if failure != nil {
					t.Fatal(failure)
				}
			})
			if allocated != 0 {
				t.Fatalf("native candidate path allocates %g times; SDK fallback may be active", allocated)
			}
		}
		visited++
	}
	if err = batch.Error(); err != nil {
		t.Fatal(err)
	}
	if visited != count {
		t.Fatalf("visited %d", visited)
	}
}
