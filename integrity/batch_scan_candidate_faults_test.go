package integrity

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

// A supplied artifact root retains original stores after success or failure.
func candidateNativeRoot(t *testing.T) string {
	t.Helper()
	if root := os.Getenv("WF_AUDIT_BATCH_ROOT"); root != "" {
		if !filepath.IsAbs(root) {
			t.Fatal("candidate artifact root must be absolute")
		}
		name := strings.ReplaceAll(t.Name(), "/", "-")
		path := filepath.Join(root, name)
		if err := os.MkdirAll(root, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	return t.TempDir()
}
func candidateNativeStream(t *testing.T, js jetstream.JetStream, ctx context.Context) jetstream.Stream {
	t.Helper()
	for {
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		_, err := js.AccountInfo(call)
		var stream jetstream.Stream
		if err == nil {
			stream, err = js.CreateStream(call, jetstream.StreamConfig{Name: "AUDIT_FAULT", Subjects: []string{"audit.fault.>"}, Replicas: 3, Storage: jetstream.FileStorage})
		}
		stop()
		if err == nil {
			return stream
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
func candidatePublish(t *testing.T, js jetstream.JetStream, ctx context.Context, count int) {
	t.Helper()
	for seq := 1; seq <= count; seq++ {
		if _, err := js.Publish(ctx, fmt.Sprintf("audit.fault.%d", seq%13), []byte(fmt.Sprintf("record-%d", seq))); err != nil {
			t.Fatal(err)
		}
	}
}

type candidateObservedStream struct {
	jetstream.Stream
	name             string
	gapCalls         int
	consumerReplicas int
}

func (s *candidateObservedStream) CreateConsumer(ctx context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.name = cfg.Name
	consumer, err := s.Stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return consumer, err
	}
	s.consumerReplicas = consumer.CachedInfo().Config.Replicas
	if s.consumerReplicas != cfg.Replicas {
		return nil, fmt.Errorf("consumer replicas=%d requested=%d", s.consumerReplicas, cfg.Replicas)
	}
	return consumer, nil
}
func (s *candidateObservedStream) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	s.gapCalls++
	return s.Stream.GetMsg(ctx, seq, opts...)
}
func TestAuditBatchScanCandidateNativeLeaderLossAndCancellation(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native batch candidate qualification")
	}
	for _, fault := range []string{"leader-loss", "cancellation", "consumer-deletion"} {
		t.Run(fault, func(t *testing.T) {
			cluster, err := testcluster.Start(candidateNativeRoot(t), 3)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			stream := candidateNativeStream(t, js, ctx)
			const records = 3000
			candidatePublish(t, js, ctx, records)
			info, err := stream.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			leader := -1
			for i, server := range cluster.Servers {
				if server.Name() == info.Cluster.Leader {
					leader = i
				}
			}
			if leader < 0 {
				t.Fatal("stream leader not found")
			}
			// Keep the audit's pinned connection on a surviving server.
			clientNode := (leader + 1) % 3
			js, err = jetstream.New(cluster.Clients[clientNode])
			if err != nil {
				t.Fatal(err)
			}
			stream, err = js.Stream(ctx, "AUDIT_FAULT")
			if err != nil {
				t.Fatal(err)
			}
			observed := &candidateObservedStream{Stream: stream}
			expected := sha256.New()
			for seq := uint64(1); seq <= records; seq++ {
				msg, err := stream.GetMsg(ctx, seq)
				if err != nil {
					t.Fatal(err)
				}
				if err := candidateDigest(expected, msg); err != nil {
					t.Fatal(err)
				}
			}
			auditCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			actual := sha256.New()
			count := 0
			started := time.Now()
			err = candidateBatchScan(auditCtx, observed, nil, func(msg *jetstream.RawStreamMsg) error {
				count++
				if count == 128 {
					switch fault {
					case "leader-loss":
						cluster.KillNode(leader)
					case "cancellation":
						cancel()
					case "consumer-deletion":
						if err := stream.DeleteConsumer(ctx, observed.name); err != nil {
							return err
						}
					}
				}
				return candidateDigest(actual, msg)
			})
			if fault == "cancellation" {
				if !errors.Is(err, context.Canceled) || count != 128 {
					t.Fatalf("cancellation err=%v visited=%d", err, count)
				}
			} else if fault == "consumer-deletion" {
				// Deletion may fail the scan explicitly, but must never silently
				// qualify a partial result as complete.
				if err == nil && (count != records || fmt.Sprintf("%x", actual.Sum(nil)) != fmt.Sprintf("%x", expected.Sum(nil))) {
					t.Fatalf("false completion after deletion: count=%d", count)
				}
			} else if err != nil || count != records || fmt.Sprintf("%x", actual.Sum(nil)) != fmt.Sprintf("%x", expected.Sum(nil)) {
				t.Fatalf("leader recovery err=%v count=%d", err, count)
			}
			if time.Since(started) >= 20*time.Second {
				t.Fatal("audit exceeded its original budget")
			}
			info, infoErr := stream.Info(ctx)
			if infoErr != nil || info.State.Consumers != 0 {
				t.Fatalf("consumer cleanup err=%v info=%+v", infoErr, info)
			}
			t.Logf("fault=%s leader=%d client=%d visited=%d elapsed=%s gap_reads=%d consumer_replicas=%d scan_error=%v consumers=%d", fault, leader, clientNode, count, time.Since(started), observed.gapCalls, observed.consumerReplicas, err, info.State.Consumers)
		})
	}
}

func TestAuditBatchScanCandidateNativeLegacy211(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native batch candidate qualification")
	}
	legacy := os.Getenv("WF_AUDIT_BATCH_LEGACY_BINARY")
	if legacy == "" {
		t.Skip("set WF_AUDIT_BATCH_LEGACY_BINARY to the supported NATS 2.11.17 executable")
	}
	root := candidateNativeRoot(t)
	data, err := os.ReadFile(legacy)
	if err != nil {
		t.Fatal(err)
	}
	legacy = filepath.Join(root, "nats-server-2.11.17")
	if err := os.WriteFile(legacy, data, 0755); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.StartMixedVersionProcesses(filepath.Join(root, "stores"), []string{legacy, legacy, legacy})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	for _, nc := range cluster.Clients {
		if nc.ConnectedServerVersion() != "2.11.17" {
			t.Fatalf("unexpected legacy server %s", nc.ConnectedServerVersion())
		}
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	stream := candidateNativeStream(t, js, ctx)
	const records = 1000
	candidatePublish(t, js, ctx, records)
	for _, seq := range []uint64{2, 500, 1000} {
		if err := stream.DeleteMsg(ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	expected := sha256.New()
	if err := scan(ctx, stream, func(msg *jetstream.RawStreamMsg) error { return candidateDigest(expected, msg) }); err != nil {
		t.Fatal(err)
	}
	observed := &candidateObservedStream{Stream: stream}
	actual := sha256.New()
	count := 0
	auditCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	started := time.Now()
	if err := candidateBatchScan(auditCtx, observed, nil, func(msg *jetstream.RawStreamMsg) error { count++; return candidateDigest(actual, msg) }); err != nil {
		t.Fatal(err)
	}
	if count != records-3 || fmt.Sprintf("%x", expected.Sum(nil)) != fmt.Sprintf("%x", actual.Sum(nil)) {
		t.Fatalf("legacy mismatch count=%d", count)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 {
		t.Fatalf("cleanup err=%v info=%+v", err, info)
	}
	t.Logf("version=2.11.17 replicas=3 count=%d elapsed=%s gap_reads=%d consumer_replicas=%d digest=%x consumers=%d", count, time.Since(started), observed.gapCalls, observed.consumerReplicas, actual.Sum(nil), info.State.Consumers)
}

func TestAuditBatchScanCandidateNativeSparseHundredThousandSpan(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in native batch candidate qualification")
	}
	cluster, err := testcluster.Start(candidateNativeRoot(t), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0], jetstream.WithPublishAsyncMaxPending(512))
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	stream := candidateNativeStream(t, js, ctx)
	if _, err := js.Publish(ctx, "audit.fault.anchor", []byte("first")); err != nil {
		t.Fatal(err)
	}
	const holes = 100000
	futures := make([]jetstream.PubAckFuture, 0, holes)
	for i := 0; i < holes; i++ {
		future, err := js.PublishAsync("audit.fault.hole", []byte("deleted-span"))
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, future)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for i, future := range futures {
		select {
		case ack := <-future.Ok():
			if ack.Sequence != uint64(i+2) {
				t.Fatalf("publication sequence=%d expected=%d", ack.Sequence, i+2)
			}
		case err := <-future.Err():
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	last, err := js.Publish(ctx, "audit.fault.anchor", []byte("last"))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject("audit.fault.hole")); err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.FirstSeq != 1 || info.State.LastSeq != holes+2 || info.State.Msgs != 2 {
		t.Fatalf("incorrect sparse fixture %+v", info.State)
	}
	expected := sha256.New()
	for _, seq := range []uint64{1, last.Sequence} {
		msg, err := stream.GetMsg(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		if err := candidateDigest(expected, msg); err != nil {
			t.Fatal(err)
		}
	}
	observed := &candidateObservedStream{Stream: stream}
	actual := sha256.New()
	count := 0
	auditCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	started := time.Now()
	if err := candidateBatchScan(auditCtx, observed, nil, func(msg *jetstream.RawStreamMsg) error { count++; return candidateDigest(actual, msg) }); err != nil {
		t.Fatal(err)
	}
	if count != 2 || observed.gapCalls != 1 || fmt.Sprintf("%x", actual.Sum(nil)) != fmt.Sprintf("%x", expected.Sum(nil)) {
		t.Fatalf("sparse scan count=%d gap_reads=%d", count, observed.gapCalls)
	}
	info, err = stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 {
		t.Fatalf("cleanup err=%v info=%+v", err, info)
	}
	t.Logf("span=%d retained=%d deleted=%d elapsed=%s leader_gap_reads=%d digest=%x consumers=%d", last.Sequence, count, holes, time.Since(started), observed.gapCalls, actual.Sum(nil), info.State.Consumers)
}
