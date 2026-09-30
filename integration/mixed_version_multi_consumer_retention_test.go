//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Expected bad-state controls are separate from clean runtime release evidence.
// This contract observes two consumer groups, then proves physical removal after
// the stream leader moves and after two distinct leaders run upgraded servers.
func TestMixedVersionMultipleConsumerRetentionRecovery(t *testing.T) {
	old := os.Getenv("WF_NATS_SERVER_BIN")
	if old == "" {
		t.Skip("requires NATS 2.11.17 through WF_NATS_SERVER_BIN")
	}
	cluster, err := testcluster.StartMixedVersionProcesses(t.TempDir(), []string{old, old, old})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for node := range cluster.Commands {
			data, err := os.ReadFile(cluster.LogPath(node))
			if err != nil {
				t.Logf("server %d log: %v", node, err)
				continue
			}
			if prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX"); prefix != "" {
				if err := os.WriteFile(fmt.Sprintf("%s-node-%d.log", prefix, node), data, 0600); err != nil {
					t.Error(err)
				}
			}
			if len(data) > 4096 {
				data = data[len(data)-4096:]
			}
			t.Logf("server %d log tail:\n%s", node, data)
		}
	}()
	for node, version := range matrixClusterVersions(cluster) {
		if version != "2.11.17" {
			t.Fatalf("initial node %d version=%s", node, version)
		}
	}
	ctx, done := context.WithTimeout(context.Background(), 2*time.Minute)
	defer done()
	js, err := jetstream.New(cluster.Clients[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := waitMixedAckMetadata(ctx, js.AccountInfo); err != nil {
		t.Fatal(err)
	}
	stream, err := waitMixedAckMetadata(ctx, func(ctx context.Context) (jetstream.Stream, error) {
		return js.CreateStream(ctx, jetstream.StreamConfig{Name: "ACK_MULTI", Subjects: []string{"ack.multi.*"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: 3})
	})
	if err != nil {
		t.Fatal(err)
	}
	consumers := make([]jetstream.Consumer, 2)
	for i := range consumers {
		consumers[i], err = waitMixedAckMetadata(ctx, func(ctx context.Context) (jetstream.Consumer, error) {
			return stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Durable: fmt.Sprintf("ACK_MULTI_%d", i), FilterSubject: fmt.Sprintf("ack.multi.%d", i), AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	preferStream := func(node int) {
		t.Helper()
		preferMixedAckLeader(t, ctx, cluster.Clients[2], "$JS.API.STREAM.LEADER.STEPDOWN.ACK_MULTI", fmt.Sprintf("wf-process-%d", node), func(ctx context.Context) (*jetstream.ClusterInfo, error) {
			info, err := stream.Info(ctx)
			if err != nil {
				return nil, err
			}
			return info.Cluster, nil
		})
	}
	preferConsumer := func(i, node int) {
		t.Helper()
		preferMixedAckLeader(t, ctx, cluster.Clients[2], fmt.Sprintf("$JS.API.CONSUMER.LEADER.STEPDOWN.ACK_MULTI.ACK_MULTI_%d", i), fmt.Sprintf("wf-process-%d", node), func(ctx context.Context) (*jetstream.ClusterInfo, error) {
			info, err := consumers[i].Info(ctx)
			if err != nil {
				return nil, err
			}
			return info.Cluster, nil
		})
	}
	preferStream(2)
	preferConsumer(0, 1)
	preferConsumer(1, 0)
	const count = 33
	publishFetch := func(round int) [2][]jetstream.Msg {
		t.Helper()
		var delivered [2][]jetstream.Msg
		for i := range consumers {
			for n := 0; n < count; n++ {
				if _, err := js.Publish(ctx, fmt.Sprintf("ack.multi.%d", i), []byte(fmt.Sprintf("round-%d-consumer-%d-record-%d", round, i, n))); err != nil {
					t.Fatal(err)
				}
			}
			batch, err := consumers[i].Fetch(count, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			for message := range batch.Messages() {
				delivered[i] = append(delivered[i], message)
			}
			if batch.Error() != nil || len(delivered[i]) != count {
				t.Fatalf("consumer %d deliveries=%d error=%v", i, len(delivered[i]), batch.Error())
			}
		}
		return delivered
	}
	ackBoth := func(delivered [2][]jetstream.Msg) {
		t.Helper()
		for parity := 0; parity < 2; parity++ {
			for n := parity; n < count; n += 2 {
				for i := range consumers {
					attempt, stop := context.WithTimeout(ctx, 2*time.Second)
					err := delivered[i][n].DoubleAck(attempt)
					stop()
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	readProgress := func(leaders [2]string, deliveries uint64) {
		t.Helper()
		for i, consumer := range consumers {
			info, err := waitMixedAckMetadata(ctx, consumer.Info)
			if err != nil || info == nil || info.NumPending != 0 || info.NumAckPending != 0 || info.Delivered.Consumer != deliveries || info.AckFloor.Consumer != deliveries || info.Cluster == nil || info.Cluster.Leader != leaders[i] {
				t.Fatalf("consumer %d progress=%+v err=%v", i, info, err)
			}
		}
	}
	assertRaw := func(delivered [2][]jetstream.Msg, retained [2]bool) {
		t.Helper()
		for i := range delivered {
			for _, message := range delivered[i] {
				metadata, err := message.Metadata()
				if err != nil {
					t.Fatal(err)
				}
				attempt, stop := context.WithTimeout(ctx, time.Second)
				raw, err := stream.GetMsg(attempt, metadata.Sequence.Stream)
				stop()
				if retained[i] {
					if err != nil || raw == nil || string(raw.Data) != string(message.Data()) {
						t.Fatalf("retained consumer %d seq=%d raw=%+v err=%v", i, metadata.Sequence.Stream, raw, err)
					}
				} else if !errors.Is(err, jetstream.ErrMsgNotFound) {
					t.Fatalf("consumer %d seq=%d removal unconfirmed: %v", i, metadata.Sequence.Stream, err)
				}
			}
		}
	}
	awaitEmpty := func() {
		t.Helper()
		until := time.Now().Add(30 * time.Second)
		for time.Now().Before(until) && ctx.Err() == nil {
			attempt, stop := context.WithTimeout(ctx, time.Second)
			info, err := stream.Info(attempt)
			stop()
			if err == nil && info.State.Msgs == 0 {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		captureMatrixQueueDiagnostics(t, stream)
		t.Fatal("multi-consumer stream did not drain within thirty seconds")
	}
	delivered := publishFetch(1)
	if err := cluster.KillNode(1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.UpgradeNode(1); err != nil {
		t.Fatal(err)
	}
	preferConsumer(0, 1)
	preferConsumer(1, 0)
	preferStream(2)
	ackBoth(delivered)
	// Keep the same thirty-second allowance as the strict conformance fixture.
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(30 * time.Second):
	}
	readProgress([2]string{"wf-process-1", "wf-process-0"}, count)
	info, err := waitMixedAckMetadata(ctx, func(ctx context.Context) (*jetstream.StreamInfo, error) { return stream.Info(ctx) })
	if err != nil || info == nil || info.Cluster == nil || info.State.Msgs != count || info.Cluster.Leader != "wf-process-2" {
		t.Fatalf("expected split-version retention absent: stream=%+v err=%v", info, err)
	}
	assertRaw(delivered, [2]bool{true, false})
	t.Logf("ACK_MULTI_EXPECTED_BAD_STATE versions=%v retained=%d consumer_leaders=[new1 old0] stream_leader=old2", matrixClusterVersions(cluster), info.State.Msgs)
	if prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX"); prefix != "" {
		t.Setenv("MATRIX_ARTIFACT_PREFIX", prefix+"-before-move")
		captureMatrixQueueDiagnostics(t, stream)
		t.Setenv("MATRIX_ARTIFACT_PREFIX", prefix)
	}
	preferStream(1)
	awaitEmpty()
	assertRaw(delivered, [2]bool{false, false})
	readProgress([2]string{"wf-process-1", "wf-process-0"}, count)
	// Now exercise two upgraded consumer leaders on distinct nodes, one remote
	// from the upgraded stream leader. Removal must work for the remote upgraded consumer leader.
	if err := cluster.KillNode(0); err != nil {
		t.Fatal(err)
	}
	if err := cluster.UpgradeNode(0); err != nil {
		t.Fatal(err)
	}
	preferConsumer(1, 0)
	preferConsumer(0, 1)
	preferStream(1)
	if versions := matrixClusterVersions(cluster); versions[0] != "2.15.0" || versions[1] != "2.15.0" || versions[2] != "2.11.17" {
		t.Fatalf("second-stage versions=%v", versions)
	}
	delivered = publishFetch(2)
	ackBoth(delivered)
	awaitEmpty()
	assertRaw(delivered, [2]bool{false, false})
	readProgress([2]string{"wf-process-1", "wf-process-0"}, 2*count)
	info, err = waitMixedAckMetadata(ctx, func(ctx context.Context) (*jetstream.StreamInfo, error) { return stream.Info(ctx) })
	if err != nil || info == nil || info.Cluster == nil || info.Cluster.Leader != "wf-process-1" {
		t.Fatalf("final stream leader changed: stream=%+v err=%v", info, err)
	}
	t.Logf("ACK_MULTI_RECOVERY versions=%v rounds=2 messages=132 raw_removed=132 stream_leader=new1 consumer_leaders=[new1 new0]", matrixClusterVersions(cluster))
}
