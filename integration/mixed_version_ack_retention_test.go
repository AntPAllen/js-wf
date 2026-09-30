//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Isolate explicit-ack WorkQueue retention from workflow code. The stream
// leader stays old while a consumer with delivered messages moves to a newly
// upgraded leader; acknowledge out of order to cover advancing ack floors.
func TestMixedVersionExplicitAckWorkQueueRetention(t *testing.T) {
	old := os.Getenv("WF_NATS_SERVER_BIN")
	if old == "" {
		t.Skip("requires NATS 2.11.17 through WF_NATS_SERVER_BIN")
	}
	for _, mode := range []string{"old-control", "mixed-ack", "mixed-double-ack", "mixed-co-located", "mixed-inherited-replicas"} {
		t.Run(mode, func(t *testing.T) {
			if prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX"); prefix != "" {
				t.Setenv("MATRIX_ARTIFACT_PREFIX", prefix+"-"+mode)
			}
			cluster, err := testcluster.StartMixedVersionProcesses(t.TempDir(), []string{old, old, old})
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			for node, version := range matrixClusterVersions(cluster) {
				if version != "2.11.17" {
					t.Fatalf("initial node %d version=%s want=2.11.17", node, version)
				}
			}
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
			ctx, done := context.WithTimeout(context.Background(), 90*time.Second)
			defer done()
			nc := cluster.Clients[2]
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			_, err = waitMixedAckMetadata(ctx, func(attempt context.Context) (*jetstream.AccountInfo, error) { return js.AccountInfo(attempt) })
			if err != nil {
				t.Fatal(err)
			}
			stream, err := waitMixedAckMetadata(ctx, func(attempt context.Context) (jetstream.Stream, error) {
				return js.CreateStream(attempt, jetstream.StreamConfig{Name: "ACK_CONTRACT", Subjects: []string{"ack.jobs.32"}, Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: 3})
			})
			if err != nil {
				t.Fatal(err)
			}
			consumerReplicas := 3
			if mode == "mixed-inherited-replicas" {
				consumerReplicas = 0
			}
			consumer, err := waitMixedAckMetadata(ctx, func(attempt context.Context) (jetstream.Consumer, error) {
				return stream.CreateConsumer(attempt, jetstream.ConsumerConfig{Durable: "ACK_P_32", FilterSubject: "ack.jobs.32", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, Replicas: consumerReplicas})
			})
			if err != nil {
				t.Fatal(err)
			}
			preferMixedAckLeader(t, ctx, nc, "$JS.API.STREAM.LEADER.STEPDOWN.ACK_CONTRACT", "wf-process-2", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
				info, err := stream.Info(ctx)
				if err != nil {
					return nil, err
				}
				return info.Cluster, nil
			})
			preferMixedAckLeader(t, ctx, nc, "$JS.API.CONSUMER.LEADER.STEPDOWN.ACK_CONTRACT.ACK_P_32", "wf-process-1", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
				info, err := consumer.Info(ctx)
				if err != nil {
					return nil, err
				}
				return info.Cluster, nil
			})
			const count = 33
			for i := 0; i < count; i++ {
				if _, err := js.Publish(ctx, "ack.jobs.32", []byte(fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
			}
			batch, err := consumer.Fetch(count, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			var messages []jetstream.Msg
			for message := range batch.Messages() {
				messages = append(messages, message)
			}
			if batch.Error() != nil || len(messages) != count {
				t.Fatalf("delivered=%d err=%v", len(messages), batch.Error())
			}
			if mode != "old-control" {
				if err := cluster.KillNode(1); err != nil {
					t.Fatal(err)
				}
				if err := cluster.UpgradeNode(1); err != nil {
					t.Fatal(err)
				}
				preferMixedAckLeader(t, ctx, nc, "$JS.API.CONSUMER.LEADER.STEPDOWN.ACK_CONTRACT.ACK_P_32", "wf-process-1", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
					info, err := consumer.Info(ctx)
					if err != nil {
						return nil, err
					}
					return info.Cluster, nil
				})
			}
			if mode == "mixed-co-located" {
				preferMixedAckLeader(t, ctx, nc, "$JS.API.STREAM.LEADER.STEPDOWN.ACK_CONTRACT", "wf-process-1", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
					info, err := stream.Info(ctx)
					if err != nil {
						return nil, err
					}
					return info.Cluster, nil
				})
			}
			// Ack evens before odds, then require both consumer progress and actual
			// stream removal. DoubleAck confirms the consumer reply, not deletion.
			for parity := 0; parity < 2; parity++ {
				for i := parity; i < count; i += 2 {
					if mode == "mixed-double-ack" {
						attempt, stop := context.WithTimeout(ctx, 2*time.Second)
						err = messages[i].DoubleAck(attempt)
						stop()
					} else {
						err = messages[i].Ack()
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			until := time.Now().Add(30 * time.Second)
			var info *jetstream.StreamInfo
			var ci *jetstream.ConsumerInfo
			for time.Now().Before(until) && ctx.Err() == nil {
				attempt, stop := context.WithTimeout(ctx, time.Second)
				info, err = stream.Info(attempt)
				if err == nil {
					ci, err = consumer.Info(attempt)
				}
				stop()
				if err == nil && info.State.Msgs == 0 && ci.NumAckPending == 0 && ci.NumPending == 0 && ci.AckFloor.Stream == count {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if err != nil || info == nil || ci == nil || info.State.Msgs != 0 || ci.NumAckPending != 0 || ci.NumPending != 0 || ci.AckFloor.Stream != count {
				captureMatrixQueueDiagnostics(t, stream)
				t.Fatalf("retention mode=%s stream=%+v consumer=%+v err=%v", mode, info, ci, err)
			}
			t.Logf("ACK_RETENTION mode=%s versions=%v stream_leader=%s consumer_leader=%s ack_floor=%d retained=%d", mode, matrixClusterVersions(cluster), info.Cluster.Leader, ci.Cluster.Leader, ci.AckFloor.Stream, info.State.Msgs)
		})
	}
}

func preferMixedAckLeader(t *testing.T, ctx context.Context, nc *nats.Conn, api, preferred string, lookup func(context.Context) (*jetstream.ClusterInfo, error)) {
	t.Helper()
	info, err := waitMixedAckMetadata(ctx, lookup)
	if err != nil || info == nil {
		t.Fatalf("leader lookup: %+v %v", info, err)
	}
	if info.Leader != preferred {
		data, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": preferred}})
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		reply, err := nc.RequestWithContext(attempt, api, data)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			Success bool `json:"success"`
		}
		if json.Unmarshal(reply.Data, &response) != nil || !response.Success {
			t.Fatalf("preferred leader rejected: %s", reply.Data)
		}
	}
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		info, err = lookup(attempt)
		stop()
		if err == nil && info != nil && info.Leader == preferred && len(info.Replicas) == 2 {
			ready := true
			for _, replica := range info.Replicas {
				ready = ready && replica.Current && !replica.Offline
			}
			if ready {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("preferred leader=%s info=%+v err=%v", preferred, info, err)
}

// Listening for clients does not prove the old cluster has elected its metadata
// leader. Retry bounded read attempts during fixture readiness only.
func waitMixedAckMetadata[T any](ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	bound, done := context.WithTimeout(ctx, 20*time.Second)
	defer done()
	var value T
	var err error
	for bound.Err() == nil {
		value, err = matrixReadMetadata(bound, read)
		var api *jetstream.APIError
		// Initial R3 creation can precede metadata peer eligibility, even after
		// account readiness. The fixed three-node fixture must become placeable.
		placementPending := errors.As(err, &api) && api.ErrorCode == 10005
		if err == nil || (!matrixTransientTransport(err) && !placementPending) {
			return value, err
		}
		select {
		case <-bound.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	return value, bound.Err()
}
