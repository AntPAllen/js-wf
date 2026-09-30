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
	for _, mode := range []string{"old-control", "mixed-ack", "mixed-double-ack", "mixed-co-located", "mixed-inherited-replicas", "mixed-move-after-ack"} {
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
			caseTimeout := 90 * time.Second
			if mode == "mixed-move-after-ack" {
				caseTimeout = 120 * time.Second
			}
			ctx, done := context.WithTimeout(context.Background(), caseTimeout)
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
			if mode == "mixed-inherited-replicas" || mode == "mixed-move-after-ack" {
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
			var info *jetstream.StreamInfo
			var ci *jetstream.ConsumerInfo
			waitDrain := func() {
				until := time.Now().Add(30 * time.Second)
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
			}
			waitDrain()
			if mode == "mixed-move-after-ack" {
				// Require the observed bad state before trying a recovery action.
				// Co-location before ack is already a passing control; it does not
				// establish that a leader move afterward repairs retained records.
				if err != nil || info == nil || ci == nil || info.State.Msgs != count || ci.NumAckPending != 0 || ci.NumPending != 0 || ci.AckFloor.Stream != count || info.Cluster == nil || ci.Cluster == nil || info.Cluster.Leader != "wf-process-2" || ci.Cluster.Leader != "wf-process-1" {
					t.Fatalf("recovery precondition absent: stream=%+v consumer=%+v err=%v", info, ci, err)
				}
				t.Logf("ACK_RETENTION_BEFORE_MOVE versions=%v stream_leader=%s consumer_leader=%s ack_floor=%d retained=%d", matrixClusterVersions(cluster), info.Cluster.Leader, ci.Cluster.Leader, ci.AckFloor.Stream, info.State.Msgs)
				if prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX"); prefix != "" {
					t.Setenv("MATRIX_ARTIFACT_PREFIX", prefix+"-before-move")
					captureMatrixQueueDiagnostics(t, stream)
					t.Setenv("MATRIX_ARTIFACT_PREFIX", prefix)
				}
				preferMixedAckLeader(t, ctx, nc, "$JS.API.STREAM.LEADER.STEPDOWN.ACK_CONTRACT", "wf-process-1", func(ctx context.Context) (*jetstream.ClusterInfo, error) {
					info, err := stream.Info(ctx)
					if err != nil {
						return nil, err
					}
					return info.Cluster, nil
				})
				waitDrain()
			}
			if err != nil || info == nil || ci == nil || info.State.Msgs != 0 || ci.NumAckPending != 0 || ci.NumPending != 0 || ci.AckFloor.Stream != count {
				captureMatrixQueueDiagnostics(t, stream)
				t.Fatalf("retention mode=%s stream=%+v consumer=%+v err=%v", mode, info, ci, err)
			}
			if mode == "mixed-move-after-ack" && (info.Cluster.Leader != "wf-process-1" || ci.Cluster.Leader != "wf-process-1") {
				t.Fatalf("recovery leader placement changed: stream=%+v consumer=%+v", info.Cluster, ci.Cluster)
			}
			if mode == "mixed-move-after-ack" {
				for sequence := uint64(1); sequence <= count; sequence++ {
					attempt, stop := context.WithTimeout(ctx, time.Second)
					_, readErr := stream.GetMsg(attempt, sequence)
					stop()
					if !errors.Is(readErr, jetstream.ErrMsgNotFound) {
						t.Fatalf("post-move raw sequence %d still present or unconfirmed: %v", sequence, readErr)
					}
				}
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
	data, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": preferred}})
	var requested time.Time
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		info, err = lookup(attempt)
		stop()
		ready := err == nil && info != nil && len(info.Replicas) == 2
		if ready {
			for _, replica := range info.Replicas {
				ready = ready && replica.Current && !replica.Offline
			}
		}
		if ready && info.Leader == preferred {
			return
		}
		// A restart can report readiness before the consumer replica can win
		// its preferred election. Do not churn an uncaught-up group; retry the
		// placement only after every peer is current, within the same bound.
		if ready && time.Since(requested) >= time.Second {
			requested = time.Now()
			attempt, stop := context.WithTimeout(ctx, time.Second)
			reply, requestErr := nc.RequestWithContext(attempt, api, data)
			stop()
			if requestErr != nil && !matrixTransientTransport(requestErr) {
				t.Fatal(requestErr)
			}
			if requestErr == nil {
				var response struct {
					Success bool `json:"success"`
				}
				if json.Unmarshal(reply.Data, &response) != nil || !response.Success {
					t.Fatalf("preferred leader rejected: %s", reply.Data)
				}
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
