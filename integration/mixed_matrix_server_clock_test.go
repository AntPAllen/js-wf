//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

var matrixClockStreams = []string{"WF_INV", "WF_RUN", "WF_JRN", "WF_SIG", "KV_WF_STATE", "KV_WF_LEASE"}

type matrixServerClockSample struct {
	Node           int           `json:"node"`
	ServerAt       time.Time     `json:"server_at"`
	MeasuredOffset time.Duration `json:"offset_ns"`
}

func preferMatrixSkewLeaders(ctx context.Context, nc *nats.Conn, js jetstream.JetStream) error {
	const leader = "wf-process-2"
	for _, name := range matrixClockStreams {
		stream, err := js.Stream(ctx, name)
		if err != nil {
			return err
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return err
		}
		if info.Cluster == nil {
			return fmt.Errorf("%s lacks cluster information", name)
		}
		if info.Cluster.Leader != leader {
			payload, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": leader}})
			attempt, done := context.WithTimeout(ctx, 5*time.Second)
			reply, err := nc.RequestWithContext(attempt, "$JS.API.STREAM.LEADER.STEPDOWN."+name, payload)
			done()
			if err != nil {
				return err
			}
			var response struct {
				Success bool `json:"success"`
			}
			if json.Unmarshal(reply.Data, &response) != nil || !response.Success {
				return fmt.Errorf("%s preferred leader rejected: %s", name, reply.Data)
			}
		}
		until := time.Now().Add(20 * time.Second)
		for time.Now().Before(until) && ctx.Err() == nil {
			attempt, done := context.WithTimeout(ctx, 2*time.Second)
			info, err = stream.Info(attempt)
			done()
			if err == nil && info.Cluster != nil && info.Cluster.Leader == leader {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil || info.Cluster == nil || info.Cluster.Leader != leader {
			return fmt.Errorf("%s failed to elect skewed leader: %+v %v", name, info, err)
		}
	}
	return nil
}

func verifyMatrixServerClocks(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, offset time.Duration, scheduled time.Time) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: 2}
	for node := 0; node < 3; node++ {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		now, err := cluster.ServerNow(attempt, node)
		done()
		measured := now.Sub(time.Now())
		want := time.Duration(0)
		if node == 2 {
			want = offset
		}
		if err != nil || measured < want-2*time.Second || measured > want+2*time.Second {
			return event, fmt.Errorf("node %d clock=%s want=%s±2s err=%v", node, measured, want, err)
		}
		event.ServerClocks = append(event.ServerClocks, matrixServerClockSample{Node: node, ServerAt: now, MeasuredOffset: measured})
	}
	for _, name := range matrixClockStreams {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		stream, err := js.Stream(attempt, name)
		var info *jetstream.StreamInfo
		if err == nil {
			info, err = stream.Info(attempt)
		}
		done()
		if err != nil || info.Cluster == nil || info.Cluster.Leader != "wf-process-2" {
			return event, fmt.Errorf("%s left the verified skewed leader: %+v %v", name, info, err)
		}
	}
	event.Healed = time.Now()
	return event, nil
}
