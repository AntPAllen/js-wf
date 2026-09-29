//go:build !windows

package testcluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestProcessClusterPauseLeaderAndRecoverReplica(t *testing.T) {
	c, err := StartProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range c.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	var stream jetstream.Stream
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		stream, err = js[0].CreateStream(attempt, jetstream.StreamConfig{Name: "PAUSE_TEST", Subjects: []string{"pause.test"}, Replicas: 3, Storage: jetstream.FileStorage})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create three-replica stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil || !strings.HasPrefix(info.Cluster.Leader, "wf-process-") {
		t.Fatalf("stream leader: info=%+v err=%v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("invalid stream leader %q: %v", info.Cluster.Leader, err)
	}
	first, err := js[leader].Publish(ctx, "pause.test", []byte("before"))
	if err != nil || first == nil || first.Sequence != 1 {
		t.Fatalf("first publish=%+v err=%v", first, err)
	}
	if err := (FaultSchedule{Seed: 42, Events: []FaultEvent{{Op: PauseNode, A: leader}}}).Run(ctx, c.ApplyFault); err != nil {
		t.Fatal(err)
	}
	if err := c.Clients[leader].FlushTimeout(200 * time.Millisecond); err == nil {
		t.Fatal("paused server still responded to a client flush")
	}
	majority := (leader + 1) % 3
	var second *jetstream.PubAck
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		second, err = js[majority].Publish(attempt, "pause.test", []byte("during"))
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || second == nil || second.Sequence != 2 {
		t.Fatalf("majority publish while node %d paused: ack=%+v err=%v", leader, second, err)
	}
	if err := c.ResumeNode(leader); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		conn, dialErr := c.Dial(leader)
		if dialErr == nil {
			resumed, jsErr := jetstream.New(conn)
			if jsErr == nil {
				attempt, stop := context.WithTimeout(ctx, time.Second)
				stream, jsErr := resumed.Stream(attempt, "PAUSE_TEST")
				if jsErr == nil {
					var message *jetstream.RawStreamMsg
					message, jsErr = stream.GetMsg(attempt, 2)
					if jsErr == nil && string(message.Data) == "during" {
						stop()
						conn.Close()
						return
					}
				}
				stop()
			}
			conn.Close()
			err = jsErr
		} else {
			err = dialErr
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("resumed node %d did not recover acknowledged publish: %s (log=%s)", leader, fmt.Sprint(err), c.LogPath(leader))
}
