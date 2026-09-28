package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumerLeaderKillDuringInFlightWorkflow(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	const typ = "consumerkill"
	partition := identity.Partition(typ, "job-0", provision.Partitions)
	var ids []string
	for candidate := 0; len(ids) < 100; candidate++ {
		id := fmt.Sprintf("job-%d", candidate)
		if identity.Partition(typ, id, provision.Partitions) == partition {
			ids = append(ids, id)
		}
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("WF_P_%02d", partition)
	consumer, err := run.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: name, Durable: name, FilterSubject: "wf.run." + strconv.FormatUint(uint64(partition), 10), AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, MaxDeliver: -1, MaxAckPending: 1000})
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(ctx)
	if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
		t.Fatalf("consumer leader: info=%+v err=%v", info, err)
	}
	leader := -1
	for index, server := range cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			leader = index
			break
		}
	}
	if leader < 0 {
		t.Fatalf("unknown consumer leader %q", info.Cluster.Leader)
	}
	workerNode := (leader + 1) % 3
	starters := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	for index, id := range ids {
		if _, err := starters[index%len(starters)].Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var effects atomic.Int64
	w, err := worker.New(ctx, all[workerNode], "consumer-kill-worker", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		if effects.Add(1) == 1 {
			close(entered)
			<-release
		}
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, partition) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("worker exited before leader kill: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	readyUntil := time.Now().Add(15 * time.Second)
	for {
		current, err := consumer.Info(ctx)
		ready := err == nil && current.Cluster != nil && current.Cluster.Leader == info.Cluster.Leader && len(current.Cluster.Replicas) == 2
		if ready {
			for _, replica := range current.Cluster.Replicas {
				ready = ready && replica.Current && !replica.Offline
			}
		}
		streamInfo, streamErr := run.Info(ctx)
		ready = ready && streamErr == nil && streamInfo.Cluster != nil && len(streamInfo.Cluster.Replicas) == 2
		if ready {
			for _, replica := range streamInfo.Cluster.Replicas {
				ready = ready && replica.Current && !replica.Offline
			}
		}
		for node, server := range cluster.Servers {
			routes, routeErr := server.Routez(nil)
			if routeErr != nil {
				ready = false
				continue
			}
			peers := map[string]bool{}
			for _, route := range routes.Routes {
				peers[route.RemoteName] = true
			}
			for other, peer := range cluster.Servers {
				if other != node {
					ready = ready && peers[peer.Name()]
				}
			}
		}
		if ready {
			break
		}
		if time.Now().After(readyUntil) || ctx.Err() != nil {
			var consumerCluster, streamCluster any
			if current != nil {
				consumerCluster = current.Cluster
			}
			if streamInfo != nil {
				streamCluster = streamInfo.Cluster
			}
			t.Fatalf("consumer or stream replicas not current before kill: consumer_cluster=%+v err=%v stream_cluster=%+v err=%v routes=%d/%d/%d", consumerCluster, err, streamCluster, streamErr, cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
		}
		time.Sleep(25 * time.Millisecond)
	}
	cluster.KillNode(leader)
	survivor := all[workerNode]
	otherNode := (leader + 2) % 3
	electionUntil := time.Now().Add(20 * time.Second)
	var lastConsumers [3]*jetstream.ConsumerInfo
	var lastErrors [3]error
	for {
		elected := false
		for _, node := range []int{workerNode, otherNode} {
			attempt, done := context.WithTimeout(ctx, time.Second)
			current, err := all[node].Consumer(attempt, "WF_RUN", name)
			if err == nil {
				status, infoErr := current.Info(attempt)
				lastConsumers[node], lastErrors[node] = status, infoErr
				if infoErr == nil && status.Cluster != nil && status.Cluster.Leader != "" && status.Cluster.Leader != info.Cluster.Leader {
					elected = true
				}
			} else {
				lastErrors[node] = err
			}
			done()
		}
		if elected {
			break
		}
		if time.Now().After(electionUntil) || ctx.Err() != nil {
			var streamInfo [3]*jetstream.StreamInfo
			var streamErr [3]error
			for _, node := range []int{workerNode, otherNode} {
				attempt, done := context.WithTimeout(context.Background(), time.Second)
				view, err := all[node].Stream(attempt, "WF_RUN")
				if err == nil {
					streamInfo[node], streamErr[node] = view.Info(attempt)
				} else {
					streamErr[node] = err
				}
				done()
			}
			t.Fatalf("consumer did not elect a surviving leader: consumers=%+v errors=%v streams=%+v stream_errors=%v routes=%d/%d/%d", lastConsumers, lastErrors, streamInfo, streamErr, cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
		}
		time.Sleep(25 * time.Millisecond)
	}
	close(release)
	reader := client.New(survivor)
	for _, id := range ids {
		result, err := reader.Await(ctx, typ, id)
		if err != nil || string(result) != "1" {
			select {
			case workerErr := <-done:
				t.Fatalf("await %s: result=%s err=%v worker=%v", id, result, err, workerErr)
			default:
				t.Fatalf("await %s: result=%s err=%v", id, result, err)
			}
		}
	}
	for {
		view, err := survivor.Stream(ctx, "WF_RUN")
		if err == nil {
			state, infoErr := view.Info(ctx)
			if infoErr == nil && state.State.Msgs == 0 {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal("run queue did not drain after leader kill")
		}
		time.Sleep(25 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := integrity.Check(ctx, survivor); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RestartNode(leader); err != nil {
		t.Fatal(err)
	}
	rejoined, err := jetstream.New(cluster.Clients[leader])
	if err != nil {
		t.Fatal(err)
	}
	rejoinUntil := time.Now().Add(20 * time.Second)
	var lastRejoined *jetstream.StreamInfo
	var lastRejoinErr error
	for {
		attempt, done := context.WithTimeout(ctx, time.Second)
		view, err := rejoined.Stream(attempt, "WF_RUN")
		if err == nil {
			state, infoErr := view.Info(attempt)
			lastRejoined, lastRejoinErr = state, infoErr
			ready := infoErr == nil && state.State.Msgs == 0 && state.Cluster != nil && len(state.Cluster.Replicas) == 2
			if ready {
				for _, replica := range state.Cluster.Replicas {
					ready = ready && replica.Current && !replica.Offline
				}
			}
			if ready {
				done()
				break
			}
		} else {
			lastRejoinErr = err
		}
		done()
		if time.Now().After(rejoinUntil) || ctx.Err() != nil {
			var clusterInfo any
			var messages uint64
			if lastRejoined != nil {
				clusterInfo = lastRejoined.Cluster
				messages = lastRejoined.State.Msgs
			}
			t.Fatalf("restarted consumer leader did not catch up: cluster=%+v messages=%d err=%v routes=%d/%d/%d", clusterInfo, messages, lastRejoinErr, cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := integrity.Check(ctx, rejoined); err != nil {
		t.Fatalf("rejoined node integrity: %v", err)
	}
	t.Logf("consumer leader %d killed during first handler; completed %d invocations with %d handler calls", leader, len(ids), effects.Load())
}
