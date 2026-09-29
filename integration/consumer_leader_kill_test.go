package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// WF_REPEATED_CONSUMER_KILL=1 kills and restarts the current durable consumer
// leader three times while a single hot partition has a large live backlog.
func TestRepeatedConsumerLeaderKillsDuringBacklog(t *testing.T) {
	if os.Getenv("WF_REPEATED_CONSUMER_KILL") == "" {
		t.Skip("set WF_REPEATED_CONSUMER_KILL=1 for repeated consumer leader kills")
	}
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const typ = "consumerkills"
	partition := identity.Partition(typ, "job-0", provision.Partitions)
	ids := make([]string, 0, 1000)
	for candidate := 0; len(ids) < cap(ids); candidate++ {
		id := fmt.Sprintf("job-%d", candidate)
		if identity.Partition(typ, id, provision.Partitions) == partition {
			ids = append(ids, id)
		}
	}
	var active, calls atomic.Int64
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		calls.Add(1)
		active.Add(1)
		defer active.Add(-1)
		select {
		case <-time.After(50 * time.Millisecond):
			return json.RawMessage(`1`), nil
		case <-c.Context().Done():
			return nil, c.Context().Err()
		}
	}
	urls := make([]string, len(cluster.Servers))
	for i, server := range cluster.Servers {
		urls[i] = server.ClientURL()
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.ReconnectWait(50*time.Millisecond), nats.MaxReconnects(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	workerJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, workerJS, "repeated-leader-worker", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, partition) }()
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	for i, id := range ids {
		if _, err := clients[i%len(clients)].Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	name := fmt.Sprintf("WF_P_%02d", partition)
	ready := func() *jetstream.ConsumerInfo {
		t.Helper()
		until := time.Now().Add(20 * time.Second)
		for {
			attempt, done := context.WithTimeout(ctx, time.Second)
			consumer, err := all[0].Consumer(attempt, "WF_RUN", name)
			var info *jetstream.ConsumerInfo
			if err == nil {
				info, err = consumer.Info(attempt)
			}
			var streamInfo *jetstream.StreamInfo
			if err == nil {
				var run jetstream.Stream
				run, err = all[0].Stream(attempt, "WF_RUN")
				if err == nil {
					streamInfo, err = run.Info(attempt)
				}
			}
			valid := err == nil && info != nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2 && streamInfo != nil && streamInfo.Cluster != nil && len(streamInfo.Cluster.Replicas) == 2
			if valid {
				for _, replica := range info.Cluster.Replicas {
					valid = valid && replica.Current && !replica.Offline
				}
				for _, replica := range streamInfo.Cluster.Replicas {
					valid = valid && replica.Current && !replica.Offline
				}
			}
			for node, server := range cluster.Servers {
				routes, routeErr := server.Routez(nil)
				if routeErr != nil {
					valid = false
					continue
				}
				peers := make(map[string]bool)
				for _, route := range routes.Routes {
					peers[route.RemoteName] = true
				}
				for other, peer := range cluster.Servers {
					if other != node {
						valid = valid && peers[peer.Name()]
					}
				}
			}
			done()
			if valid {
				return info
			}
			if time.Now().After(until) || ctx.Err() != nil {
				t.Fatalf("consumer replicas not ready: consumer=%+v stream=%+v err=%v", info, streamInfo, err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	killed := make([]string, 0, 3)
	for round := 0; round < 3; round++ {
		info := ready()
		until := time.Now().Add(45 * time.Second)
		for active.Load() == 0 {
			if time.Now().After(until) || ctx.Err() != nil {
				var workerErr any
				exited := false
				select {
				case workerErr = <-workerDone:
					exited = true
				default:
				}
				attempt, done := context.WithTimeout(context.Background(), 2*time.Second)
				view, viewErr := all[0].Consumer(attempt, "WF_RUN", name)
				var consumerInfo *jetstream.ConsumerInfo
				if viewErr == nil {
					consumerInfo, viewErr = view.Info(attempt)
				}
				run, runErr := all[0].Stream(attempt, "WF_RUN")
				var runInfo *jetstream.StreamInfo
				if runErr == nil {
					runInfo, runErr = run.Info(attempt)
				}
				done()
				stack := make([]byte, 1<<20)
				stack = stack[:runtime.Stack(stack, true)]
				for _, goroutine := range strings.Split(string(stack), "\n\n") {
					if strings.Contains(goroutine, "js-wf/worker.(*Worker).") {
						t.Logf("worker stack:\n%s", goroutine)
					}
				}
				t.Fatalf("no handler active before leader kill %d: calls=%d connection=%s worker_exited=%t worker_err=%v consumer=%+v consumer_err=%v stream=%+v stream_err=%v metrics=%+v", round, calls.Load(), nc.Status(), exited, workerErr, consumerInfo, viewErr, runInfo, runErr, w.Metrics())
			}
			time.Sleep(time.Millisecond)
		}
		leader := -1
		for node, server := range cluster.Servers {
			if server.Name() == info.Cluster.Leader {
				leader = node
			}
		}
		if leader < 0 {
			t.Fatalf("unknown consumer leader %q", info.Cluster.Leader)
		}
		cluster.KillNode(leader)
		killed = append(killed, info.Cluster.Leader)
		other := (leader + 1) % 3
		until = time.Now().Add(20 * time.Second)
		for {
			attempt, done := context.WithTimeout(ctx, time.Second)
			consumer, err := all[other].Consumer(attempt, "WF_RUN", name)
			var elected *jetstream.ConsumerInfo
			if err == nil {
				elected, err = consumer.Info(attempt)
			}
			done()
			if err == nil && elected.Cluster != nil && elected.Cluster.Leader != "" && elected.Cluster.Leader != info.Cluster.Leader {
				break
			}
			if time.Now().After(until) || ctx.Err() != nil {
				t.Fatalf("consumer leader did not move after kill %d: info=%+v err=%v", round, elected, err)
			}
			time.Sleep(25 * time.Millisecond)
		}
		if err := cluster.RestartNode(leader); err != nil {
			t.Fatal(err)
		}
		all[leader], err = jetstream.New(cluster.Clients[leader])
		if err != nil {
			t.Fatal(err)
		}
		ready()
	}
	reader := client.New(all[0])
	t.Logf("after kills: handler_calls=%d connection=%s worker_metrics=%+v", calls.Load(), nc.Status(), w.Metrics())
	for _, id := range ids {
		value, err := reader.Await(ctx, typ, id)
		if err != nil || string(value) != "1" {
			var workerErr any
			select {
			case workerErr = <-workerDone:
			default:
			}
			stack := make([]byte, 1<<20)
			stack = stack[:runtime.Stack(stack, true)]
			for _, goroutine := range strings.Split(string(stack), "\n\n") {
				if strings.Contains(goroutine, "js-wf/worker.(*Worker).") {
					t.Logf("worker stack:\n%s", goroutine)
				}
			}
			t.Fatalf("await %s: value=%s err=%v handler_calls=%d connection=%s worker_err=%v metrics=%+v", id, value, err, calls.Load(), nc.Status(), workerErr, w.Metrics())
		}
	}
	for {
		view, err := all[0].Stream(ctx, "WF_RUN")
		if err == nil {
			state, infoErr := view.Info(ctx)
			if infoErr == nil && state.State.Msgs == 0 {
				break
			}
		}
		if ctx.Err() != nil {
			t.Fatal("run queue did not drain after repeated leader kills")
		}
		time.Sleep(25 * time.Millisecond)
	}
	stop()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Terminal != len(ids) || report.Invocations != len(ids) {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
	t.Logf("killed consumer leaders=%v completed=%d handler_calls=%d redeliveries=%d", killed, len(ids), calls.Load(), w.Metrics().Redeliveries)
}

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
	consumer, err := run.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: name, Durable: name, FilterSubject: "wf.run." + strconv.FormatUint(uint64(partition), 10), AckPolicy: jetstream.AckExplicitPolicy, AckWait: worker.DefaultAckWait, MaxDeliver: -1, MaxAckPending: 1000})
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
