//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFiveContainerConsumerLeaderKillDuringEffect(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	const typ, count = "tier3-consumer-kill", 10
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for node := 0; node < 5; node++ {
			if logs, err := cluster.Logs(node); err == nil {
				if len(logs) > 8192 {
					logs = logs[len(logs)-8192:]
				}
				t.Logf("node %d log tail:\n%s", node, logs)
			}
		}
	}()
	t.Logf("file_store_sync_interval=%s", cluster.SyncInterval())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	connect := func(node int) (*nats.Conn, jetstream.JetStream, error) {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			return nil, nil, err
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, nil, err
		}
		return nc, js, nil
	}
	controlConn, controlJS, err := connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer controlConn.Close()
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.Ensure(attempt, controlJS, 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision five-replica stores: %v", err)
	}
	partition := identity.Partition(typ, "job-0", provision.Partitions)
	ids := make([]string, 0, count)
	for candidate := 0; len(ids) < count; candidate++ {
		id := fmt.Sprintf("job-%d", candidate)
		if identity.Partition(typ, id, provision.Partitions) == partition {
			ids = append(ids, id)
		}
	}
	attempt, stop := context.WithTimeout(ctx, 5*time.Second)
	run, err := controlJS.Stream(attempt, "WF_RUN")
	if err == nil {
		name := fmt.Sprintf("WF_P_%02d", partition)
		_, err = run.CreateConsumer(attempt, jetstream.ConsumerConfig{Name: name, Durable: name, FilterSubject: "wf.run." + strconv.FormatUint(uint64(partition), 10), AckPolicy: jetstream.AckExplicitPolicy, AckWait: worker.DefaultAckWait, MaxDeliver: -1, MaxAckPending: 1000})
	}
	stop()
	if err != nil {
		t.Fatalf("create durable partition consumer: %v", err)
	}
	if err := waitFiveReplicaReadiness(ctx, controlJS, partition); err != nil {
		t.Fatalf("five-replica readiness before kill: %v", err)
	}
	consumerName := fmt.Sprintf("WF_P_%02d", partition)
	leaderInfo := func(js jetstream.JetStream) (*jetstream.ConsumerInfo, error) {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		consumer, err := js.Consumer(attempt, "WF_RUN", consumerName)
		if err != nil {
			return nil, err
		}
		return consumer.Info(attempt)
	}
	info, err := leaderInfo(controlJS)
	if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
		t.Fatalf("consumer leader before kill: info=%+v err=%v", info, err)
	}
	leader := -1
	for node := 0; node < 5; node++ {
		if cluster.NodeName(node) == info.Cluster.Leader {
			leader = node
			break
		}
	}
	if leader < 0 {
		t.Fatalf("unknown consumer leader %q", info.Cluster.Leader)
	}
	workerNode := (leader + 1) % 5
	workerConn, workerJS, err := connect(workerNode)
	if err != nil {
		t.Fatal(err)
	}
	defer workerConn.Close()
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_CONSUMER_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create consumer leader history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write consumer leader history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close consumer leader history: %v", err)
			}
		}
	}()
	startClient := client.NewObserved(workerJS, recorder)
	for _, id := range ids {
		if _, err := startClient.Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var effects atomic.Int64
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", 0, func(context.Context) (int, error) {
			if effects.Add(1) == 1 {
				close(entered)
				<-release
			}
			return 1, nil
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	w, err := worker.New(ctx, workerJS, "tier3-consumer-kill-worker", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workCtx, partition) }()
	workerExited := false
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		stopWork()
		if !workerExited {
			<-done
		}
	}()
	select {
	case <-entered:
	case err := <-done:
		workerExited = true
		t.Fatalf("worker exited before consumer leader kill: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := cluster.KillNode(leader); err != nil {
		t.Fatalf("kill consumer leader %d: %v", leader, err)
	}
	var elected string
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		current, readErr := leaderInfo(workerJS)
		if readErr == nil && current.Cluster != nil && current.Cluster.Leader != "" && current.Cluster.Leader != info.Cluster.Leader {
			elected = current.Cluster.Leader
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if elected == "" {
		t.Fatalf("consumer did not elect a new leader after killing node %d", leader)
	}
	healedAt := time.Now()
	close(release)
	reader := client.NewObserved(workerJS, recorder)
	var slowest time.Duration
	for _, id := range ids {
		value, err := reader.Await(ctx, typ, id)
		if err != nil || string(value) != "1" {
			t.Fatalf("result %s after consumer leader kill=%s err=%v", id, value, err)
		}
		if latency := time.Since(healedAt); latency > slowest {
			slowest = latency
		}
	}
	if slowest >= 30*time.Second {
		t.Errorf("consumer leader recovery slowest=%s from new leader election, want <30s", slowest)
	}
	if got := effects.Load(); got != count {
		t.Fatalf("effect executions=%d want=%d", got, count)
	}
	if err := cluster.RestartNode(leader); err != nil {
		t.Fatalf("restart consumer leader %d: %v", leader, err)
	}
	restartedConn, restartedJS, err := connect(leader)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedConn.Close()
	if _, err := waitRouteCounts(ctx, cluster, leader, 16, 30*time.Second); err != nil {
		t.Fatalf("routes after consumer leader restart: %v", err)
	}
	if err := waitFiveReplicaReadiness(ctx, restartedJS, partition); err != nil {
		t.Fatalf("five-replica readiness after consumer leader restart: %v", err)
	}
	peerReader := client.NewObserved(restartedJS, recorder)
	for _, id := range ids {
		value, err := peerReader.Await(ctx, typ, id)
		if err != nil || string(value) != "1" {
			t.Fatalf("restarted leader result %s=%s err=%v", id, value, err)
		}
	}
	operations := recorder.Snapshot()
	if result, err := history.CheckStarts(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("consumer leader start history=%s: %v", result, err)
	}
	if result, err := history.CheckResults(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("consumer leader result history=%s: %v", result, err)
	}
	report, err := integrity.Check(ctx, restartedJS)
	if err != nil || report.Invocations != count || report.Journals != count || report.Terminal != count || report.Entries != count*4 {
		t.Fatalf("consumer leader retained audit=%+v: %v", report, err)
	}
	t.Logf("killed consumer leader=%d elected=%s effects=%d slowest_post_election=%s retained=%+v", leader, elected, effects.Load(), slowest, report)
}
