//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestFiveContainerWorkflowSurvivesIsolationAndRestart is the first Tier 3
// container slice. Every server has its own network namespace and file store.
func TestFiveContainerWorkflowSurvivesIsolationAndRestart(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for i := 0; i < 5; i++ {
			if logs, err := cluster.Logs(i); err == nil {
				const tailBytes = 8192
				if len(logs) > tailBytes {
					logs = logs[len(logs)-tailBytes:]
				}
				t.Logf("node %d log tail:\n%s", i, logs)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	nc, err := nats.Connect(cluster.ClientURL(0))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	readyUntil := time.Now().Add(60 * time.Second)
	for time.Now().Before(readyUntil) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.Ensure(attempt, js, 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision five-replica stores: %v", err)
	}
	const typ, id = "tier3", "isolate-restart"
	w, err := worker.New(ctx, js, "tier3-worker", map[string]worker.Handler{typ: func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		return input, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	partition := identity.Partition(typ, id, provision.Partitions)
	go func() { workDone <- w.RunPartition(workCtx, partition) }()
	defer func() {
		stopWork()
		<-workDone
	}()
	if err := waitFiveReplicaReadiness(ctx, js, partition); err != nil {
		t.Fatalf("five-replica readiness before route cut: %v", err)
	}
	if _, err := waitRouteCounts(ctx, cluster, 4, 4, 30*time.Second); err != nil {
		t.Fatalf("node four routes before cut: %v", err)
	}
	baseRoutes, err := waitRouteCounts(ctx, cluster, 0, 4, 30*time.Second)
	if err != nil {
		t.Fatalf("five-node routes before cut: %v", err)
	}
	if err := cluster.DisconnectNode(4); err != nil {
		t.Fatal(err)
	}
	if _, err := waitRouteCounts(ctx, cluster, 4, 0, 2*time.Minute); err != nil {
		t.Fatalf("isolated node kept routes: %v", err)
	}
	majorityRoutes, err := cluster.RouteCount(ctx, 0)
	if err != nil || majorityRoutes < 4 || majorityRoutes > baseRoutes-4 {
		t.Fatalf("majority route count after cut=%d, before=%d: %v", majorityRoutes, baseRoutes, err)
	}
	c := client.New(js)
	if _, err := c.Start(ctx, typ, id, []byte(`42`)); err != nil {
		t.Fatalf("start on four-node majority: %v", err)
	}
	if value, err := c.Await(ctx, typ, id); err != nil || string(value) != "42" {
		t.Fatalf("majority result=%s err=%v", value, err)
	}
	isolated, err := nats.Connect(cluster.ClientURL(4), nats.NoReconnect())
	if err != nil {
		t.Fatalf("isolated node lost its client port: %v", err)
	}
	isolatedJS, err := jetstream.New(isolated)
	if err != nil {
		isolated.Close()
		t.Fatal(err)
	}
	probeUntil := time.Now().Add(2 * time.Second)
	for time.Now().Before(probeUntil) && ctx.Err() == nil {
		probeCtx, stopProbe := context.WithTimeout(ctx, 250*time.Millisecond)
		isolatedValue, isolatedErr := client.New(isolatedJS).Await(probeCtx, typ, id)
		stopProbe()
		if isolatedErr == nil {
			isolated.Close()
			t.Fatalf("isolated node observed post-cut result before heal: %s", isolatedValue)
		}
		time.Sleep(50 * time.Millisecond)
	}
	isolated.Close()
	if err := cluster.ConnectNode(4); err != nil {
		t.Fatal(err)
	}
	readNode := func() {
		t.Helper()
		peer, err := nats.Connect(cluster.ClientURL(4), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		peerJS, err := jetstream.New(peer)
		if err != nil {
			t.Fatal(err)
		}
		until := time.Now().Add(45 * time.Second)
		var value []byte
		for time.Now().Before(until) && ctx.Err() == nil {
			attempt, stop := context.WithTimeout(ctx, 3*time.Second)
			value, err = client.New(peerJS).Await(attempt, typ, id)
			stop()
			if err == nil && string(value) == "42" {
				return
			}
			time.Sleep(150 * time.Millisecond)
		}
		t.Fatalf("node four did not serve immutable result: %s, %v", value, err)
	}
	readNode()
	if err := cluster.KillNode(4); err != nil {
		t.Fatal(err)
	}
	if err := cluster.RestartNode(4); err != nil {
		t.Fatal(err)
	}
	readNode()
	until := time.Now().Add(30 * time.Second)
	var report integrity.Report
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		report, err = integrity.Check(attempt, js)
		stop()
		if err == nil && report.Invocations == 1 && report.Journals == 1 && report.Terminal == 1 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if err != nil || report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 {
		t.Fatalf("five-container retained audit=%+v: %v", report, err)
	}
	t.Logf("five-container result survived one network isolation and one file-store restart: %+v", report)
}

func waitRouteCounts(ctx context.Context, cluster *testcluster.DockerCluster, node, minimum int, wait time.Duration) (int, error) {
	until := time.Now().Add(wait)
	var count int
	var err error
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		count, err = cluster.RouteCount(attempt, node)
		stop()
		if err == nil && ((minimum == 0 && count == 0) || (minimum > 0 && count >= minimum)) {
			return count, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
	return count, fmt.Errorf("node %d routes=%d, wanted %d: %v (context: %v)", node, count, minimum, err, ctx.Err())
}

func waitFiveReplicaReadiness(ctx context.Context, js jetstream.JetStream, partition uint32) error {
	streams := []string{"WF_INV", "WF_RUN", "WF_JRN", "WF_SIG", "WF_PURGE", "KV_WF_LEASE", "KV_WF_STATE", "KV_WF_VIEW", "KV_WF_ASSIGN", "OBJ_WF_BLOB"}
	until := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(until) && ctx.Err() == nil {
		ready := true
		for _, name := range streams {
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			stream, err := js.Stream(attempt, name)
			var info *jetstream.StreamInfo
			if err == nil {
				info, err = stream.Info(attempt)
			}
			stop()
			if err != nil || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
				ready = false
				last = fmt.Sprintf("%s: info=%+v err=%v", name, info, err)
				break
			}
			for _, replica := range info.Cluster.Replicas {
				if replica == nil || !replica.Current || replica.Offline {
					ready = false
					last = fmt.Sprintf("%s: replica=%+v", name, replica)
					break
				}
			}
			if !ready {
				break
			}
			if name == "WF_RUN" {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				consumer, consumerErr := stream.Consumer(attempt, fmt.Sprintf("WF_P_%02d", partition))
				var consumerInfo *jetstream.ConsumerInfo
				if consumerErr == nil {
					consumerInfo, consumerErr = consumer.Info(attempt)
				}
				stop()
				if consumerErr != nil || consumerInfo.Cluster == nil || consumerInfo.Cluster.Leader == "" || len(consumerInfo.Cluster.Replicas) != 4 {
					ready = false
					last = fmt.Sprintf("run consumer: info=%+v err=%v", consumerInfo, consumerErr)
					break
				}
				for _, replica := range consumerInfo.Cluster.Replicas {
					if replica == nil || !replica.Current || replica.Offline {
						ready = false
						last = fmt.Sprintf("run consumer: replica=%+v", replica)
						break
					}
				}
			}
		}
		if ready {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for five current replicas: %s (context: %v)", last, ctx.Err())
}
