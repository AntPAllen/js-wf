//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
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
	nc, err := nats.Connect(cluster.ClientURL(0), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
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
	w, err := worker.New(ctx, js, "tier3-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.AwaitSignal(c, "go")
		return json.RawMessage(value), err
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
	recorder := &history.Recorder{}
	defer func() {
		path := os.Getenv("WF_TIER3_HISTORY_OUT")
		if path == "" {
			return
		}
		file, err := os.Create(path)
		if err != nil {
			t.Errorf("create Tier 3 client history: %v", err)
			return
		}
		if err := recorder.WriteJSONL(file); err != nil {
			t.Errorf("write Tier 3 client history: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Errorf("close Tier 3 client history: %v", err)
		}
	}()
	c := client.NewObserved(js, recorder)
	clients := []*client.Client{c}
	for i := 1; i < 4; i++ {
		peer, err := nats.Connect(cluster.ClientURL(i), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatalf("connect majority node %d: %v", i, err)
		}
		defer peer.Close()
		peerJS, err := jetstream.New(peer)
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, client.NewObserved(peerJS, recorder))
	}
	const startCalls = 8
	startResults := make(chan error, startCalls)
	var starts sync.WaitGroup
	for i := 0; i < startCalls; i++ {
		starts.Add(1)
		go func(i int) {
			defer starts.Done()
			handle, err := clients[i%len(clients)].Start(ctx, typ, id, []byte(`42`))
			if handle.InvSeq == 0 {
				startResults <- fmt.Errorf("start %d returned zero invocation sequence: %v", i, err)
				return
			}
			startResults <- err
		}(i)
	}
	starts.Wait()
	close(startResults)
	var started, already int
	for err := range startResults {
		switch {
		case err == nil:
			started++
		case errors.Is(err, client.ErrAlreadyStarted):
			already++
		default:
			t.Fatalf("concurrent majority start: %v", err)
		}
	}
	if started != 1 || already != startCalls-1 {
		t.Fatalf("concurrent majority starts: started=%d already=%d", started, already)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("five-container start history=%s: %v", result, err)
	}
	suspendedUntil := time.Now().Add(30 * time.Second)
	suspended := false
	for time.Now().Before(suspendedUntil) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		entries, _, readErr := journal.New(js).Read(attempt, typ, id)
		stop()
		if readErr == nil {
			for _, entry := range entries {
				if entry.Kind == journal.Suspended {
					suspended = true
				}
			}
		}
		if suspended {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !suspended {
		t.Fatal("workflow did not suspend before majority signal")
	}
	sequence, err := clients[1].Signal(ctx, typ, id, "go", []byte(`42`), "tier3-go")
	if err != nil {
		t.Fatalf("majority signal: %v", err)
	}
	duplicate, err := clients[2].Signal(ctx, typ, id, "go", []byte(`42`), "tier3-go")
	if err != nil || duplicate != sequence {
		t.Fatalf("majority signal retry=%d want=%d: %v", duplicate, sequence, err)
	}
	if _, err := clients[3].Signal(ctx, typ, id, "go", []byte(`43`), "tier3-go"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("majority signal mismatch: %v", err)
	}
	if result, err := history.CheckSignals(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("five-container signal history=%s: %v", result, err)
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
				observedCtx, stopObserved := context.WithTimeout(ctx, 3*time.Second)
				observed, observedErr := client.NewObserved(peerJS, recorder).Await(observedCtx, typ, id)
				stopObserved()
				if observedErr != nil || string(observed) != "42" {
					t.Fatalf("observed node four result=%s err=%v", observed, observedErr)
				}
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
	if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("five-container result history=%s: %v", result, err)
	}
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
	entries, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil {
		t.Fatalf("read five-container journal: %v", err)
	}
	var consumed int
	for _, entry := range entries {
		if entry.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	if consumed != 1 {
		t.Fatalf("five-container journal consumed %d signals, want 1", consumed)
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
