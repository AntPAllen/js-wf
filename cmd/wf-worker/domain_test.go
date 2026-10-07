package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func TestWorkerRunnerCompletesWorkflowAndServesMetricsInJetStreamDomain(t *testing.T) {
	pluginPath := testWorkerPlugin(t)
	for _, mode := range []string{"static", "kv", "auto"} {
		t.Run(mode, func(t *testing.T) { runWorkerSmokeWithDomain(t, pluginPath, mode, "WFWORKER") })
	}
}

func workerDomainCluster(t *testing.T, domain string) (*testcluster.Cluster, error) {
	return workerDomainClusterWithLeaf(t, domain, false)
}

func workerDomainClusterWithLeaf(t *testing.T, domain string, leaf bool) (*testcluster.Cluster, error) {
	t.Helper()
	root := os.Getenv("WF_WORKER_TEST_ROOT")
	if root == "" {
		root = t.TempDir()
	} else {
		var err error
		root, err = os.MkdirTemp(root, strings.ReplaceAll(t.Name(), "/", "_")+"-")
		if err != nil {
			t.Fatal(err)
		}
	}
	if domain == "" {
		return testcluster.Start(root, 1)
	}
	if leaf {
		return testcluster.StartWithLeafDomain(root, 3, domain)
	}
	return testcluster.StartWithDomain(root, 3, domain)
}

func workerDomainJS(cluster *testcluster.Cluster, domain string) (jetstream.JetStream, error) {
	if domain == "" {
		return jetstream.New(cluster.Clients[0])
	}
	return jetstream.NewWithDomain(cluster.Clients[0], domain)
}

func workerDomainArgs(domain string, replicas int) []string {
	args := []string{"-replicas", strconv.Itoa(replicas)}
	if domain != "" {
		args = append(args, "-domain", domain)
	}
	return args
}

func runDomainWorker(t *testing.T, ctx context.Context, domain string, args []string) error {
	if domain == "" {
		return run(ctx, args)
	}
	var observed, wrong atomic.Int64
	trace := &jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
		if strings.HasPrefix(subject, "$JS."+domain+".API.") {
			observed.Add(1)
		} else {
			wrong.Add(1)
		}
	}}
	err := runWithJetStreamOptions(ctx, args, jetstream.WithClientTrace(trace))
	if observed.Load() == 0 || wrong.Load() != 0 {
		t.Errorf("worker domain API routing: observed=%d wrong=%d", observed.Load(), wrong.Load())
	}
	t.Logf("worker real domain=%s domain_api_requests=%d wrong_prefix_requests=%d", domain, observed.Load(), wrong.Load())
	return err
}

func admitWorkerDomain(t *testing.T, ctx context.Context, cluster *testcluster.Cluster, domain string) {
	t.Helper()
	ready, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ids := map[string]bool{}
	for node, nc := range cluster.Clients {
		js, err := jetstream.NewWithDomain(nc, domain)
		if err != nil {
			t.Fatal(err)
		}
		for {
			attempt, stop := context.WithTimeout(ready, 4*time.Second)
			info, err := js.AccountInfo(attempt)
			stop()
			if err == nil {
				id := nc.ConnectedServerId()
				if info.Domain != domain || nc.ConnectedDomain() != domain || id == "" || ids[id] {
					t.Fatalf("worker domain admission node=%d info=%+v connected=%s id=%s", node, info, nc.ConnectedDomain(), id)
				}
				ids[id] = true
				t.Logf("worker domain admitted node=%d domain=%s server_id=%s", node, domain, id)
				break
			}
			if ready.Err() != nil {
				t.Fatalf("worker domain admission node=%d: %v", node, err)
			}
			select {
			case <-ready.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}
