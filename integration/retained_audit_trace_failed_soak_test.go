//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
	"js-wf/testcluster"
)

// Caller supplies verified disposable copies of checkpoint1040 failed-soak stores.
// One complete quiescent-cohort audit uses its original20s attempt deadline.
// It does not reproduce the concurrent original fault/workload conditions.
func TestRetainedAuditTraceFailedSoakCopiedCohort(t *testing.T) {
	storesRoot := os.Getenv("WF_AUDIT_FAILED_SOAK_STORES")
	artifact := os.Getenv("WF_AUDIT_FAILED_SOAK_ROOT")
	if storesRoot == "" || artifact == "" {
		t.Skip("opt-in verified copied-store R5 trace qualification")
	}
	if !filepath.IsAbs(storesRoot) || !filepath.IsAbs(artifact) {
		t.Fatal("absolute artifact and copied-store paths required")
	}
	root := filepath.Join(artifact, t.Name())
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	stores := map[int]string{}
	for node := 0; node < 5; node++ {
		stores[node] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", node))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, os.Getenv("WF_AUDIT_FAILED_SOAK_IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", node)), []byte(logs), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	var urls []string
	for node := 0; node < 5; node++ {
		urls = append(urls, cluster.ClientURL(node))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	startup, stop := context.WithTimeout(context.Background(), 4*time.Minute)
	defer stop()
	for {
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN"} {
			call, done := context.WithTimeout(startup, 2*time.Second)
			stream, err := js.Stream(call, name)
			done()
			if err != nil || stream.CachedInfo().Cluster == nil || stream.CachedInfo().Cluster.Leader == "" {
				ready = false
				break
			}
			info := stream.CachedInfo()
			expected := uint64(29120)
			if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || (name == "WF_INV" && info.State.Msgs < expected) || (name == "WF_JRN" && info.State.Msgs == 0) {
				t.Fatalf("%s: incorrect full R5 cohort %+v", name, info)
			}
		}
		if ready {
			break
		}
		if startup.Err() != nil {
			t.Fatal(startup.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	trace := &retainedAuditTrace{}
	call, done := context.WithTimeout(context.Background(), 20*time.Second)
	began := time.Now()
	report, failure := integrity.CheckThroughInvocationSequenceWithStreamingStateReads(call, tracedAuditJS{JetStream: js, trace: trace}, 29120)
	elapsed := time.Since(began)
	done()
	result := struct {
		Cutoff    uint64                     `json:"cutoff"`
		ElapsedNS int64                      `json:"elapsed_ns"`
		Report    integrity.Report           `json:"report"`
		Error     string                     `json:"error"`
		Trace     retainedAuditTraceSnapshot `json:"trace"`
	}{29120, int64(elapsed), report, fmt.Sprint(failure), trace.snapshot()}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "copied-cohort-audit.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("copied failed-soak cohort elapsed=%s report=%+v err=%v", elapsed, report, failure)
	if failure != nil || report.Invocations != 29120 || report.Journals != 29120 || report.Terminal != 29120 || report.Entries == 0 || elapsed >= 20*time.Second {
		t.Fatalf("original-budget copied cohort audit: %+v", result)
	}
	if c := result.Trace.Counts["WF_STATE.WatchAll"]; c.Started != 1 || c.Completed != 1 || c.Errors != 0 {
		t.Fatalf("watch creation: %+v", c)
	}
	if c := result.Trace.Counts["WF_STATE.WatchStop"]; c.Started != 1 || c.Completed != 1 || c.Errors != 0 {
		t.Fatalf("watch cleanup: %+v", c)
	}
}
