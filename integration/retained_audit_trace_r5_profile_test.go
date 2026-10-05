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

// Caller verifies the disposable full-cohort clone before opening it. Each
// complete audit retains the original20s deadline, including tracing overhead.
func TestRetainedAuditTraceR5FullCohort(t *testing.T) {
	storesRoot := os.Getenv("WF_AUDIT_R5_PROFILE_STORES")
	artifact := os.Getenv("WF_AUDIT_TRACE_R5_ROOT")
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
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, os.Getenv("WF_AUDIT_R5_PROFILE_IDENTITY"))
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
	nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond))
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
			expected := uint64(100000)
			if name == "WF_JRN" {
				expected = 1200000
			}
			if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.State.Msgs != expected {
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
	type result struct {
		Label     string
		ElapsedNS int64
		Report    integrity.Report
		Error     string
		Trace     *retainedAuditTraceSnapshot `json:"trace,omitempty"`
	}
	var results []result
	for _, label := range []string{"plain", "traced"} {
		var auditJS jetstream.JetStream = js
		var trace *retainedAuditTrace
		if label == "traced" {
			trace = &retainedAuditTrace{}
			auditJS = tracedAuditJS{JetStream: js, trace: trace}
		}
		call, done := context.WithTimeout(context.Background(), 20*time.Second)
		began := time.Now()
		report, err := integrity.CheckWithStreamingState(call, auditJS)
		elapsed := time.Since(began)
		done()
		r := result{Label: label, ElapsedNS: int64(elapsed), Report: report, Error: fmt.Sprint(err)}
		if trace != nil {
			snapshot := trace.snapshot()
			r.Trace = &snapshot
		}
		results = append(results, r)
		data, marshalErr := json.MarshalIndent(results, "", "  ")
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if err := os.WriteFile(filepath.Join(root, "comparisons.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		want := integrity.Report{Invocations: 100000, Journals: 100000, Entries: 1200000, Terminal: 100000}
		if err != nil || report != want {
			t.Fatalf("%s elapsed=%s report=%+v err=%v", label, elapsed, report, err)
		}
		if trace != nil {
			for _, spec := range []struct {
				name           string
				records, bytes int
			}{{"WF_INV", 100000, 500000}, {"WF_JRN", 1200000, 84700000}} {
				count := r.Trace.Counts[spec.name+".Next"]
				if count.Started != spec.records || count.Completed != spec.records || count.Errors != 0 || count.Bytes != spec.bytes || count.LastSuccess.IsZero() {
					t.Fatalf("%s delivery=%+v", spec.name, count)
				}
				if created := r.Trace.Counts[spec.name+".Messages"]; created.Started != 1 || created.Completed != 1 || created.Errors != 0 {
					t.Fatalf("%s iterator=%+v", spec.name, created)
				}
				if created := r.Trace.Counts[spec.name+".CreateConsumer"]; created.Completed != 1 || created.Errors != 0 {
					t.Fatalf("%s cursor=%+v", spec.name, created)
				}
			}
			if len(r.Trace.Recent) > 64 {
				t.Fatal("unbounded trace history")
			}
		}
		t.Logf("full R5 %s elapsed=%s report=%+v", label, elapsed, report)
	}
}
