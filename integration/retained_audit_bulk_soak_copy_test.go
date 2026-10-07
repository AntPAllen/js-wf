//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
	"js-wf/testcluster"
)

// This diagnostic preserves the entire failed checkpoint8520 cohort and its
// original attempt budget. Only fresh, fully verified restored stores are used.
// Quiescent recovery cannot qualify the original concurrent 24-hour run.
func TestRetainedAuditBulkSoakCheckpoint8520VerifiedCopy(t *testing.T) {
	const cutoff = 238560
	storesRoot, artifact := os.Getenv("WF_AUDIT_BULK_SOAK_STORES"), os.Getenv("WF_AUDIT_BULK_SOAK_ROOT")
	identity := os.Getenv("WF_AUDIT_BULK_SOAK_IDENTITY")
	if storesRoot == "" || artifact == "" {
		t.Skip("opt-in verified original checkpoint8520 copied-store audit")
	}
	if !filepath.IsAbs(storesRoot) || !filepath.IsAbs(artifact) || identity == "" {
		t.Fatal("absolute fresh copied stores, artifact root and original Raft identity required")
	}
	root := filepath.Join(artifact, t.Name())
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	stores := map[int]string{}
	for n := 0; n < 5; n++ {
		stores[n] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", n))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for n := 0; n < 5; n++ {
			logs, err := cluster.Logs(n)
			if err != nil {
				t.Error(err)
				continue
			}
			if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", n)), []byte(logs), 0600); err != nil {
				t.Error(err)
			}
		}
	})
	var urls []string
	for n := 0; n < 5; n++ {
		urls = append(urls, cluster.ClientURL(n))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.Timeout(time.Second))
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
	readiness := map[string]*jetstream.StreamInfo{}
	for {
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE", "WF_PURGE"} {
			call, cancel := context.WithTimeout(startup, 2*time.Second)
			stream, e := js.Stream(call, name)
			cancel()
			if e != nil {
				ready = false
				break
			}
			info := stream.CachedInfo()
			if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage {
				t.Fatalf("%s: original full R5 file source required: %+v", name, info)
			}
			if (name == "WF_INV" && info.State.Msgs < cutoff) || (name == "WF_JRN" && info.State.Msgs == 0) {
				t.Fatalf("%s: incomplete full original cohort: %+v", name, info)
			}
			if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
				ready = false
				break
			}
			for _, peer := range info.Cluster.Replicas {
				if !peer.Current || peer.Offline {
					ready = false
				}
			}
			if !ready {
				break
			}
			readiness[name] = info
		}
		if ready {
			break
		}
		if startup.Err() != nil {
			t.Fatal("all four original R5 sources did not heal", startup.Err())
		}
		time.Sleep(50 * time.Millisecond)
	}
	var cpuProfile *os.File
	var memoryBefore, memoryAfter runtime.MemStats
	profiling := os.Getenv("WF_AUDIT_BULK_SOAK_CPU_PROFILE") == "1"
	if profiling {
		var err error
		cpuProfile, err = os.Create(filepath.Join(root, "audit-cpu.pprof"))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.StartCPUProfile(cpuProfile); err != nil {
			cpuProfile.Close()
			t.Fatal(err)
		}
		runtime.ReadMemStats(&memoryBefore)
	}
	trace := &retainedAuditTrace{}
	parallelDecode := os.Getenv("WF_AUDIT_BULK_SOAK_PARALLEL_DECODE") == "1"
	call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	began := time.Now()
	check := integrity.CheckThroughInvocationSequenceWithChunkedConcurrentStateReads
	if parallelDecode {
		check = integrity.CheckThroughInvocationSequenceWithParallelDecodedChunkedStateReads
	}
	report, failure := check(call, tracedAuditJS{JetStream: js, trace: trace}, cutoff)
	elapsed := time.Since(began)
	cancel()
	if profiling {
		pprof.StopCPUProfile()
		if err := cpuProfile.Close(); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&memoryAfter)
		observations := struct {
			Before runtime.MemStats `json:"before"`
			After  runtime.MemStats `json:"after"`
			Scope  string           `json:"scope"`
		}{memoryBefore, memoryAfter, "Snapshots around the unchanged full checker; not lifetime/peak heap or server memory. CPU profiler setup/drain and these snapshots are outside the original checker elapsed/deadline; instrumentation may perturb execution."}
		data, err := json.MarshalIndent(observations, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "audit-memory.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}

	result := struct {
		Cutoff         uint64                           `json:"cutoff"`
		ElapsedNS      int64                            `json:"elapsed_ns"`
		Report         integrity.Report                 `json:"report"`
		Error          string                           `json:"error"`
		Readiness      map[string]*jetstream.StreamInfo `json:"readiness"`
		Trace          retainedAuditTraceSnapshot       `json:"trace"`
		Qualifies24h   bool                             `json:"qualifies_24h"`
		ParallelDecode bool                             `json:"parallel_decode"`
	}{cutoff, int64(elapsed), report, fmt.Sprint(failure), readiness, trace.snapshot(), false, parallelDecode}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "copied-checkpoint-audit.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("full original checkpoint8520 copied audit cutoff=%d elapsed=%s report=%+v err=%v", cutoff, elapsed, report, failure)
	if failure != nil || report.Invocations != cutoff || report.Journals != cutoff || report.Terminal != cutoff || report.Entries == 0 || elapsed >= 20*time.Second {
		t.Fatalf("original-budget complete copied cohort audit failed: %+v", result.Report)
	}
	for _, name := range []string{"WF_STATE.WatchAll", "WF_STATE.WatchStop"} {
		c := result.Trace.Counts[name]
		if c.Started != 1 || c.Completed != 1 || c.Errors != 0 {
			t.Fatalf("original state snapshot lifecycle %s: %+v", name, c)
		}
	}
}
