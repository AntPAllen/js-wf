//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"errors"
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
	"js-wf/testcluster"
)

// The caller supplies archive-verified disposable copies, never original stores.
// Diagnostic completion records a failed audit as a failure, even if the test
// itself passes because instrumentation and cleanup completed successfully.
func TestConcurrentStateR5CopiedCapacityProfile(t *testing.T) {
	storesRoot := os.Getenv("WF_AUDIT_CAPACITY_PROFILE_STORES")
	if storesRoot == "" {
		t.Skip("opt-in verified 400k copied-store capacity phase/CPU profile")
	}
	if !filepath.IsAbs(storesRoot) || os.Getenv("WF_AUDIT_CAPACITY_PROFILE_IDENTITY") == "" {
		t.Fatal("absolute copied-store path and original identity required")
	}
	root := candidateNativeRoot(t)
	stores := map[int]string{}
	for n := 0; n < 5; n++ {
		stores[n] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", n))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, os.Getenv("WF_AUDIT_CAPACITY_PROFILE_IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	t.Cleanup(func() {
		for n := 0; n < 5; n++ {
			data, e := cluster.Logs(n)
			if e != nil {
				t.Error(e)
				continue
			}
			if e = os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", n)), []byte(data), 0600); e != nil {
				t.Error(e)
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
	for {
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE"} {
			call, cancel := context.WithTimeout(startup, 2*time.Second)
			stream, e := js.Stream(call, name)
			cancel()
			if e != nil || stream.CachedInfo().Cluster == nil || stream.CachedInfo().Cluster.Leader == "" {
				ready = false
				break
			}
			info := stream.CachedInfo()
			expected := uint64(400000)
			if name == "WF_JRN" {
				expected *= 12
			}
			if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage || info.State.Msgs != expected {
				t.Fatalf("incorrect copied population %s: %+v", name, info)
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
	type phase struct {
		Stream  string
		Records int
		Bytes   int64
		ScanNS  int64
		VisitNS int64
		Error   string
	}
	var phases []phase
	metadataReader := "sdk"
	read := scanByteBoundedThrough
	if os.Getenv("WF_AUDIT_CAPACITY_COMPACT_METADATA") == "1" {
		metadataReader = "compact-ack-candidate"
		read = scanCompactByteBoundedThrough
	}
	scanner := func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		p := phase{Stream: stream.CachedInfo().Config.Name}
		began := time.Now()
		var failure error
		pprof.Do(ctx, pprof.Labels("audit_phase", p.Stream), func(ctx context.Context) {
			failure = read(ctx, stream, cutoff, func(msg *jetstream.RawStreamMsg) error {
				p.Records++
				p.Bytes += int64(len(msg.Data))
				entered := time.Now()
				e := visit(msg)
				p.VisitNS += int64(time.Since(entered))
				return e
			})
		})
		p.ScanNS = int64(time.Since(began))
		p.Error = fmt.Sprint(failure)
		phases = append(phases, p)
		return failure
	}
	cpu, err := os.Create(filepath.Join(root, "audit-cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err = pprof.StartCPUProfile(cpu); err != nil {
		cpu.Close()
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	began := time.Now()
	report, failure := checkUsingConcurrentOptions(call, js, nil, scanner, true, true, true)
	elapsed := time.Since(began)
	cancel()
	runtime.ReadMemStats(&after)
	pprof.StopCPUProfile()
	if err = cpu.Close(); err != nil {
		t.Fatal(err)
	}
	allocations, err := os.Create(filepath.Join(root, "audit-allocs.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	if err = pprof.Lookup("allocs").WriteTo(allocations, 0); err != nil {
		allocations.Close()
		t.Fatal(err)
	}
	if err = allocations.Close(); err != nil {
		t.Fatal(err)
	}
	result := struct {
		Phases         []phase
		Report         Report
		ElapsedNS      int64
		Error          string
		AllocatedBytes uint64
		GCCycles       uint32
		HeapAllocBytes uint64
		DiagnosticOnly bool
		MetadataReader string
	}{Phases: phases, Report: report, ElapsedNS: int64(elapsed), Error: fmt.Sprint(failure), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, GCCycles: after.NumGC - before.NumGC, HeapAllocBytes: after.HeapAlloc, DiagnosticOnly: true, MetadataReader: metadataReader}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "capacity-phases.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("400k copied profile elapsed=%s phases=%+v report=%+v err=%v allocated=%d gc=%d", elapsed, phases, report, failure, result.AllocatedBytes, result.GCCycles)
	if failure == nil && report != (Report{Invocations: 400000, Journals: 400000, Entries: 4800000, Terminal: 400000}) {
		t.Fatalf("incomplete success report %+v", report)
	}
	if failure != nil && !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatalf("unexpected audit failure %v", failure)
	}
	if len(phases) == 0 {
		t.Fatal("no scanned phase recorded")
	}
}
