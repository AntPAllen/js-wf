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
	if os.Getenv("WF_AUDIT_CAPACITY_PLAIN_COMPARISON") == "1" {
		compareCompactCopiedCapacity(t, js, root)
		return
	}
	type phase struct {
		Stream     string
		StartedNS  int64
		FinishedNS int64
		Records    int
		Bytes      int64
		ScanNS     int64
		VisitNS    int64
		Error      string
	}
	var phases []phase
	var profileBegan time.Time
	var cursorReplicas []int
	var cursorStarts []uint64
	var cursorPointReads int
	stateTrace := &capacityStateTrace{origin: &profileBegan}
	metadataReader := "sdk"
	read := scanByteBoundedThrough
	if os.Getenv("WF_AUDIT_CAPACITY_COMPACT_METADATA") == "1" {
		metadataReader = "compact-ack-candidate"
		read = scanCompactByteBoundedThrough
	}
	if os.Getenv("WF_AUDIT_CAPACITY_SINGLE_REPLICA_PROFILE") == "1" {
		metadataReader = "single-replica-callback-profile"
		read = func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
			observed := &profileCursorStream{Stream: stream, replicas: 1}
			err := scanConsumeByteBoundedThrough(ctx, observed, cutoff, visit)
			cursorReplicas = append(cursorReplicas, observed.creates...)
			cursorStarts = append(cursorStarts, observed.starts...)
			cursorPointReads += observed.pointReads
			return err
		}
		js = capacityStateTraceJS{JetStream: js, trace: stateTrace}
	}
	scanner := func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
		p := phase{Stream: stream.CachedInfo().Config.Name}
		began := time.Now()
		p.StartedNS = int64(began.Sub(profileBegan))
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
		p.FinishedNS = int64(time.Since(profileBegan))
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
	profileBegan = began
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
		Phases           []phase
		Report           Report
		ElapsedNS        int64
		Error            string
		AllocatedBytes   uint64
		GCCycles         uint32
		HeapAllocBytes   uint64
		DiagnosticOnly   bool
		MetadataReader   string
		CursorReplicas   []int
		CursorStarts     []uint64
		CursorPointReads int
		StateWatches     []capacityWatchTiming
	}{Phases: phases, Report: report, ElapsedNS: int64(elapsed), Error: fmt.Sprint(failure), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, GCCycles: after.NumGC - before.NumGC, HeapAllocBytes: after.HeapAlloc, DiagnosticOnly: true, MetadataReader: metadataReader, CursorReplicas: cursorReplicas, CursorStarts: cursorStarts, CursorPointReads: cursorPointReads, StateWatches: stateTrace.snapshot()}
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

// Compare the metadata readers on the same quiet population without CPU/visitor
// profiling. Each full invocation/journal/state reread retains its own20s limit.
func compareCompactCopiedCapacity(t *testing.T, js jetstream.JetStream, root string) {
	type result struct {
		Mode             string
		Report           Report
		Error            string
		ElapsedNS        int64
		AllocatedBytes   uint64
		GCCycles         uint32
		CursorReplicas   []int
		CursorStarts     []uint64
		CursorPointReads int
	}
	var results []result
	want := Report{Invocations: 400000, Journals: 400000, Entries: 4800000, Terminal: 400000}
	type readerMode struct {
		name string
		read retainedScanner
	}
	modes := []readerMode{{"sdk_concurrent", scanByteBoundedThrough}, {"compact_concurrent", scanCompactByteBoundedThrough}}
	candidate := "compact_concurrent"
	if os.Getenv("WF_AUDIT_CAPACITY_CALLBACK_COMPARISON") == "1" {
		candidate = "callback_concurrent"
		modes = append(modes, readerMode{candidate, scanConsumeByteBoundedThrough})
	}
	modes = append(modes, readerMode{"sdk_recheck", scanByteBoundedThrough})
	var cursorReplicas []int
	var cursorStarts []uint64
	var cursorPointReads int
	observeCursors := os.Getenv("WF_AUDIT_CAPACITY_SINGLE_REPLICA_COMPARISON") == "1" || os.Getenv("WF_AUDIT_CAPACITY_DIRECT_CALLBACK_COMPARISON") == "1"
	if observeCursors {
		candidate = "single_replica_callback"
		observedReader := func(replicas int, read retainedScanner) retainedScanner {
			return func(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
				observed := &profileCursorStream{Stream: stream, replicas: replicas}
				err := read(ctx, observed, cutoff, visit)
				cursorReplicas = append(cursorReplicas, observed.creates...)
				cursorStarts = append(cursorStarts, observed.starts...)
				cursorPointReads += observed.pointReads
				return err
			}
		}
		modes = []readerMode{{"replicated_callback", observedReader(5, scanConsumeByteBoundedThrough)}, {candidate, observedReader(1, scanConsumeByteBoundedThrough)}, {"replicated_callback_recheck", observedReader(5, scanConsumeByteBoundedThrough)}}
		if os.Getenv("WF_AUDIT_CAPACITY_DIRECT_CALLBACK_COMPARISON") == "1" {
			candidate = "single_replica_direct_callback"
			modes = []readerMode{{"single_replica_callback", observedReader(1, scanConsumeByteBoundedThrough)}, {candidate, observedReader(1, scanConsumeDirectWindowsThrough)}, {"single_replica_callback_recheck", observedReader(1, scanConsumeByteBoundedThrough)}}
		}
	}
	for _, mode := range modes {
		cursorReplicas, cursorStarts, cursorPointReads = nil, nil, 0
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		call, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		began := time.Now()
		report, err := checkUsingConcurrentOptions(call, js, nil, mode.read, true, true, true)
		elapsed := time.Since(began)
		cancel()
		runtime.ReadMemStats(&after)
		r := result{Mode: mode.name, Report: report, Error: fmt.Sprint(err), ElapsedNS: int64(elapsed), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, GCCycles: after.NumGC - before.NumGC}
		if observeCursors {
			r.CursorReplicas, r.CursorStarts, r.CursorPointReads = cursorReplicas, cursorStarts, cursorPointReads
			if len(cursorReplicas) < 2 {
				t.Errorf("%s missing actual INV/JRN cursor observations: %v", mode.name, cursorReplicas)
			}
		}
		results = append(results, r)
		data, e := json.MarshalIndent(results, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, "compact-capacity-comparison.json"), append(data, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
		t.Logf("plain400k mode=%s elapsed=%s report=%+v err=%v allocated=%d gc=%d", mode.name, elapsed, report, err, results[len(results)-1].AllocatedBytes, results[len(results)-1].GCCycles)
		if err == nil && report != want {
			t.Errorf("%s incomplete successful report %+v", mode.name, report)
		}
		if mode.name == candidate && (err != nil || report != want || elapsed >= 20*time.Second) {
			t.Errorf("%s 400k capacity gate missed: %+v", candidate, results[len(results)-1])
		}
	}
}
