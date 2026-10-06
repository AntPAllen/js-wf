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
	"runtime/metrics"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Invoked only by the verified-copy capacity harness. Each fault requires fresh
// copies of the entire closed donor, never a retained failed fixture reopened.
func directR1CopiedCapacityFault(t *testing.T, js jetstream.JetStream, cluster *testcluster.DockerCluster, root, kind string) {
	t.Helper()
	want := Report{Invocations: 400000, Journals: 400000, Entries: 4800000, Terminal: 400000}
	ready, stopReady := context.WithTimeout(context.Background(), time.Minute)
	defer stopReady()
	for {
		allCurrent := true
		for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE"} {
			call, stop := context.WithTimeout(ready, time.Second)
			stream, err := js.Stream(call, name)
			stop()
			if err != nil {
				allCurrent = false
				break
			}
			info := stream.CachedInfo()
			if info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 4 {
				allCurrent = false
				break
			}
			for _, replica := range info.Cluster.Replicas {
				if !replica.Current || replica.Lag != 0 {
					allCurrent = false
				}
			}
		}
		if allCurrent {
			break
		}
		select {
		case <-ready.Done():
			t.Fatal("full R5 retained source replicas did not catch up")
		case <-time.After(50 * time.Millisecond):
		}
	}
	type observation struct {
		Stream   string `json:"stream"`
		Consumer string `json:"consumer"`
		Replicas int    `json:"replicas"`
		Start    uint64 `json:"start"`
	}
	type cleanupObservation struct {
		Stream    string `json:"stream"`
		Consumers int    `json:"consumers"`
		ElapsedNS int64  `json:"elapsed_ns"`
		Error     string `json:"error"`
	}
	type result struct {
		Kind             string                            `json:"kind"`
		Report           Report                            `json:"report"`
		Error            string                            `json:"error"`
		AuditNS          int64                             `json:"audit_ns"`
		TotalNS          int64                             `json:"total_ns"`
		JournalVisits    int                               `json:"journal_visits"`
		Target           *jetstream.ConsumerInfo           `json:"target"`
		Kill             testcluster.DockerKillObservation `json:"kill"`
		RestartCompleted time.Time                         `json:"restart_completed,omitempty"`
		Cursors          []observation                     `json:"cursors"`
		CursorInfo       []*jetstream.ConsumerInfo         `json:"cursor_info"`
		CursorInfoErrors []string                          `json:"cursor_info_errors"`
		Deletions        []processDeleteObservation        `json:"deletions"`
		Cleanup          []cleanupObservation              `json:"cleanup"`
		AllocatedBytes   uint64                            `json:"allocated_bytes"`
		GCCycles         uint32                            `json:"gc_cycles"`
		MemoryLimitBytes uint64                            `json:"memory_limit_bytes"`
		GOMAXPROCS       int                               `json:"gomaxprocs"`
		HeapBefore       uint64                            `json:"heap_before"`
		HeapAfter        uint64                            `json:"heap_after"`
		NextGCBefore     uint64                            `json:"next_gc_before"`
		NextGCAfter      uint64                            `json:"next_gc_after"`
		GCPauseNS        uint64                            `json:"gc_pause_ns"`
		CPUProfile       bool                              `json:"cpu_profile"`
		ChunkedCallback  bool                              `json:"chunked_callback"`
		Phases           []struct {
			Stream     string `json:"stream"`
			StartedNS  int64  `json:"started_ns"`
			FinishedNS int64  `json:"finished_ns"`
		} `json:"phases"`
		StateWatches []capacityWatchTiming `json:"state_watches,omitempty"`
	}
	profileCPU := os.Getenv("WF_AUDIT_CAPACITY_R1_CPU_PROFILE") == "1"
	reader := scanConsumeDirectWindowsThrough
	chunked := os.Getenv("WF_AUDIT_CHUNKED_CALLBACK") == "1"
	if chunked {
		reader = scanConsumeChunkedWindowsThrough
	}
	t.Logf("full-capacity chunked callback candidate=%v", chunked)
	var results []result
	// A current-source healthy baseline precedes the one fault in the same fresh
	// fixture. A baseline failure stops before injecting any destructive fault.
	for _, mode := range []string{"baseline", kind} {
		r := result{Kind: mode, ChunkedCallback: chunked}
		var deletionMu sync.Mutex
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		attempt, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		started := time.Now()
		var cpu *os.File
		currentJS := js
		trace := &capacityStateTrace{origin: &started}
		if profileCPU {
			var err error
			cpu, err = os.Create(filepath.Join(root, mode+"-cpu.pprof"))
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			if err = pprof.StartCPUProfile(cpu); err != nil {
				cpu.Close()
				cancel()
				t.Fatal(err)
			}
			currentJS = capacityStateTraceJS{JetStream: js, trace: trace}
		}
		var failure error
		r.Report, failure = checkUsingConcurrentOptions(attempt, currentJS, nil, func(call context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
			streamName := stream.CachedInfo().Config.Name
			phaseStart := int64(time.Since(started))
			defer func() {
				r.Phases = append(r.Phases, struct {
					Stream     string `json:"stream"`
					StartedNS  int64  `json:"started_ns"`
					FinishedNS int64  `json:"finished_ns"`
				}{Stream: streamName, StartedNS: phaseStart, FinishedNS: int64(time.Since(started))})
			}()
			observed := &candidateObservedStream{Stream: stream, cursorReplicas: 1}
			record := func(info *jetstream.ConsumerInfo, err error) {
				if info != nil {
					r.Cursors = append(r.Cursors, observation{Stream: info.Stream, Consumer: info.Name, Replicas: info.Config.Replicas, Start: info.Config.OptStartSeq})
				}
				if info != nil {
					r.CursorInfo = append(r.CursorInfo, info)
				}
				if err != nil {
					r.CursorInfoErrors = append(r.CursorInfoErrors, err.Error())
				}
			}
			wrapped := processObservedStream{candidateObservedStream: observed, record: record, deleted: func(o processDeleteObservation) {
				deletionMu.Lock()
				defer deletionMu.Unlock()
				r.Deletions = append(r.Deletions, o)
			}}
			read := func(call context.Context) error {
				return reader(call, wrapped, cutoff, func(msg *jetstream.RawStreamMsg) error {
					if streamName != "WF_JRN" {
						return visit(msg)
					}
					r.JournalVisits++
					if msg.Sequence != uint64(r.JournalVisits) {
						return fmt.Errorf("duplicate or omitted capacity journal entry: visit=%d seq=%d", r.JournalVisits, msg.Sequence)
					}
					if mode != "baseline" && r.JournalVisits == 128 {
						target, err := observed.consumer.Info(call)
						if err != nil {
							return err
						}
						r.Target = target
						if target.Name != observed.name || target.Stream != "WF_JRN" || target.Config.Replicas != 1 || !target.Config.MemoryStorage || target.Config.AckPolicy != jetstream.AckNonePolicy || target.Cluster == nil || target.NumPending == 0 {
							return fmt.Errorf("invalid active capacity cursor: %+v", target)
						}
						owner := -1
						for n := 0; n < 5; n++ {
							if cluster.NodeName(n) == target.Cluster.Leader {
								owner = n
							}
						}
						if owner < 0 {
							return fmt.Errorf("capacity cursor owner %q absent", target.Cluster.Leader)
						}
						logs, err := cluster.Logs(owner)
						if err != nil {
							return err
						}
						if err = os.WriteFile(filepath.Join(root, "capacity-owner-before-kill.log"), []byte(logs), 0600); err != nil {
							return err
						}
						r.Kill, err = cluster.KillNodeObserved(owner)
						if err != nil {
							return err
						}
						if r.Kill.SourceStopped.IsZero() {
							return errors.New("capacity cursor owner exit unconfirmed")
						}
						if mode == "owner-restart" {
							if err = cluster.RestartNode(owner); err != nil {
								return err
							}
							r.RestartCompleted = time.Now().UTC()
						}
					}
					return visit(msg)

				})
			}
			if profileCPU {
				var err error
				pprof.Do(call, pprof.Labels("audit_phase", streamName), func(ctx context.Context) { err = read(ctx) })
				return err
			}
			return read(call)
		}, true, true, true)
		r.AuditNS = int64(time.Since(started))
		if failure == nil && (r.Report != want || r.JournalVisits != want.Entries) {
			failure = fmt.Errorf("incomplete capacity report=%+v visits=%d", r.Report, r.JournalVisits)
		}
		if failure == nil && mode != "baseline" && (r.Target == nil || r.Kill.SourceStopped.IsZero()) {
			failure = errors.New("capacity fault never reached")
		}
		if failure == nil {
			for _, name := range []string{"WF_INV", "WF_JRN"} {
				for {
					call, stop := context.WithTimeout(attempt, time.Second)
					stream, err := js.Stream(call, name)
					stop()
					o := cleanupObservation{Stream: name, Consumers: -1, ElapsedNS: int64(time.Since(started))}
					if err == nil {
						o.Consumers = stream.CachedInfo().State.Consumers
					} else {
						o.Error = err.Error()
					}
					r.Cleanup = append(r.Cleanup, o)
					if err == nil && o.Consumers == 0 {
						break
					}
					if attempt.Err() != nil {
						failure = attempt.Err()
						break
					}
					if err != nil && !errors.Is(err, context.DeadlineExceeded) && !batchReadTransportError(err) {
						failure = err
						break
					}
					select {
					case <-attempt.Done():
						failure = attempt.Err()
					case <-time.After(50 * time.Millisecond):
					}
					if failure != nil {
						break
					}
				}
				if failure != nil {
					break
				}
			}
		}
		r.TotalNS = int64(time.Since(started))
		if failure == nil && (attempt.Err() != nil || r.TotalNS >= int64(20*time.Second)) {
			failure = context.DeadlineExceeded
		}
		cancel()
		if profileCPU {
			pprof.StopCPUProfile()
			if err := cpu.Close(); err != nil {
				failure = errors.Join(failure, err)
			}
			r.StateWatches = trace.snapshot()
		}
		runtime.ReadMemStats(&after)
		r.AllocatedBytes = after.TotalAlloc - before.TotalAlloc
		r.GCCycles = after.NumGC - before.NumGC
		r.HeapBefore, r.HeapAfter = before.HeapAlloc, after.HeapAlloc
		r.NextGCBefore, r.NextGCAfter = before.NextGC, after.NextGC
		r.GCPauseNS = after.PauseTotalNs - before.PauseTotalNs
		r.CPUProfile = profileCPU
		samples := []metrics.Sample{{Name: "/gc/gomemlimit:bytes"}}
		metrics.Read(samples)
		if samples[0].Value.Kind() != metrics.KindUint64 {
			t.Fatal("runtime memory limit metric unavailable")
		}
		r.MemoryLimitBytes = samples[0].Value.Uint64()
		r.GOMAXPROCS = runtime.GOMAXPROCS(0)
		if failure != nil {
			r.Error = failure.Error()
		}
		results = append(results, r)
		data, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "direct-r1-capacity-fault.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("full400k R1 mode=%s audit=%s total=%s report=%+v journal_visits=%d err=%v", mode, time.Duration(r.AuditNS), time.Duration(r.TotalNS), r.Report, r.JournalVisits, failure)
		if failure != nil {
			t.Fatalf("full400k %s failed: %v", mode, failure)
		}
	}
}
