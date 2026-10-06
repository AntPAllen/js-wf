//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/integrity"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A real completed cohort from a failed soak, on fresh verified disposable
// copies only. It does not turn that failed campaign into a passing soak.
func TestMatrixParallelInvocationAuditsRetainedCohort(t *testing.T) {
	root := os.Getenv("WF_MATRIX_LATENCY_COHORT_ROOT")
	if root == "" {
		t.Skip("opt-in verified copied 87920 real-workflow cohort")
	}
	storesRoot := os.Getenv("WF_MATRIX_LATENCY_COHORT_STORES")
	if !filepath.IsAbs(root) || !filepath.IsAbs(storesRoot) {
		t.Fatal("fixture and copied stores must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(os.Getenv("WF_MATRIX_LATENCY_COHORT_FAULTS"))
	if err != nil {
		t.Fatal(err)
	}
	var faults []struct {
		Healed time.Time `json:"healed"`
	}
	if err := json.Unmarshal(data, &faults); err != nil || len(faults) == 0 || faults[len(faults)-1].Healed.IsZero() {
		t.Fatalf("original fault deadline unavailable: %v", err)
	}
	completionDeadline := faults[len(faults)-1].Healed.Add(5 * time.Minute)
	stores := map[int]string{}
	for i := 0; i < 5; i++ {
		stores[i] = filepath.Join(storesRoot, fmt.Sprintf("node-%d", i))
	}
	cluster, err := testcluster.StartDockerClusterWithRestoredIdentity(filepath.Join(root, "cluster"), 5, stores, os.Getenv("WF_MATRIX_LATENCY_COHORT_IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		for i := 0; i < 5; i++ {
			logs, err := cluster.Logs(i)
			if err == nil {
				err = os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", i)), []byte(logs), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}
	}()
	var urls []string
	for i := 0; i < 5; i++ {
		urls = append(urls, cluster.ClientURL(i))
	}
	nc, err := nats.Connect(strings.Join(urls, ","), nats.IgnoreDiscoveredServers(), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	faultMode := os.Getenv("WF_MATRIX_BULK_LATENCY_CURSOR_FAULT")
	if faultMode != "" && (faultMode != "owner-restart" || os.Getenv("WF_MATRIX_BULK_LATENCY_COHORT") != "1") {
		t.Fatal("cursor fault requires bulk cohort owner-restart")
	}
	ready, stopReady := context.WithTimeout(context.Background(), time.Minute)
	err = waitFiveReplicaReadiness(ready, js, 0)
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	if faultMode != "" {
		prepare, stop := context.WithTimeout(context.Background(), 20*time.Second)
		err = matrixPrepareCopiedBulkCursors(prepare, js, root)
		stop()
		if err != nil {
			t.Fatal(err)
		}
	}
	meta, stopMeta := context.WithTimeout(context.Background(), 5*time.Second)
	inv, err := js.Stream(meta, "WF_INV")
	stopMeta()
	if err != nil {
		t.Fatal(err)
	}
	info := inv.CachedInfo()
	const cutoff = uint64(87920)
	if info == nil || info.Config.Replicas != 5 || info.State.FirstSeq != 1 || info.State.LastSeq < cutoff {
		t.Fatalf("unexpected original invocation boundary: %+v", info)
	}
	pointJS := js
	var metadata *matrixLatencyMetadataJS
	if os.Getenv("WF_MATRIX_CACHED_LATENCY_METADATA") == "1" {
		metadata = &matrixLatencyMetadataJS{JetStream: js}
		pointJS = metadata
	}
	bulkMode := os.Getenv("WF_MATRIX_BULK_LATENCY_COHORT") == "1"
	var baseline struct {
		Cutoff             uint64                  `json:"cutoff"`
		PointError         string                  `json:"point_error"`
		ReportError        string                  `json:"report_error"`
		CompletionDeadline time.Time               `json:"original_completion_deadline"`
		Samples            [][]matrixLatencySample `json:"samples"`
	}
	var fullReport integrity.Report
	if bulkMode {
		oraclePath := os.Getenv("WF_MATRIX_LATENCY_COHORT_ORACLE")
		if !filepath.IsAbs(oraclePath) {
			t.Fatal("bulk cohort requires verified original point oracle")
		}
		data, err := os.ReadFile(oraclePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &baseline); err != nil {
			t.Fatal(err)
		}
		if baseline.Cutoff != cutoff || baseline.PointError != "<nil>" || baseline.ReportError != "<nil>" || len(baseline.Samples) != int(cutoff) || !baseline.CompletionDeadline.Equal(completionDeadline) {
			t.Fatal("bulk cohort point oracle boundary/deadline/verdict differs")
		}
		check, stop := context.WithTimeout(context.Background(), 20*time.Second)
		fullReport, err = integrity.CheckThroughInvocationSequenceWithChunkedConcurrentStateReads(check, js, cutoff)
		stop()
		if err != nil || fullReport != (integrity.Report{Invocations: 87920, Journals: 87920, Entries: 969925, Terminal: 87920}) {
			t.Fatalf("bulk cohort requires full original integrity: %+v %v", fullReport, err)
		}
	}
	var completed atomic.Uint64
	started := time.Now()
	stage, stopStage := context.WithTimeout(context.Background(), 6*time.Minute)
	var results []matrixInvocationAuditResult
	var failure error
	var bulkStats matrixBulkLatencyStats
	var faultProof *matrixBulkCursorFaultProof
	if bulkMode {
		bulkJS := js
		var fault *matrixBulkCursorFault
		if faultMode != "" {
			faultContext, cancel := context.WithCancel(stage)
			defer cancel()
			fault = &matrixBulkCursorFault{JetStream: js, cluster: cluster, root: root, ctx: faultContext, cancel: cancel}
			bulkJS = fault
			stage = faultContext
		}
		results, bulkStats, failure = matrixBulkInvocationAuditsThrough(stage, bulkJS, fullReport, completionDeadline, cutoff)
		if fault != nil {
			p := fault.snapshot()
			faultProof = &p
			resumed := false
			for _, cursor := range p.Cursors {
				// The callback trigger is delivered metadata, not the visitor's
				// commit point. Buffered records can put the exact resume on
				// either side of it; the scanner verifies contiguous visits.
				if p.Target != nil && cursor.Name != p.Target.Name && cursor.Config.OptStartSeq > p.Target.Config.OptStartSeq {
					resumed = true
				}
			}
			if failure == nil && (p.Error != "" || p.Kill.SourceStopped.IsZero() || p.RestartCompleted.IsZero() || !resumed) {
				failure = fmt.Errorf("bulk cursor fault/recovery unproven: %+v", p)
			}
		}
		if failure == nil {
			if len(results) != len(baseline.Samples) {
				failure = fmt.Errorf("bulk sample census differs from accepted point oracle")
			}
			for index, result := range results {
				if failure != nil {
					break
				}
				if !reflect.DeepEqual(result.samples, baseline.Samples[index]) {
					failure = fmt.Errorf("bulk samples differ from accepted point oracle at invocation %d", index+1)
				}
			}
		}
		if failure != nil {
			results = nil
		}
	} else {
		results, failure = matrixParallelInvocationAudits(stage, 1, cutoff, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
			var result matrixInvocationAuditResult
			msg, err := inv.GetMsg(ctx, sequence)
			if err != nil {
				return result, err
			}
			parts := strings.Split(msg.Subject, ".")
			if len(parts) != 4 {
				return result, fmt.Errorf("invalid invocation subject %q", msg.Subject)
			}
			result.samples, err = matrixInvocationLatencies(ctx, pointJS, parts[2], parts[3], msg.Time, completionDeadline)
			if err == nil {
				if n := completed.Add(1); n%10000 == 0 {
					t.Logf("LATENCY_COHORT_PROGRESS completed=%d/%d elapsed=%s", n, cutoff, time.Since(started))
				}
			}
			return result, err
		})
	}
	elapsed := time.Since(started)
	stopStage()
	var report integrity.Report
	var reportError error
	if failure == nil {
		check, stop := context.WithTimeout(context.Background(), 20*time.Second)
		report, reportError = integrity.CheckThroughInvocationSequenceWithChunkedConcurrentStateReads(check, js, cutoff)
		stop()
	}
	expectedReport := integrity.Report{Invocations: 87920, Journals: 87920, Entries: 969925, Terminal: 87920}
	if failure != nil || reportError != nil || report != expectedReport {
		results = nil
	}
	persisted := make([][]matrixLatencySample, len(results))
	terminals := 0
	for i, result := range results {
		persisted[i] = result.samples
		for _, sample := range result.samples {
			if sample.Event == "terminal" {
				terminals++
			}
		}
	}
	proof := map[string]any{"cutoff": cutoff, "completed_point_checks": completed.Load(), "elapsed_ns": elapsed.Nanoseconds(), "stage_limit_ns": int64(6 * time.Minute), "point_error": fmt.Sprint(failure), "original_completion_deadline": completionDeadline, "terminal_samples": terminals, "report": report, "report_error": fmt.Sprint(reportError), "samples": persisted, "scope": "quiet copied87920 real-workflow point checks; no original24h/full400k/currentmatrix qualification; partial errors discard whole samples"}
	if bulkMode {
		proof["audit_mode"] = "bulk"
		proof["bulk_stats"] = bulkStats
		proof["bulk_equals_accepted_point_oracle"] = failure == nil && reportError == nil && report == expectedReport
		proof["scope"] = "quiet copied87920 bulk samples compared with committed verified original cached point oracle; no original24h/full400k/currentmatrix qualification; any errors discard all samples"
		if faultProof != nil {
			proof["cursor_fault"] = faultProof
		}
	}
	if metadata != nil {
		proof["metadata_handle_lookups"] = metadata.lookupCounts()
	}
	data, err = json.MarshalIndent(proof, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(root, "cohort.json"), data, 0600)
	}
	if err != nil {
		t.Fatal(err)
	}
	if failure != nil || reportError != nil || terminals != int(cutoff) || report != expectedReport {
		t.Fatalf("point checks=%d terminals=%d elapsed=%s err=%v report=%+v report_err=%v", completed.Load(), terminals, elapsed, failure, report, reportError)
	}
	t.Logf("LATENCY_COHORT_RESULT invocations=87920 terminals=%d elapsed=%s", terminals, elapsed)
}
