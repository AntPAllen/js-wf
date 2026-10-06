//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/integrity"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

// Retain the original real-cluster fixture on success or failure. This compares
// identical point checks on completed workflows; it is not a fault/scale gate.
func TestMatrixParallelInvocationAuditsNativeOracle(t *testing.T) {
	root := os.Getenv("WF_MATRIX_PARALLEL_LATENCY_ROOT")
	if root == "" {
		t.Skip("set WF_MATRIX_PARALLEL_LATENCY_ROOT to a fresh retained fixture")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("native fixture root must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.Start(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := setupCluster(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	short := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", 0, func(context.Context) (int, error) { return 7, nil })
		return json.RawMessage(fmt.Sprint(value)), err
	}
	handlers := map[string]worker.Handler{
		"latencyshort": short,
		"latencychild": short,
		"latencytimer": func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
			if err := wf.Sleep(c, "timer", 50*time.Millisecond); err != nil {
				return nil, err
			}
			return json.RawMessage("7"), nil
		},
		"latencysignal": func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
			return wf.AwaitSignal(c, "go")
		},
		"latencyparent": func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
			return wf.Call(c, "latencychild", raw)
		},
	}
	w, err := worker.New(ctx, all[0], "latency-native", handlers, worker.WithPartitionConcurrency(8))
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunAssigned(workerCtx, 0, 1) }()
	joined := false
	defer func() {
		stopWorker()
		if !joined {
			<-workerDone
		}
	}()
	c := client.New(all[1])
	types := []string{"latencyshort", "latencytimer", "latencysignal", "latencyparent"}
	for _, typ := range types {
		for i := 0; i < 32; i++ {
			id := fmt.Sprintf("case-%d", i)
			if _, err := c.Start(ctx, typ, id, []byte("null")); err != nil {
				t.Fatal(err)
			}
			if typ == "latencysignal" {
				if _, err := c.Signal(ctx, typ, id, "go", []byte("7"), "signal"); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, typ := range types {
		for i := 0; i < 32; i++ {
			value, err := c.Await(ctx, typ, fmt.Sprintf("case-%d", i))
			if err != nil || string(value) != "7" {
				t.Fatalf("%s/%d: value=%s err=%v", typ, i, value, err)
			}
		}
	}
	stopWorker()
	err = <-workerDone
	joined = true
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	inv, err := all[2].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil || info.State.Msgs != 160 {
		t.Fatalf("invocations=%+v err=%v", info, err)
	}
	deadline := time.Now().Add(time.Minute)
	read := func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		var result matrixInvocationAuditResult
		msg, err := inv.GetMsg(ctx, sequence)
		if err != nil {
			return result, err
		}
		parts := strings.Split(msg.Subject, ".")
		result.samples, err = matrixInvocationLatencies(ctx, all[2], parts[2], parts[3], msg.Time, deadline)
		return result, err
	}
	started := time.Now()
	parallel, err := matrixParallelInvocationAudits(ctx, info.State.FirstSeq, info.State.LastSeq, read)
	parallelElapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	var serial []matrixInvocationAuditResult
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		result, err := read(attempt, sequence)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		serial = append(serial, result)
	}
	serialElapsed := time.Since(started)
	if !reflect.DeepEqual(parallel, serial) {
		t.Fatal("parallel samples differ from serial point oracle")
	}
	// Evaluate the same retained bytes with the frozen pre-extraction point
	// implementation. This oracle has its own latency reduction logic.
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		msg, err := inv.GetMsg(ctx, sequence)
		if err != nil {
			t.Fatal(err)
		}
		parts := strings.Split(msg.Subject, ".")
		legacy, err := matrixInvocationLatenciesLegacyOracle(ctx, all[2], parts[2], parts[3], msg.Time, deadline, 0)
		if err != nil || !reflect.DeepEqual(legacy, serial[sequence-info.State.FirstSeq].samples) {
			t.Fatalf("shared reducer differs from frozen point oracle for %s: %v", msg.Subject, err)
		}
	}
	metadata := &matrixLatencyMetadataJS{JetStream: all[2]}
	cached, err := matrixParallelInvocationAudits(ctx, info.State.FirstSeq, info.State.LastSeq, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		var result matrixInvocationAuditResult
		msg, err := inv.GetMsg(ctx, sequence)
		if err != nil {
			return result, err
		}
		parts := strings.Split(msg.Subject, ".")
		result.samples, err = matrixInvocationLatencies(ctx, metadata, parts[2], parts[3], msg.Time, deadline)
		return result, err
	})
	if err != nil || !reflect.DeepEqual(cached, serial) {
		t.Fatalf("metadata reuse changes point oracle: %v", err)
	}
	counts := metadata.lookupCounts()
	if counts["journal"] != 1 || counts["signals"] != 1 || counts["state"] != 1 || counts["objects"] != 0 {
		t.Fatalf("unexpected native handle lookups: %v", counts)
	}
	auditCtx, stopAudit := context.WithTimeout(ctx, 20*time.Second)
	fullReport, err := integrity.CheckWithChunkedConcurrentStateReads(auditCtx, all[2])
	stopAudit()
	if err != nil || fullReport.Invocations != 160 || fullReport.Terminal != 160 {
		t.Fatalf("bulk oracle requires complete integrity: %+v %v", fullReport, err)
	}
	bulk, bulkStats, err := matrixBulkInvocationAudits(ctx, all[2], fullReport, deadline)
	if err != nil || !reflect.DeepEqual(bulk, serial) || bulkStats.SnapshotFallbacks != 0 {
		t.Fatalf("bulk timestamp samples differ from frozen point oracle: stats=%+v err=%v", bulkStats, err)
	}
	// The same actual terminal records must still fail a violated deadline.
	deadline = time.Unix(0, 0)
	_, serialErr := read(ctx, info.State.FirstSeq)
	failed, parallelErr := matrixParallelInvocationAudits(ctx, info.State.FirstSeq, info.State.FirstSeq, read)
	if serialErr == nil || parallelErr == nil || failed != nil || !strings.Contains(serialErr.Error(), "post-heal deadline") || !strings.Contains(parallelErr.Error(), serialErr.Error()) {
		t.Fatalf("deadline invariant weakened: serial=%v parallel=%v result=%v", serialErr, parallelErr, failed)
	}
	cachedFailure, cachedErr := matrixParallelInvocationAudits(ctx, info.State.FirstSeq, info.State.FirstSeq, func(ctx context.Context, sequence uint64) (matrixInvocationAuditResult, error) {
		var result matrixInvocationAuditResult
		msg, err := inv.GetMsg(ctx, sequence)
		if err != nil {
			return result, err
		}
		parts := strings.Split(msg.Subject, ".")
		result.samples, err = matrixInvocationLatencies(ctx, metadata, parts[2], parts[3], msg.Time, deadline)
		return result, err
	})
	if cachedFailure != nil || cachedErr == nil || !strings.Contains(cachedErr.Error(), serialErr.Error()) {
		t.Fatalf("metadata reuse weakened terminal deadline: %v", cachedErr)
	}
	bulkFailure, _, bulkErr := matrixBulkInvocationAudits(ctx, all[2], fullReport, deadline)
	if bulkFailure != nil || bulkErr == nil || !strings.Contains(bulkErr.Error(), "post-heal deadline") {
		t.Fatalf("bulk audit weakened terminal deadline: %v", bulkErr)
	}
	persistedSamples := make([][]matrixLatencySample, len(parallel))
	for index, result := range parallel {
		persistedSamples[index] = result.samples
	}
	data, err := json.MarshalIndent(map[string]any{"invocations": len(serial), "serial_equals_parallel": true, "legacy_equals_shared_reducer": true, "bulk_equals_serial": true, "bulk_stats": bulkStats, "cached_metadata_equals_serial": true, "metadata_handle_lookups": counts, "deadline_negative_control": true, "parallel_elapsed": parallelElapsed, "serial_elapsed": serialElapsed, "measurement_scope": "ordered same-fixture point and bulk checks; no isolated speed ratio or large/fault/24h qualification", "samples": persistedSamples}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "oracle.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("LATENCY_NATIVE_ORACLE invocations=160 samples_equal=true deadline_negative=true parallel=%s serial=%s", parallelElapsed, serialElapsed)
}
