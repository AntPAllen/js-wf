package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

var replayPluginOnce sync.Once
var replayPluginPath, replayPluginDir string
var replayPluginErr error

func TestMain(m *testing.M) {
	code := m.Run()
	if replayPluginDir != "" {
		_ = os.RemoveAll(replayPluginDir)
	}
	os.Exit(code)
}

func buildReplayPlugin(t *testing.T) string {
	t.Helper()
	replayPluginOnce.Do(func() {
		replayPluginDir, replayPluginErr = os.MkdirTemp("", "js-wf-replay-plugin-")
		if replayPluginErr != nil {
			return
		}
		replayPluginPath = filepath.Join(replayPluginDir, "handler.so")
		buildArgs := []string{"build"}
		if replayPluginRace {
			buildArgs = append(buildArgs, "-race")
		}
		buildArgs = append(buildArgs, "-buildmode=plugin", "-o", replayPluginPath, "./testdata/replayplugin")
		build := exec.Command("go", buildArgs...)
		if output, err := build.CombinedOutput(); err != nil {
			replayPluginErr = fmt.Errorf("build replay handler plugin: %w: %s", err, output)
		}
	})
	if replayPluginErr != nil {
		t.Fatal(replayPluginErr)
	}
	return replayPluginPath
}

func TestOperatorCommands(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "operator"
	ack, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(js)
	seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := json.Marshal(wf.Outcome{InvSeq: ack.Sequence, Result: []byte(`1`)})
	if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 1, Kind: journal.Completed, Payload: terminal}, seq); err != nil {
		t.Fatal(err)
	}
	state, _ := js.KeyValue(ctx, "WF_STATE")
	if _, err := state.Put(ctx, identity.Key(typ, id), terminal); err != nil {
		t.Fatal(err)
	}
	base := []string{"-url", cluster.Servers[0].ClientURL()}
	call := func(args ...string) []byte {
		t.Helper()
		var output bytes.Buffer
		if err := run(append(append([]string{}, base...), args...), &output); err != nil {
			t.Fatal(err)
		}
		return output.Bytes()
	}
	call("assignment-init", "owner-a", "owner-b")
	var assignmentRow struct {
		Owner    string `json:"owner"`
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(call("assignment-get", "0"), &assignmentRow); err != nil || assignmentRow.Owner != "owner-a" || assignmentRow.Revision == 0 {
		t.Fatalf("assignment=%+v err=%v", assignmentRow, err)
	}
	oldRevision := assignmentRow.Revision
	if err := json.Unmarshal(call("assignment-move", "0", "owner-b", strconv.FormatUint(oldRevision, 10)), &assignmentRow); err != nil || assignmentRow.Owner != "owner-b" || assignmentRow.Revision <= oldRevision {
		t.Fatalf("moved assignment=%+v err=%v", assignmentRow, err)
	}
	var ignored bytes.Buffer
	if err := run(append(append([]string{}, base...), "assignment-move", "0", "owner-a", strconv.FormatUint(oldRevision, 10)), &ignored); !errors.Is(err, assignment.ErrConflict) {
		t.Fatalf("stale move: %v", err)
	}
	var rows []visibility.Row
	if err := json.Unmarshal(call("-rebuild", "list", "completed"), &rows); err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("list=%+v err=%v", rows, err)
	}
	var lag struct {
		Pending uint64 `json:"pending"`
	}
	if err := json.Unmarshal(call("lag"), &lag); err != nil || lag.Pending != 2 {
		t.Fatalf("lag=%+v err=%v", lag, err)
	}
	var scan struct {
		Reenqueued int `json:"reenqueued"`
	}
	if err := json.Unmarshal(call("scan-suspended"), &scan); err != nil || scan.Reenqueued != 0 {
		t.Fatalf("scan=%+v err=%v", scan, err)
	}
	var described struct {
		Row     visibility.Row   `json:"row"`
		Journal []journal.Record `json:"journal"`
	}
	if err := json.Unmarshal(call("describe", typ, id), &described); err != nil || described.Row.Status != "completed" || len(described.Journal) != 2 {
		t.Fatalf("describe=%+v err=%v", described, err)
	}
	var exported []journal.Record
	if err := json.Unmarshal(call("export-journal", typ, id), &exported); err != nil || len(exported) != 2 {
		t.Fatalf("export=%+v err=%v", exported, err)
	}
	pluginPath := buildReplayPlugin(t)
	const replayID = "replay-target"
	replayWorker, err := worker.New(ctx, js, "operator-replay-worker", map[string]worker.Handler{typ: func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "double", input.N, func(context.Context) (int, error) { return input.N * 2, nil })
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}})
	if err != nil {
		t.Fatal(err)
	}
	replayCtx, stopReplayWorker := context.WithCancel(ctx)
	replayDone := make(chan error, 1)
	go func() {
		replayDone <- replayWorker.RunPartition(replayCtx, identity.Partition(typ, replayID, provision.Partitions))
	}()
	if _, err := client.New(js).Start(ctx, typ, replayID, []byte(`{"n":5}`)); err != nil {
		t.Fatal(err)
	}
	if value, err := client.New(js).Await(ctx, typ, replayID); err != nil || string(value) != "10" {
		t.Fatalf("replay fixture result=%s err=%v", value, err)
	}
	stopReplayWorker()
	if err := <-replayDone; err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "effect")
	t.Setenv("WF_REPLAY_EFFECT_MARKER", marker)
	var replayed replayReport
	if err := json.Unmarshal(call("-handler-plugin", pluginPath, "replay", typ, replayID), &replayed); err != nil || replayed.Type != typ || replayed.ID != replayID || string(replayed.Result) != "10" || replayed.JournalEntries != 4 {
		t.Fatalf("replay report=%+v err=%v", replayed, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("effect ran during replay: %v", err)
	}
	var ignoredReplay bytes.Buffer
	if err := run(append(append([]string{}, base...), "-handler-plugin", pluginPath, "-handler-symbol", "ChangedWorkflow", "replay", typ, replayID), &ignoredReplay); !errors.Is(err, wf.ErrNonDeterministic) {
		t.Fatalf("changed workflow replay: %v", err)
	}
	ignoredReplay.Reset()
	if err := run(append(append([]string{}, base...), "-handler-plugin", pluginPath, "-handler-symbol", "ChangedResult", "replay", typ, replayID), &ignoredReplay); err == nil || !strings.Contains(err.Error(), "differs from terminal outcome") {
		t.Fatalf("changed result replay: %v", err)
	}
	const blobID = "replay-blobs"
	blobWorker, err := worker.New(ctx, js, "operator-blob-replay-worker", map[string]worker.Handler{typ: func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var input struct {
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "large", len(input.Payload), func(context.Context) (string, error) {
			return strings.Repeat("z", wf.MaxInlineResult+1), nil
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(len(value))
	}})
	if err != nil {
		t.Fatal(err)
	}
	blobCtx, stopBlobWorker := context.WithCancel(ctx)
	blobDone := make(chan error, 1)
	go func() {
		blobDone <- blobWorker.RunPartition(blobCtx, identity.Partition(typ, blobID, provision.Partitions))
	}()
	largeInput, _ := json.Marshal(map[string]string{"payload": strings.Repeat("x", client.MaxInlineInput)})
	if _, err := client.New(js).Start(ctx, typ, blobID, largeInput); err != nil {
		t.Fatal(err)
	}
	if value, err := client.New(js).Await(ctx, typ, blobID); err != nil || string(value) != strconv.Itoa(wf.MaxInlineResult+1) {
		t.Fatalf("blob replay fixture result=%s err=%v", value, err)
	}
	stopBlobWorker()
	if err := <-blobDone; err != nil {
		t.Fatal(err)
	}
	var blobReplay replayReport
	if err := json.Unmarshal(call("-handler-plugin", pluginPath, "-handler-symbol", "LargeWorkflow", "replay", typ, blobID), &blobReplay); err != nil || string(blobReplay.Result) != strconv.Itoa(wf.MaxInlineResult+1) {
		t.Fatalf("blob replay report=%+v err=%v", blobReplay, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("large effect ran during replay: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "replay.json")
	if err := os.WriteFile(bundlePath, call("export-replay", typ, blobID), 0600); err != nil {
		t.Fatal(err)
	}
	var offline bytes.Buffer
	if err := run([]string{"-url", "nats://127.0.0.1:1", "-handler-plugin", pluginPath, "-handler-symbol", "LargeWorkflow", "-replay-bundle", bundlePath, "replay"}, &offline); err != nil {
		t.Fatalf("offline replay with unreachable NATS: %v", err)
	}
	var offlineReport replayReport
	if err := json.Unmarshal(offline.Bytes(), &offlineReport); err != nil || string(offlineReport.Result) != strconv.Itoa(wf.MaxInlineResult+1) {
		t.Fatalf("offline replay=%+v err=%v", offlineReport, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("large effect ran during offline replay: %v", err)
	}
	bundle, err := loadReplayBundle(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	for name := range bundle.Objects {
		bundle.Objects[name] = []byte(`"corrupt"`)
		break
	}
	corruptPath := filepath.Join(t.TempDir(), "corrupt.json")
	corruptBytes, _ := json.Marshal(bundle)
	if err := os.WriteFile(corruptPath, corruptBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-url", "nats://127.0.0.1:1", "-handler-plugin", pluginPath, "-handler-symbol", "LargeWorkflow", "-replay-bundle", corruptPath, "replay"}, &offline); !errors.Is(err, wf.ErrCorruptJournal) {
		t.Fatalf("corrupt offline replay object: %v", err)
	}
	if _, err := client.New(js).Start(ctx, typ, "cancel-target", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	var cancellation struct {
		Requested bool   `json:"requested"`
		SignalSeq uint64 `json:"signal_seq"`
	}
	if err := json.Unmarshal(call("cancel", typ, "cancel-target"), &cancellation); err != nil || !cancellation.Requested || cancellation.SignalSeq == 0 {
		t.Fatalf("cancel=%+v err=%v", cancellation, err)
	}
	call("-grace", "1ns", "purge", typ, id)
	if err := json.Unmarshal(call("-rebuild", "list", "completed"), &rows); err != nil || len(rows) != 2 || rows[0].ID == id || rows[1].ID == id {
		t.Fatalf("list after purge=%+v err=%v", rows, err)
	}
	var tombstonePage struct {
		Eligible int `json:"eligible"`
		Deleted  int `json:"deleted"`
	}
	if err := json.Unmarshal(call("-budget", "100", "scan-tombstones"), &tombstonePage); err != nil || tombstonePage.Eligible != 1 || tombstonePage.Deleted != 0 {
		t.Fatalf("dry tombstone page=%+v err=%v", tombstonePage, err)
	}
	var sweep struct {
		Deleted int `json:"deleted"`
	}
	if err := json.Unmarshal(call("sweep-tombstones"), &sweep); err != nil || sweep.Deleted != 1 {
		t.Fatalf("sweep=%+v err=%v", sweep, err)
	}
}

func TestListAttributeFilter(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "attribute-cli"
	if _, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	j := journal.New(js)
	entries := []journal.Entry{
		{Index: 0, Epoch: 1, Kind: journal.Started},
		{Index: 1, Epoch: 1, Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"search_attributes","name":"set"}`)},
		{Index: 2, Epoch: 1, Kind: journal.StepCompleted, Payload: json.RawMessage(`{"result":{"team":"a=b"}}`)},
		{Index: 3, Epoch: 1, Kind: journal.Completed},
	}
	var tail uint64
	for _, entry := range entries {
		tail, err = j.Append(ctx, typ, id, entry, tail)
		if err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"-url", cluster.Servers[0].ClientURL(), "-rebuild", "-attribute", "team=a=b", "list", "completed"}
	var output bytes.Buffer
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	var rows []visibility.Row
	if err := json.Unmarshal(output.Bytes(), &rows); err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("filtered rows=%+v err=%v", rows, err)
	}
	output.Reset()
	args[4] = "team=other"
	if err := run(args, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &rows); err != nil || len(rows) != 0 {
		t.Fatalf("unmatched rows=%+v err=%v", rows, err)
	}
}

func TestJournalCapacityAlert(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const limit int64 = 32 * 1024
	if err := provision.EnsureWithJournalLimit(ctx, js, 1, limit); err != nil {
		t.Fatal(err)
	}
	if err := provision.EnsureWithJournalLimit(ctx, js, 1, limit); err != nil {
		t.Fatalf("same cap should be idempotent: %v", err)
	}
	if err := provision.EnsureWithJournalLimit(ctx, js, 1, 2*limit); err == nil {
		t.Fatal("different journal cap was silently adopted")
	}
	capacity, err := provision.CheckJournalCapacity(ctx, js)
	if err != nil || capacity.Alert || capacity.LimitBytes != limit {
		t.Fatalf("initial capacity=%+v err=%v", capacity, err)
	}
	var output bytes.Buffer
	args := []string{"-url", cluster.Servers[0].ClientURL(), "journal-capacity"}
	if err := run(args, &output); err != nil {
		t.Fatalf("healthy capacity command: %v", err)
	}
	stepPayload, _ := json.Marshal(strings.Repeat("x", 1024))
	store := journal.New(js)
	var seq uint64
	for index := 0; index < 50; index++ {
		kind := journal.StepRequested
		if index == 0 {
			kind = journal.Started
		}
		seq, err = store.Append(ctx, "capacity", "alert", journal.Entry{Index: uint64(index), Epoch: 1, Kind: kind, Payload: stepPayload}, seq)
		if err != nil {
			t.Fatal(err)
		}
		capacity, err = provision.CheckJournalCapacity(ctx, js)
		if err != nil {
			t.Fatal(err)
		}
		if capacity.Alert {
			break
		}
	}
	if !capacity.Alert || capacity.Utilization < .7 || capacity.Utilization >= 1 {
		t.Fatalf("expected pre-quota warning: %+v", capacity)
	}
	output.Reset()
	if err := run(args, &output); err == nil || !strings.Contains(err.Error(), "70% alert threshold") {
		t.Fatalf("capacity command did not alert: %v", err)
	}
	var reported provision.JournalCapacity
	if err := json.Unmarshal(output.Bytes(), &reported); err != nil || !reported.Alert || reported.UsedBytes != capacity.UsedBytes {
		t.Fatalf("capacity report=%+v err=%v", reported, err)
	}
}

func TestOperatorReplayFailedAndSuspended(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	pluginPath := buildReplayPlugin(t)
	base := []string{"-url", cluster.Servers[0].ClientURL(), "-handler-plugin", pluginPath}
	const typ = "replay-state"
	startAndWait := func(id string, handler worker.Handler, want journal.Kind, options ...worker.Option) {
		t.Helper()
		w, err := worker.New(ctx, js, "replay-state-"+id, map[string]worker.Handler{typ: handler}, options...)
		if err != nil {
			t.Fatal(err)
		}
		workerCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
		defer func() {
			stop()
			if err := <-done; err != nil {
				t.Errorf("worker %s: %v", id, err)
			}
		}()
		if _, err := client.New(js).Start(ctx, typ, id, []byte(`null`)); err != nil {
			t.Fatal(err)
		}
		for ctx.Err() == nil {
			records, _, err := journal.New(js).Read(ctx, typ, id)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) > 0 && records[len(records)-1].Kind == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("workflow %s did not reach %s: %v", id, want, ctx.Err())
	}
	startAndWait("failed-step", func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.Run(c, "fail", 7, func(context.Context) (int, error) { return 0, errors.New("boom") })
		return nil, err
	}, journal.Failed)
	startAndWait("failed-panic", func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		panic("boom")
	}, journal.Failed, worker.WithMaxPanicAttempts(1))
	startAndWait("waiting-signal", func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.AwaitSignal(c, "go")
		return nil, err
	}, journal.Suspended)
	startAndWait("waiting-timer", func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		return nil, wf.Sleep(c, "later", time.Hour)
	}, journal.Suspended)
	const cancelledID = "cancelled"
	cancelWorker, err := worker.New(ctx, js, "replay-state-cancelled", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		_, err := wf.AwaitSignal(c, "go")
		return nil, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	cancelCtx, stopCancelWorker := context.WithCancel(ctx)
	cancelDone := make(chan error, 1)
	go func() {
		cancelDone <- cancelWorker.RunPartition(cancelCtx, identity.Partition(typ, cancelledID, provision.Partitions))
	}()
	cancelClient := client.New(js)
	if _, err := cancelClient.Start(ctx, typ, cancelledID, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		records, _, err := journal.New(js).Read(ctx, typ, cancelledID)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := cancelClient.Cancel(ctx, typ, cancelledID); err != nil {
		t.Fatal(err)
	}
	if _, err := cancelClient.Await(ctx, typ, cancelledID); !errors.Is(err, client.ErrCancelled) {
		t.Fatalf("cancelled replay fixture: %v", err)
	}
	stopCancelWorker()
	if err := <-cancelDone; err != nil {
		t.Fatal(err)
	}
	const cancelBeforeID = "cancel-before-handler"
	if _, err := cancelClient.Start(ctx, typ, cancelBeforeID, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if _, err := cancelClient.Cancel(ctx, typ, cancelBeforeID); err != nil {
		t.Fatal(err)
	}
	beforeWorker, err := worker.New(ctx, js, "replay-state-cancel-before", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		t.Error("handler ran after cancellation was stored")
		return nil, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	beforeCtx, stopBefore := context.WithCancel(ctx)
	beforeDone := make(chan error, 1)
	go func() {
		beforeDone <- beforeWorker.RunPartition(beforeCtx, identity.Partition(typ, cancelBeforeID, provision.Partitions))
	}()
	if _, err := cancelClient.Await(ctx, typ, cancelBeforeID); !errors.Is(err, client.ErrCancelled) {
		t.Fatalf("pre-handler cancellation fixture: %v", err)
	}
	stopBefore()
	if err := <-beforeDone; err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "effect")
	t.Setenv("WF_REPLAY_EFFECT_MARKER", marker)
	check := func(id, symbol, status, detail string) {
		t.Helper()
		var out bytes.Buffer
		args := append(append([]string{}, base...), "-handler-symbol", symbol, "replay", typ, id)
		if err := run(args, &out); err != nil {
			t.Fatalf("replay %s: %v", id, err)
		}
		var report replayReport
		if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Status != status || report.ID != id || status == "failed" && report.Error != detail || status == "suspended" && report.WaitingOn != detail {
			t.Fatalf("replay %s report=%+v err=%v", id, report, err)
		}
	}
	check("failed-step", "FailWorkflow", "failed", "boom")
	check("failed-panic", "PanicWorkflow", "failed", "workflow panic: boom")
	check("waiting-signal", "WaitSignalWorkflow", "suspended", "signal:go")
	check("waiting-timer", "WaitTimerWorkflow", "suspended", "timer:later")
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed effect ran during replay: %v", err)
	}
	var changed bytes.Buffer
	if err := run(append(append([]string{}, base...), "-handler-symbol", "ChangedFailure", "replay", typ, "failed-step"), &changed); err == nil || !strings.Contains(err.Error(), "replayed error differs") {
		t.Fatalf("changed failure replay: %v", err)
	}
	changed.Reset()
	if err := run(append(append([]string{}, base...), "-handler-symbol", "ImmediateFailure", "replay", typ, "failed-step"), &changed); !errors.Is(err, wf.ErrNonDeterministic) {
		t.Fatalf("failure skipped recorded step: %v", err)
	}
	changed.Reset()
	if err := run(append(append([]string{}, base...), "-handler-symbol", "ChangedWait", "replay", typ, "waiting-signal"), &changed); !errors.Is(err, wf.ErrNonDeterministic) {
		t.Fatalf("changed suspended wait: %v", err)
	}
	check(cancelledID, "WaitSignalWorkflow", "failed", client.ErrCancelled.Error())
	check(cancelBeforeID, "WaitSignalWorkflow", "failed", client.ErrCancelled.Error())
	changed.Reset()
	if err := run(append(append([]string{}, base...), "-handler-symbol", "ChangedWait", "replay", typ, cancelledID), &changed); err == nil || !strings.Contains(err.Error(), "differs from prior suspension") {
		t.Fatalf("changed pre-cancellation wait: %v", err)
	}
	for _, caseID := range []string{"failed-step", "waiting-timer", cancelledID} {
		var exported bytes.Buffer
		if err := run([]string{"-url", cluster.Servers[0].ClientURL(), "export-replay", typ, caseID}, &exported); err != nil {
			t.Fatal(err)
		}
		bundlePath := filepath.Join(t.TempDir(), caseID+".json")
		if err := os.WriteFile(bundlePath, exported.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		symbol := "FailWorkflow"
		if caseID == "waiting-timer" {
			symbol = "WaitTimerWorkflow"
		} else if caseID == cancelledID {
			symbol = "WaitSignalWorkflow"
		}
		var offline bytes.Buffer
		if err := run([]string{"-url", "nats://127.0.0.1:1", "-handler-plugin", pluginPath, "-handler-symbol", symbol, "-replay-bundle", bundlePath, "replay"}, &offline); err != nil {
			t.Fatalf("offline replay %s: %v", caseID, err)
		}
		if caseID == cancelledID {
			bundle, err := loadReplayBundle(bundlePath)
			if err != nil {
				t.Fatal(err)
			}
			for i := range bundle.Journal {
				if bundle.Journal[i].Kind != journal.SignalConsumed || !bytes.Contains(bundle.Journal[i].Payload, []byte(client.CancelSignalName)) {
					continue
				}
				var signal map[string]any
				if err := json.Unmarshal(bundle.Journal[i].Payload, &signal); err != nil {
					t.Fatal(err)
				}
				signal["name"] = "ordinary"
				bundle.Journal[i].Payload, _ = json.Marshal(signal)
			}
			corrupt, _ := json.Marshal(bundle)
			corruptPath := filepath.Join(t.TempDir(), "missing-cancel.json")
			if err := os.WriteFile(corruptPath, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			offline.Reset()
			if err := run([]string{"-url", "nats://127.0.0.1:1", "-handler-plugin", pluginPath, "-handler-symbol", symbol, "-replay-bundle", corruptPath, "replay"}, &offline); err == nil || !strings.Contains(err.Error(), "no consumed cancellation signal") {
				t.Fatalf("missing cancellation signal replay: %v", err)
			}
		}
	}
}
