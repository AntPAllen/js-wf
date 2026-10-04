//go:build linux

package integration_test

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
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

const promiseKillType = "promise-kill-parent"
const promiseKillID = "boundary"
const promiseKillChildType = "promise-kill-child"

func promiseKillHandlers(log string) (map[string]worker.Handler, map[string]worker.ContinuationHandler) {
	payload, _ := json.Marshal(strings.Repeat("x", wf.MaxInlineTerminal))
	handlers := map[string]worker.Handler{
		promiseKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			if err := continuationKillLog(log, "initial"); err != nil {
				return nil, err
			}
			promise, err := wf.CallAsync(c, promiseKillChildType, json.RawMessage(`null`))
			if err != nil {
				return nil, err
			}
			raw, _ := json.Marshal(promise)
			if err := os.WriteFile(log+".promise", raw, 0600); err != nil {
				return nil, err
			}
			result, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(result, payload) {
				return nil, fmt.Errorf("initial child bytes changed")
			}
			return nil, wf.Continue(c, "finish_v1", promise)
		},
		promiseKillChildType: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
			return payload, continuationKillLog(log, "child")
		},
	}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		var promise wf.Promise
		if err := json.Unmarshal(locals, &promise); err != nil {
			return nil, err
		}
		result, err := wf.AwaitPromise(c, promise)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(result, payload) {
			return nil, fmt.Errorf("restored child bytes changed")
		}
		result[0] = '!'
		again, err := wf.AwaitPromise(c, promise)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(again, payload) {
			return nil, fmt.Errorf("restored promise alias")
		}
		return json.RawMessage(strconv.Itoa(len(again))), nil
	}}
	return handlers, stages
}

// Capture every candidate frame, including cuts before manifest publication.
type promiseKillResultPort struct {
	*continuationKillResultPort
	frameReady *atomic.Bool
}

func (p *promiseKillResultPort) PutBytes(ctx context.Context, name string, data []byte) error {
	var frame struct {
		Stage string `json:"stage"`
	}
	if json.Unmarshal(data, &frame) == nil && frame.Stage == "finish_v1" {
		metadata, err := json.Marshal(struct {
			Name string
			Data []byte
		}{name, data})
		if err != nil {
			return err
		}
		if err := os.WriteFile(p.stop.marker+".frame", metadata, 0600); err != nil {
			return err
		}
	}
	if err := p.continuationKillResultPort.PutBytes(ctx, name, data); err != nil {
		return err
	}
	if frame.Stage == "finish_v1" {
		p.frameReady.Store(true)
	}
	return nil
}

func TestContinuationPromiseKillChild(t *testing.T) {
	if os.Getenv("WF_PROMISE_KILL_CHILD") != "1" {
		t.Skip("promise worker process helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	nc, err := nats.Connect(os.Getenv("WF_PROMISE_KILL_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	log := os.Getenv("WF_PROMISE_KILL_LOG")
	port := &continuationKillSnapshotPort{SnapshotWritePort: journal.NewSnapshotPort(js), cut: os.Getenv("WF_PROMISE_KILL_CUT"), marker: os.Getenv("WF_PROMISE_KILL_MARKER")}
	handlers, stages := promiseKillHandlers(log)
	var frameReady atomic.Bool
	observer := func(event worker.OperationEvent) {
		if event.Error != "" || event.Type != promiseKillType || event.ID != promiseKillID || !frameReady.Load() {
			return
		}
		if event.Operation == "journal_append" && event.JournalKind == journal.Suspended {
			port.stopAt("after_suspended")
		}
		if event.Operation == "lease_release" {
			port.stopAt("after_handoff_release")
		}
	}
	w, err := worker.New(ctx, js, "promise-killed", handlers, worker.WithContinuations(promiseKillType, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(js, port)), worker.WithResultBlobPort(&promiseKillResultPort{continuationKillResultPort: &continuationKillResultPort{ResultBlobPort: worker.NewResultBlobPort(js), stop: port}, frameReady: &frameReady}), worker.WithOperationObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	parentPart := identity.Partition(promiseKillType, promiseKillID, provision.Partitions)
	done := make(chan error, 2)
	go func() { done <- w.RunPartition(ctx, parentPart) }()
	var promise wf.Promise
	for {
		raw, err := os.ReadFile(log + ".promise")
		if err == nil && json.Unmarshal(raw, &promise) == nil && promise.ChildID != "" {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("parent exited before child declaration: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	childPart := identity.Partition(promiseKillChildType, promise.ChildID, provision.Partitions)
	if childPart != parentPart {
		go func() { done <- w.RunPartition(ctx, childPart) }()
	}
	select {
	case err := <-done:
		t.Fatalf("worker exited before cut: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestContinuationPromiseSIGKILLAndFullServerRestart(t *testing.T) {
	if os.Getenv("WF_PROMISE_FULL_RESTART") != "1" {
		t.Skip("set WF_PROMISE_FULL_RESTART=1 for combined process cuts")
	}
	for _, cut := range []string{"before_frame", "after_frame", "before_manifest", "after_manifest", "after_journal_purge", "after_signal_purge", "after_suspended", "after_handoff_release"} {
		t.Run(cut, func(t *testing.T) { runPromiseKillRestart(t, cut) })
	}
}

func runPromiseKillRestart(t *testing.T, cut string) {
	t.Helper()
	started := time.Now()
	root := ""
	retained := os.Getenv("WF_PROMISE_ARTIFACT_PARENT") != ""
	if retained {
		parent := os.Getenv("WF_PROMISE_ARTIFACT_PARENT")
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		var err error
		root, err = os.MkdirTemp(parent, "promise-"+cut+"-")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("retained promise cut=%s artifacts=%s", cut, root)
	} else {
		root = t.TempDir()
	}
	type auditEvent struct {
		Stage            string    `json:"stage"`
		At               time.Time `json:"at"`
		ElapsedNS        int64     `json:"elapsed_ns"`
		Error            string    `json:"error,omitempty"`
		DeadlineExceeded bool      `json:"deadline_exceeded"`
	}
	var events []auditEvent
	record := func(stage string, err error) {
		if !retained {
			return
		}
		event := auditEvent{Stage: stage, At: time.Now().UTC(), ElapsedNS: time.Since(started).Nanoseconds(), DeadlineExceeded: errors.Is(err, context.DeadlineExceeded)}
		if err != nil {
			event.Error = err.Error()
		}
		events = append(events, event)
	}
	// Registered before cluster/worker cleanup, so the report records the final
	// verdict after those processes stop. The retained stores are never reopened.
	defer func() {
		if !retained {
			return
		}
		report := struct {
			Cut     string       `json:"cut"`
			Failed  bool         `json:"failed"`
			Started time.Time    `json:"started"`
			Ended   time.Time    `json:"ended"`
			Events  []auditEvent `json:"events"`
		}{cut, t.Failed(), started.UTC(), time.Now().UTC(), events}
		raw, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "promise-cut-audit.json"), append(raw, '\n'), 0600)
		}
		if err != nil {
			t.Errorf("retain promise cut audit: %v", err)
		}
	}()
	cluster, err := testcluster.StartPartitionableProcesses(filepath.Join(root, "servers"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if t.Failed() {
			for i := range cluster.Commands {
				raw, _ := os.ReadFile(cluster.LogPath(i))
				if len(raw) > 12000 {
					raw = raw[len(raw)-12000:]
				}
				t.Logf("server %d: %s", i, raw)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	all := make([]jetstream.JetStream, 3)
	connect := func() {
		for i, nc := range cluster.Clients {
			all[i], err = jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	connect()
	for {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	c := client.New(all[0])
	handle, err := c.Start(ctx, promiseKillType, promiseKillID, json.RawMessage(`null`))
	if err != nil {
		t.Fatal(err)
	}
	marker, log := filepath.Join(root, "cut"), filepath.Join(root, "effects")
	output, err := os.Create(filepath.Join(root, "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestContinuationPromiseKillChild$")
	child.Env = append(os.Environ(), "WF_PROMISE_KILL_CHILD=1", "WF_PROMISE_KILL_URL="+cluster.ClientURL(1), "WF_PROMISE_KILL_CUT="+cut, "WF_PROMISE_KILL_MARKER="+marker, "WF_PROMISE_KILL_LOG="+log)
	child.Stdout, child.Stderr = output, output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			<-exited
		}
		if t.Failed() {
			raw, _ := os.ReadFile(filepath.Join(root, "worker.log"))
			t.Logf("worker: %s", raw)
		}
	}()
	for {
		raw, err := os.ReadFile(marker)
		if err == nil && string(raw) == cut {
			break
		}
		select {
		case err := <-exited:
			waited = true
			t.Fatalf("worker exited before cut: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	record("cut_observed", nil)
	store := journal.New(all[0])
	record("journal_read_begin", nil)
	before, _, err := store.Read(ctx, promiseKillType, promiseKillID)
	record("journal_read_end", err)
	if err != nil || len(before) == 0 {
		t.Fatalf("cut history=%v err=%v", before, err)
	}
	last := before[len(before)-1]
	wantKind := journal.StepCompleted
	if cut == "before_frame" || cut == "after_frame" {
		wantKind = journal.StepRequested
	}
	handoff := cut == "after_suspended" || cut == "after_handoff_release"
	if handoff {
		wantKind = journal.Suspended
	}
	if last.Kind != wantKind {
		t.Fatalf("cut tail=%+v", last)
	}
	if handoff {
		if string(last.Payload) != `{"waiting_on":"continuation:finish_v1"}` {
			t.Fatalf("incorrect handoff suspension: %s", last.Payload)
		}
		leases, err := all[0].KeyValue(ctx, "WF_LEASE")
		if err != nil {
			t.Fatal(err)
		}
		_, leaseErr := leases.Get(ctx, identity.Key(promiseKillType, promiseKillID))
		if cut == "after_suspended" && leaseErr != nil {
			t.Fatalf("lease absent before handoff: %v", leaseErr)
		}
		if cut == "after_handoff_release" && !errors.Is(leaseErr, jetstream.ErrKeyNotFound) {
			t.Fatalf("lease retained after handoff: %v", leaseErr)
		}
		runs, err := all[0].Stream(ctx, "WF_RUN")
		if err != nil {
			t.Fatal(err)
		}
		info, err := runs.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		messageID := fmt.Sprintf("continuation:%d:%d", handle.InvSeq, before[len(before)-2].Sequence)
		found := 0
		for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
			msg, err := runs.GetMsg(ctx, seq)
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if msg.Header.Get("Nats-Msg-Id") == messageID {
				found++
			}
		}
		want := 0
		if cut == "after_handoff_release" {
			want = 1
		}
		if found != want {
			t.Fatalf("handoff messages=%d want=%d", found, want)
		}
	}
	var promise wf.Promise
	raw, err := os.ReadFile(log + ".promise")
	if err != nil || json.Unmarshal(raw, &promise) != nil {
		t.Fatalf("promise=%s err=%v", raw, err)
	}
	var frameRaw []byte
	var abandoned string
	unpublished := cut == "before_frame" || cut == "after_frame" || cut == "before_manifest"
	if unpublished {
		var frame struct {
			Name string
			Data []byte
		}
		raw, err := os.ReadFile(marker + ".frame")
		if err != nil || json.Unmarshal(raw, &frame) != nil {
			t.Fatalf("pending frame=%s err=%v", raw, err)
		}
		frameRaw = frame.Data
		// A completed checkpoint already names its frame; manifest repair reuses it.
		// Only an uncompleted frame candidate is abandoned on epoch replacement.
		if cut != "before_manifest" {
			abandoned = frame.Name
		}
	} else {
		record("checkpoint_read_begin", nil)
		view, err := store.ReadCheckpoint(ctx, promiseKillType, promiseKillID, handle.InvSeq)
		record("checkpoint_read_end", err)
		if err != nil || view == nil {
			t.Fatalf("published frame=%v err=%v", view, err)
		}
		frameRaw = view.Frame
	}
	var frame struct {
		PromiseOutcomes map[string]json.RawMessage `json:"promise_outcomes"`
	}
	if err := json.Unmarshal(frameRaw, &frame); err != nil {
		t.Fatal(err)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(frame.PromiseOutcomes[promise.SignalName], &outcome); err != nil || outcome.ResultRef == "" || outcome.ResultHash == "" || len(outcome.Result) != 0 {
		t.Fatalf("frame promise=%+v err=%v", outcome, err)
	}
	effects, err := os.ReadFile(log)
	if err != nil || strings.Count(string(effects), "child\n") != 1 {
		t.Fatalf("effects=%s err=%v", effects, err)
	}
	initialBefore := strings.Count(string(effects), "initial\n")
	killedAt := time.Now()
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := <-exited
	waited = true
	status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("not SIGKILL: %v %v", child.ProcessState, waitErr)
	}
	for i := range cluster.Commands {
		if err := cluster.KillNode(i); err != nil {
			t.Fatal(err)
		}
	}
	for i := range cluster.Commands {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatal(err)
		}
	}
	connect()
	ready, stopReady := context.WithTimeout(ctx, 25*time.Second)
	_, err = matrixRouteCounts(ready, cluster, [3]int{8, 8, 8})
	if err == nil {
		err = waitMatrixWorkflowReplicas(ready, all[0])
	}
	stopReady()
	if err != nil {
		t.Fatal(err)
	}
	healedAt := time.Now()
	c = client.New(all[0])
	store = journal.New(all[0])
	confirmed, _, err := store.Read(ctx, promiseKillType, promiseKillID)
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(confirmed)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("restart changed confirmed prefix: %v", err)
	}
	handlers, stages := promiseKillHandlers(log)
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	blobs := &continuationPromiseBlobs{ResultBlobPort: worker.NewResultBlobPort(all[2]), childObject: outcome.ResultRef}
	options := []worker.Option{worker.WithContinuations(promiseKillType, stages), worker.WithResultBlobPort(blobs)}
	if !unpublished {
		options = append(options, worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	}
	successor, err := worker.New(ctx, all[2], "promise-successor", handlers, options...)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	enabledAt := time.Now()
	if _, err := c.Signal(ctx, promiseKillType, promiseKillID, "gate", json.RawMessage(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	done := make(chan error, 1)
	go func() {
		done <- successor.RunPartition(runCtx, identity.Partition(promiseKillType, promiseKillID, provision.Partitions))
	}()
	type awaitResult struct {
		raw json.RawMessage
		err error
	}
	awaited := make(chan awaitResult, 1)
	go func() { raw, err := c.Await(ctx, promiseKillType, promiseKillID); awaited <- awaitResult{raw, err} }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var result json.RawMessage
	var resultErr error
waitTerminal:
	for {
		select {
		case terminal := <-awaited:
			result, resultErr = terminal.raw, terminal.err
			break waitTerminal
		case <-ticker.C:
			current, _, err := store.Read(ctx, promiseKillType, promiseKillID)
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range current {
				if record.Sequence <= last.Sequence || record.Kind != journal.Suspended {
					continue
				}
				var suspension struct {
					WaitingOn string `json:"waiting_on"`
				}
				if err := json.Unmarshal(record.Payload, &suspension); err != nil {
					t.Fatal(err)
				}
				if suspension.WaitingOn == "signal:"+promise.SignalName {
					t.Fatalf("successor suspended on already-resolved frame-held promise %s", promise.SignalName)
				}
			}
		}
	}
	finishedAt := time.Now()
	stopRun()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(strings.Repeat("x", wf.MaxInlineTerminal))
	if resultErr != nil || string(result) != strconv.Itoa(len(payload)) {
		t.Fatalf("result=%s err=%v", result, resultErr)
	}
	if finishedAt.Sub(enabledAt) >= 30*time.Second || finishedAt.Sub(killedAt) >= 30*time.Second {
		t.Fatalf("recovery exceeds 30s: signal=%v kill=%v", finishedAt.Sub(enabledAt), finishedAt.Sub(killedAt))
	}
	effects, err = os.ReadFile(log)
	wantInitial := initialBefore
	if unpublished {
		wantInitial++
	}
	if err != nil || strings.Count(string(effects), "initial\n") != wantInitial || strings.Count(string(effects), "child\n") != 1 {
		t.Fatalf("handler replay=%s want_initial=%d err=%v", effects, wantInitial, err)
	}
	if !unpublished && (guard.archives != 0 || guard.frames == 0 || blobs.childReads != 1) {
		t.Fatalf("archive=%d frames=%d child_reads=%d", guard.archives, guard.frames, blobs.childReads)
	}
	records, _, err := store.Read(ctx, promiseKillType, promiseKillID)
	if err != nil || len(records) <= len(before) || records[len(records)-1].Epoch <= last.Epoch {
		t.Fatalf("replacement did not advance epoch: %v", err)
	}
	b, _ = json.Marshal(records[:len(before)])
	if !bytes.Equal(a, b) {
		t.Fatal("successor changed confirmed prefix")
	}
	declared, consumed := 0, 0
	for _, record := range records {
		if record.Kind == journal.StepRequested {
			var request struct{ Kind string }
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				t.Fatal(err)
			}
			if request.Kind == "call_async" {
				declared++
			}
		}
		if record.Kind == journal.SignalConsumed {
			var signal struct{ Name string }
			if err := json.Unmarshal(record.Payload, &signal); err != nil {
				t.Fatal(err)
			}
			if signal.Name == promise.SignalName {
				consumed++
			}
		}
	}
	if declared != 1 || consumed != 1 {
		t.Fatalf("child declarations=%d consumed=%d", declared, consumed)
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 2 || report.Terminal != 2 {
		t.Fatalf("audit=%+v err=%v", report, err)
	}
	for _, peer := range all {
		value, err := client.New(peer).Await(ctx, promiseKillType, promiseKillID)
		if err != nil || !bytes.Equal(value, result) {
			t.Fatalf("peer result=%s err=%v", value, err)
		}
	}
	view, err := store.ReadCheckpoint(ctx, promiseKillType, promiseKillID, handle.InvSeq)
	if err != nil || view == nil || view.Snapshot.Runtime.Object == abandoned {
		t.Fatalf("replacement checkpoint=%v err=%v", view, err)
	}
	if err := retention.Purge(ctx, all[0], promiseKillChildType, promise.ChildID, time.Minute); err != nil {
		t.Fatal(err)
	}
	kept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || kept.Referenced != 3 {
		t.Fatalf("retired child live references=%+v err=%v", kept, err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	childBytes, err := objects.GetBytes(ctx, outcome.ResultRef)
	if err != nil || !bytes.Equal(childBytes, payload) {
		t.Fatalf("retired child collected: %v", err)
	}
	if abandoned != "" {
		if _, err := objects.GetInfo(ctx, abandoned); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("abandoned frame survived: %v", err)
		}
	}
	history, _ := json.Marshal(records)
	replayed, err := wf.ReplayWithContinuations(history, func(c *wf.Context) (json.RawMessage, error) {
		return handlers[promiseKillType](c, json.RawMessage(`null`))
	}, map[string]wf.ReplayContinuation[json.RawMessage]{"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
		return stages["finish_v1"](c, json.RawMessage(`null`), locals)
	}}, wf.ReplayOptions{Type: promiseKillType, ID: promiseKillID, InvSeq: handle.InvSeq, Objects: map[string][]byte{outcome.ResultRef: childBytes, view.Snapshot.Runtime.Object: view.Frame}})
	if err != nil || !bytes.Equal(replayed, result) {
		t.Fatalf("offline=%s err=%v", replayed, err)
	}
	if err := retention.Purge(ctx, all[0], promiseKillType, promiseKillID, time.Minute); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || reclaimed.Deleted != 3 || reclaimed.Referenced != 0 {
		t.Fatalf("final collection=%+v err=%v", reclaimed, err)
	}
	t.Logf("promise combined cut=%s child_bytes=%d frame_bytes=%d kill_recovery=%v heal_recovery=%v signal_recovery=%v child_reads=%d archive_reads=%d abandoned_deleted=%d final_deleted=%d", cut, len(payload), len(frameRaw), finishedAt.Sub(killedAt), finishedAt.Sub(healedAt), finishedAt.Sub(enabledAt), blobs.childReads, guard.archives, kept.Deleted, reclaimed.Deleted)
}
