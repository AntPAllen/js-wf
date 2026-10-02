//go:build linux

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

const limitKillType, limitKillID = "checkpoint-limit-kill", "global"

func limitKillLog(path, line string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, line); err != nil {
		return err
	}
	return f.Sync()
}

func limitKillHandlers(path string, budget uint64, replacement bool) (map[string]Handler, map[string]ContinuationHandler) {
	padding := int((budget - 16) / 2)
	pad := func(c *wf.Context, first, last int) error {
		for i := first; i < last; i++ {
			if err := c.SetState("padding", i); err != nil {
				return err
			}
		}
		return nil
	}
	handlers := map[string]Handler{limitKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := limitKillLog(path, "initial"); err != nil {
			return nil, err
		}
		if replacement {
			return nil, fmt.Errorf("replacement reran initial prefix")
		}
		if err := c.SetState("value", 10); err != nil {
			return nil, err
		}
		if err := pad(c, 0, padding/2); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", 10)
	}}
	stages := map[string]ContinuationHandler{
		"middle_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			if err := limitKillLog(path, "middle"); err != nil {
				return nil, err
			}
			if replacement {
				return nil, fmt.Errorf("replacement reran middle prefix")
			}
			var value int
			if found, err := c.GetState("value", &value); err != nil || !found || value != 10 || string(locals) != "10" {
				return nil, fmt.Errorf("middle state mismatch: %v", err)
			}
			if err := pad(c, padding/2, padding); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", 10)
		},
		"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			if err := limitKillLog(path, "finish"); err != nil {
				return nil, err
			}
			if string(locals) != "10" {
				return nil, fmt.Errorf("finish locals mismatch")
			}
			if _, err := wf.AwaitSignal(c, "gate"); err != nil {
				return nil, err
			}
			_, err := wf.Run(c, "must_not_run", 10, func(effectCtx context.Context) (int, error) {
				if err := limitKillLog(path, "effect"); err != nil {
					return 0, err
				}
				if replacement {
					// The controller detects this durable violation and cancels the
					// worker. Do not require a terminal append beyond the hard cap.
					<-effectCtx.Done()
					return 0, effectCtx.Err()
				}
				return 20, nil
			})
			return json.RawMessage(`20`), err
		},
	}
	return handlers, stages
}

func TestContinuationLimitKillChild(t *testing.T) {
	if os.Getenv("WF_LIMIT_KILL_CHILD") != "1" {
		t.Skip("continuation limit process helper")
	}
	budget, err := strconv.ParseUint(os.Getenv("WF_LIMIT_KILL_BUDGET"), 10, 64)
	if err != nil || (budget != 16 && budget != 20 && budget != journal.MaxEntries) {
		t.Fatal("invalid fixture budget")
	}
	nc, err := nats.Connect(os.Getenv("WF_LIMIT_KILL_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	path, cut := os.Getenv("WF_LIMIT_KILL_PATH"), os.Getenv("WF_LIMIT_KILL_CUT")
	handlers, stages := limitKillHandlers(path+".handlers", budget, false)
	observer := func(e OperationEvent) {
		if e.Operation != "journal_append" || e.Error != "" {
			return
		}
		if e.JournalKind == journal.Suspended && e.JournalIndex == budget-4 {
			if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
				panic(err)
			}
		}
		index := map[string]uint64{"after_signal": budget - 3, "after_completion": budget - 2, "after_failed": budget - 1}[cut]
		kind := map[string]journal.Kind{"after_signal": journal.SignalConsumed, "after_completion": journal.StepCompleted, "after_failed": journal.Failed}[cut]
		if index != 0 && e.JournalIndex == index && e.JournalKind == kind {
			raw, err := json.Marshal(e)
			if err != nil {
				panic(err)
			}
			if err := os.WriteFile(path+".cut", raw, 0600); err != nil {
				panic(err)
			}
			select {} // The controller sends a real SIGKILL; no graceful release.
		}
	}
	w, err := New(context.Background(), js, "limit-killed", handlers, WithContinuations(limitKillType, stages), WithOperationObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if budget != journal.MaxEntries {
		w.maxEntries = budget
	} // Test-only lower cap.
	if w.maxEntries != budget {
		t.Fatal("production/default fixture limit changed")
	}
	if err := w.RunPartition(context.Background(), identity.Partition(limitKillType, limitKillID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestContinuationLimitAfterWorkerKillAndClusterRestart(t *testing.T) {
	if os.Getenv("WF_CONTINUATION_LIMIT_KILL") != "1" {
		t.Skip("set WF_CONTINUATION_LIMIT_KILL=1 for combined near-limit process/server faults")
	}
	budget := uint64(16)
	if text := os.Getenv("WF_CONTINUATION_LIMIT_KILL_BUDGET"); text != "" {
		var err error
		budget, err = strconv.ParseUint(text, 10, 64)
		if err != nil || (budget != 16 && budget != 20 && budget != journal.MaxEntries) {
			t.Fatal("budget must be16,20 or100000")
		}
	}
	for _, cut := range []string{"after_signal", "after_completion", "after_failed"} {
		t.Run(cut, func(t *testing.T) { runContinuationLimitKill(t, budget, cut) })
	}
}

func runContinuationLimitKill(t *testing.T, budget uint64, cut string) {
	t.Helper()
	root := t.TempDir()
	if base := os.Getenv("LIMIT_KILL_ARTIFACT_ROOT"); base != "" {
		root = filepath.Join(base, cut)
		if err := os.MkdirAll(root, 0755); err != nil {
			t.Fatal(err)
		}
	}
	cluster, err := testcluster.Start(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	duration := 90 * time.Second
	if budget == journal.MaxEntries {
		duration = 25 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	clients := func() []jetstream.JetStream {
		var all []jetstream.JetStream
		for _, nc := range cluster.Clients {
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, js)
		}
		return all
	}
	all := clients()
	save := func(name string, value any) {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ensure := func() {
		for ctx.Err() == nil {
			attempt, done := context.WithTimeout(ctx, 2*time.Second)
			err := provision.Ensure(attempt, all[0], 3)
			done()
			if err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("provision/restart readiness failed", ctx.Err())
	}
	ensure()
	c := client.New(all[0])
	handle, err := c.Start(ctx, limitKillType, limitKillID, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "child")
	child := exec.Command(os.Args[0], "-test.run=^TestContinuationLimitKillChild$", "-test.timeout="+(duration+time.Minute).String())
	child.Env = append(os.Environ(), "WF_LIMIT_KILL_CHILD=1", "WF_LIMIT_KILL_URL="+cluster.Clients[1].ConnectedUrl(), "WF_LIMIT_KILL_PATH="+path, "WF_LIMIT_KILL_CUT="+cut, "WF_LIMIT_KILL_BUDGET="+strconv.FormatUint(budget, 10))
	log, err := os.Create(path + ".log")
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child.Stdout, child.Stderr = log, log
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
	}()
	waitFile := func(name string) {
		for {
			if _, err := os.Stat(name); err == nil {
				return
			}
			select {
			case err := <-exited:
				waited = true
				t.Fatalf("worker exited before cut: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	waitFile(path + ".ready")
	if _, err := c.Signal(ctx, limitKillType, limitKillID, "gate", []byte(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	waitFile(path + ".cut")
	store := journal.New(all[2])
	prefix, _, err := store.Read(ctx, limitKillType, limitKillID)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]uint64{"after_signal": budget - 2, "after_completion": budget - 1, "after_failed": budget}[cut]
	if uint64(len(prefix)) != expected {
		t.Fatalf("cut prefix length=%d want=%d", len(prefix), expected)
	}
	expectedKind := map[string]journal.Kind{"after_signal": journal.SignalConsumed, "after_completion": journal.StepCompleted, "after_failed": journal.Failed}[cut]
	if prefix[len(prefix)-1].Kind != expectedKind || prefix[len(prefix)-1].Index != expected-1 {
		t.Fatal("wrong acknowledged cut")
	}
	save("prefix", prefix)
	save("invocation", handle)
	view, err := store.ReadCheckpoint(ctx, limitKillType, limitKillID, handle.InvSeq)
	if err != nil || view == nil || view.Anchor.Index != budget-7 || view.Snapshot.Runtime.StepPosition != budget-8 {
		t.Fatalf("cut checkpoint=%+v err=%v", view, err)
	}
	beforeLog, err := os.ReadFile(path + ".handlers")
	if err != nil || strings.Contains(string(beforeLog), "effect\n") {
		t.Fatalf("forbidden effect before kill: %s %v", beforeLog, err)
	}
	leaseKV, err := all[2].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	status, err := leaseKV.Status(ctx)
	if err != nil || status.TTL() != provision.LeaseTTL {
		t.Fatalf("lease TTL mismatch: %v", err)
	}
	entry, err := leaseKV.Get(ctx, identity.Key(limitKillType, limitKillID))
	if err != nil {
		t.Fatal(err)
	}
	var held lease.Value
	if err := json.Unmarshal(entry.Value(), &held); err != nil || held.Worker != "limit-killed" {
		t.Fatalf("cut lease=%+v err=%v", held, err)
	}
	save("cut-lease", held)
	save("checkpoint-runtime", view.Snapshot.Runtime)
	stateBefore, err := all[2].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateBefore.Get(ctx, identity.Key(limitKillType, limitKillID)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal("outcome published before acknowledged cut", err)
	}
	killedAt := time.Now()
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := <-exited
	waited = true
	signal, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !signal.Signaled() || signal.Signal() != syscall.SIGKILL {
		t.Fatalf("not a real SIGKILL: %v %v", child.ProcessState, waitErr)
	}
	l := lease.NewWithKeyValue(leaseKV)
	if _, err := l.Acquire(ctx, limitKillType, limitKillID, "premature-successor"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("kill did not retain active lease: %v", err)
	}
	for i := 0; i < 3; i++ {
		cluster.KillNode(i)
	}
	for i := 0; i < 3; i++ {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatal(err)
		}
	}
	all = clients()
	ensure()
	c = client.New(all[0])
	store = journal.New(all[0])
	guard := &continuationLimitFramePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	handlers, stages := limitKillHandlers(path+".handlers", budget, true)
	replacement, err := New(ctx, all[2], "limit-successor", handlers, WithContinuations(limitKillType, stages), WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if budget != journal.MaxEntries {
		replacement.maxEntries = budget
	}
	if replacement.maxEntries != budget {
		t.Fatal("replacement budget changed")
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	joined := false
	go func() {
		done <- replacement.RunPartition(runCtx, identity.Partition(limitKillType, limitKillID, provision.Partitions))
	}()
	defer func() {
		stop()
		if !joined {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}()
	awaitCtx, cancelAwait := context.WithCancel(ctx)
	defer cancelAwait()
	effectObserved := make(chan struct{}, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-awaitCtx.Done():
				return
			case <-ticker.C:
				data, err := os.ReadFile(path + ".handlers")
				if err == nil && bytes.Contains(data, []byte("effect\n")) {
					effectObserved <- struct{}{}
					cancelAwait()
					return
				}
			}
		}
	}()
	_, terminalErr := c.Await(awaitCtx, limitKillType, limitKillID)
	cancelAwait()
	<-watchDone
	forbiddenEffect := false
	select {
	case <-effectObserved:
		forbiddenEffect = true
	default:
	}
	recovery := time.Since(killedAt)
	stop()
	// Join before inspecting the non-concurrent frame-port counters.
	workerErr := <-done
	joined = true
	if workerErr != nil {
		t.Fatal(workerErr)
	}
	records, _, err := store.Read(ctx, limitKillType, limitKillID)
	if err != nil {
		t.Fatal(err)
	}
	save("journal", records)
	if forbiddenEffect {
		t.Fatal("forbidden effect ran after takeover")
	}
	if terminalErr == nil || terminalErr.Error() != journal.ErrTooLong.Error() {
		t.Fatalf("limit outcome changed: %v", terminalErr)
	}
	if recovery >= 30*time.Second {
		t.Fatalf("combined limit recovery=%s, want <30s", recovery)
	}
	if guard.archives != 0 || guard.frames == 0 {
		t.Fatalf("archive/frame reads=%d/%d", guard.archives, guard.frames)
	}
	if err != nil || uint64(len(records)) != budget || records[budget-1].Kind != journal.Failed || records[budget-1].Index != budget-1 {
		t.Fatalf("global budget escaped: entries=%d budget=%d err=%v", len(records), budget, err)
	}
	if !reflect.DeepEqual(prefix, records[:len(prefix)]) {
		t.Fatal("acknowledged cut prefix changed")
	}
	if cut != "after_failed" && records[budget-1].Epoch <= held.Epoch {
		t.Fatal("reserved failure did not use a higher epoch")
	}
	finalView, err := store.ReadCheckpoint(ctx, limitKillType, limitKillID, handle.InvSeq)
	if err != nil || finalView == nil || !bytes.Equal(view.Frame, finalView.Frame) {
		t.Fatal("checkpoint frame changed", err)
	}
	finalLog, err := os.ReadFile(path + ".handlers")
	if err != nil || strings.Contains(string(finalLog), "effect\n") || strings.Count(string(finalLog), "initial\n") != strings.Count(string(beforeLog), "initial\n") || strings.Count(string(finalLog), "middle\n") != strings.Count(string(beforeLog), "middle\n") {
		t.Fatalf("prefix/effect reexecution: before=%s after=%s err=%v", beforeLog, finalLog, err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := state.Get(ctx, identity.Key(limitKillType, limitKillID))
	if err != nil || !bytes.Equal(terminal.Value(), records[budget-1].Payload) {
		t.Fatal("terminal state differs from journal", err)
	}
	for _, js := range all {
		if _, err := client.New(js).Await(ctx, limitKillType, limitKillID); err == nil || err.Error() != journal.ErrTooLong.Error() {
			t.Fatal("peer result changed", err)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("raw integrity=%+v err=%v", report, err)
	}
	save("audit", report)
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	replayObjects := map[string][]byte{}
	for _, record := range records {
		if record.Kind != journal.StepCompleted {
			continue
		}
		var done struct {
			Ref string `json:"result_ref"`
		}
		if err := json.Unmarshal(record.Payload, &done); err != nil {
			t.Fatal(err)
		}
		if done.Ref != "" {
			raw, err := objects.GetBytes(ctx, done.Ref)
			if err != nil {
				t.Fatal(err)
			}
			replayObjects[done.Ref] = raw
		}
	}
	replayHandlers, replayStages := limitKillHandlers(path+".offline", budget, false)
	var observation wf.ReplayObservation
	raw, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	_, replayErr := wf.ReplayWithContinuations(raw, func(c *wf.Context) (json.RawMessage, error) {
		return replayHandlers[limitKillType](c, json.RawMessage(`null`))
	}, map[string]wf.ReplayContinuation[json.RawMessage]{
		"middle_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
			return replayStages["middle_v1"](c, json.RawMessage(`null`), locals)
		},
		"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
			return replayStages["finish_v1"](c, json.RawMessage(`null`), locals)
		},
	}, wf.ReplayOptions{Type: limitKillType, ID: limitKillID, InvSeq: handle.InvSeq, Objects: replayObjects, Observation: &observation})
	offlineLog, err := os.ReadFile(path + ".offline")
	if err != nil || !errors.Is(replayErr, wf.ErrReplayPendingStep) || observation.Continuations != 2 || observation.PlayedSteps != observation.RecordedSteps || strings.Contains(string(offlineLog), "effect\n") {
		t.Fatalf("offline limit audit=%+v replay=%v effects=%s err=%v", observation, replayErr, offlineLog, err)
	}
	save("offline", observation)
	t.Logf("CONTINUATION_LIMIT_KILL cut=%s budget=%d entries=%d preserved_prefix=%d anchor=%d sdk_offset=%d worker_sigkill=true all_server_restart=true lease_ttl=%s initial_lease_held=true recovery=%s effects=0 archive_reads=%d frame_reads=%d production_cap=%t", cut, budget, len(records), len(prefix), view.Anchor.Index, view.Snapshot.Runtime.StepPosition, provision.LeaseTTL, recovery, guard.archives, guard.frames, budget == journal.MaxEntries)
}
