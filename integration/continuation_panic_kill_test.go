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
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const continuationPanicKillType = "checkpoint-panic-kill"
const continuationPanicKillID = "budget"

func continuationPanicKillHandlers(path string) (map[string]worker.Handler, map[string]worker.ContinuationHandler) {
	var initialCalls, middleCalls int
	initial := map[string]worker.Handler{continuationPanicKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := continuationKillLog(path, "initial"); err != nil {
			return nil, err
		}
		initialCalls++
		if initialCalls == 1 {
			panic("initial poison")
		}
		if err := c.SetState("value", 10); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "middle_v1", 10)
	}}
	stages := map[string]worker.ContinuationHandler{
		"middle_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			if err := continuationKillLog(path, "middle"); err != nil {
				return nil, err
			}
			middleCalls++
			var value int
			found, err := c.GetState("value", &value)
			if err != nil {
				return nil, err
			}
			if !found || value != 10 || string(locals) != "10" {
				return nil, fmt.Errorf("bad middle state")
			}
			if middleCalls == 1 {
				panic("middle poison")
			}
			if err := c.SetState("value", 20); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish_v1", 20)
		},
		"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
			if err := continuationKillLog(path, "finish"); err != nil {
				return nil, err
			}
			var value int
			found, err := c.GetState("value", &value)
			if err != nil {
				return nil, err
			}
			if !found || value != 20 || string(locals) != "20" {
				return nil, fmt.Errorf("bad finish state")
			}
			panic("final poison")
		},
	}
	return initial, stages
}

func TestContinuationPanicKillChild(t *testing.T) {
	if os.Getenv("WF_CONTINUATION_PANIC_KILL_CHILD") != "1" {
		t.Skip("panic worker child helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_CONTINUATION_PANIC_KILL_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	initial, stages := continuationPanicKillHandlers(os.Getenv("WF_CONTINUATION_PANIC_KILL_LOG"))
	cut := os.Getenv("WF_CONTINUATION_PANIC_KILL_CUT")
	var attempts atomic.Int64
	// Deliberately hold the execution goroutine at an acknowledged journal cut.
	// Only this child-process fixture blocks the normally quick observer.
	observer := func(event worker.OperationEvent) {
		if event.Operation != "journal_append" || event.Error != "" {
			return
		}
		stop := false
		if event.JournalKind == journal.Attempt {
			stop = attempts.Add(1) == 3 && cut == "after_attempt"
		}
		if event.JournalKind == journal.Failed && cut == "after_failed" {
			stop = true
		}
		if stop {
			if err := os.WriteFile(os.Getenv("WF_CONTINUATION_PANIC_KILL_MARKER"), []byte(cut), 0600); err != nil {
				panic(err)
			}
			select {}
		}
	}
	w, err := worker.New(context.Background(), js, "panic-killed", initial, worker.WithContinuations(continuationPanicKillType, stages), worker.WithMaxPanicAttempts(3), worker.WithOperationObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.RunPartition(context.Background(), identity.Partition(continuationPanicKillType, continuationPanicKillID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestContinuationPanicSIGKILLTerminalCuts(t *testing.T) {
	for _, cut := range []string{"after_attempt", "after_failed"} {
		t.Run(cut, func(t *testing.T) {
			all, cluster := setup(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			c := client.New(all[0])
			handle, err := c.Start(ctx, continuationPanicKillType, continuationPanicKillID, []byte(`null`))
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			marker := filepath.Join(root, "cut")
			handlerLog := filepath.Join(root, "handlers")
			childLog := filepath.Join(root, "child.log")
			output, err := os.Create(childLog)
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(executable, "-test.run=^TestContinuationPanicKillChild$")
			child.Env = append(os.Environ(), "WF_CONTINUATION_PANIC_KILL_CHILD=1", "WF_CONTINUATION_PANIC_KILL_URL="+cluster.Servers[0].ClientURL(), "WF_CONTINUATION_PANIC_KILL_CUT="+cut, "WF_CONTINUATION_PANIC_KILL_MARKER="+marker, "WF_CONTINUATION_PANIC_KILL_LOG="+handlerLog)
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
					data, _ := os.ReadFile(childLog)
					t.Logf("child log: %s", data)
				}
			}()
			for {
				if data, err := os.ReadFile(marker); err == nil && string(data) == cut {
					break
				}
				select {
				case err := <-exited:
					waited = true
					t.Fatalf("child exited before cut: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				default:
				}
				time.Sleep(20 * time.Millisecond)
			}
			store := journal.New(all[1])
			before, _, err := store.Read(ctx, continuationPanicKillType, continuationPanicKillID)
			if err != nil {
				t.Fatal(err)
			}
			if len(before) == 0 {
				t.Fatal("empty cut journal")
			}
			attempts := 0
			for _, r := range before {
				if r.Kind == journal.Attempt {
					attempts++
					a, err := journal.DecodeAttempt(r.Payload)
					if err != nil || a.Count != attempts {
						t.Fatalf("attempt=%+v want=%d err=%v", a, attempts, err)
					}
				}
			}
			last := before[len(before)-1]
			expectedKind := journal.Attempt
			if cut == "after_failed" {
				expectedKind = journal.Failed
			}
			if attempts != 3 || last.Kind != expectedKind {
				t.Fatalf("cut attempts=%d kind=%s", attempts, last.Kind)
			}
			view, err := store.ReadCheckpoint(ctx, continuationPanicKillType, continuationPanicKillID, handle.InvSeq)
			if err != nil || view == nil {
				t.Fatalf("frame=%+v err=%v", view, err)
			}
			runtime := view.Snapshot.Runtime
			_, info, err := wf.NewCheckpointContext(ctx, nil, nil, view.Frame, wf.CheckpointLocation{Type: continuationPanicKillType, ID: continuationPanicKillID, InvSeq: handle.InvSeq, Index: runtime.Index, Epoch: runtime.Epoch, Hash: runtime.SHA256})
			if err != nil || info.Stage != "finish_v1" || info.PanicAttempts != 2 {
				t.Fatalf("baseline=%+v err=%v", info, err)
			}
			state, err := all[1].KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.Get(ctx, identity.Key(continuationPanicKillType, continuationPanicKillID)); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("terminal state published before cut: %v", err)
			}
			beforeLog, err := os.ReadFile(handlerLog)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(beforeLog), "initial\n") != 2 || strings.Count(string(beforeLog), "middle\n") != 2 || strings.Count(string(beforeLog), "finish\n") != 1 {
				t.Fatalf("cut calls: %s", beforeLog)
			}
			killedAt := time.Now()
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			waitErr := <-exited
			waited = true
			status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
			if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("not SIGKILL: state=%v err=%v", child.ProcessState, waitErr)
			}
			unexpected := func(c *wf.Context) (json.RawMessage, error) {
				if err := continuationKillLog(handlerLog, "unexpected"); err != nil {
					return nil, err
				}
				panic("budget recovery reran handler")
			}
			handlers := map[string]worker.Handler{continuationPanicKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) { return unexpected(c) }}
			stages := map[string]worker.ContinuationHandler{}
			for _, name := range []string{"middle_v1", "finish_v1"} {
				stages[name] = func(c *wf.Context, _, _ json.RawMessage) (json.RawMessage, error) { return unexpected(c) }
			}
			guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
			successor, err := worker.New(ctx, all[2], "panic-successor", handlers, worker.WithContinuations(continuationPanicKillType, stages), worker.WithMaxPanicAttempts(3), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
			if err != nil {
				t.Fatal(err)
			}
			defer successor.Close()
			runCtx, stopRun := context.WithCancel(ctx)
			defer stopRun()
			done := make(chan error, 1)
			go func() {
				done <- successor.RunPartition(runCtx, identity.Partition(continuationPanicKillType, continuationPanicKillID, provision.Partitions))
			}()
			// Recovery uses the original unacknowledged run; no synthetic wakeup.
			_, terminalErr := c.Await(ctx, continuationPanicKillType, continuationPanicKillID)
			latency := time.Since(killedAt)
			stopRun()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if terminalErr == nil || terminalErr.Error() != "workflow panic: final poison" {
				t.Fatalf("wrong recovered failure: %v", terminalErr)
			}
			if latency >= 30*time.Second {
				t.Fatalf("recovery exceeded 30s: %v", latency)
			}
			afterLog, err := os.ReadFile(handlerLog)
			if err != nil || !bytes.Equal(beforeLog, afterLog) {
				t.Fatalf("handler reran: before=%s after=%s err=%v", beforeLog, afterLog, err)
			}
			if guard.archives != 0 || guard.frames == 0 {
				t.Fatalf("archive=%d frame=%d", guard.archives, guard.frames)
			}
			records, _, err := store.Read(ctx, continuationPanicKillType, continuationPanicKillID)
			if err != nil {
				t.Fatal(err)
			}
			expectedLen := len(before)
			if cut == "after_attempt" {
				expectedLen++
			}
			if len(records) != expectedLen || records[len(records)-1].Kind != journal.Failed {
				t.Fatalf("terminal records=%d want=%d", len(records), expectedLen)
			}
			for i := range before {
				a, _ := json.Marshal(before[i])
				b, _ := json.Marshal(records[i])
				if !bytes.Equal(a, b) {
					t.Fatalf("cut prefix changed at %d", i)
				}
			}
			if cut == "after_attempt" && records[len(records)-1].Epoch <= last.Epoch {
				t.Fatal("failure not committed under higher epoch")
			}
			outcome, err := state.Get(ctx, identity.Key(continuationPanicKillType, continuationPanicKillID))
			if err != nil || !bytes.Equal(outcome.Value(), records[len(records)-1].Payload) {
				t.Fatalf("outcome does not match journal: err=%v", err)
			}
			for _, peer := range all {
				_, err := client.New(peer).Await(ctx, continuationPanicKillType, continuationPanicKillID)
				if err == nil || err.Error() != terminalErr.Error() {
					t.Fatalf("peer failure changed: %v", err)
				}
			}
			report, err := integrity.Check(ctx, all[1])
			if err != nil || report.Invocations != 1 || report.Terminal != 1 {
				t.Fatalf("integrity=%+v err=%v", report, err)
			}
			t.Logf("panic SIGKILL cut=%s attempts=%d baseline=%d recovery=%v entries=%d cut_epoch=%d final_epoch=%d archive_reads=%d frame_reads=%d", cut, attempts, info.PanicAttempts, latency, len(records), last.Epoch, records[len(records)-1].Epoch, guard.archives, guard.frames)
		})
	}
}
