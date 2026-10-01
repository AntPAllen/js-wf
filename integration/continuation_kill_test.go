//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const continuationKillType = "checkpoint-kill"
const continuationKillID = "boundary"

type continuationKillSnapshotPort struct {
	journal.SnapshotWritePort
	cut, marker string
}

func (p *continuationKillSnapshotPort) stopAt(cut string) {
	if p.cut != cut {
		return
	}
	if err := os.WriteFile(p.marker, []byte(cut), 0600); err != nil {
		panic(err)
	}
	select {}
}
func (p *continuationKillSnapshotPort) CreateManifest(ctx context.Context, key string, data []byte) error {
	p.stopAt("before_manifest")
	if err := p.SnapshotWritePort.CreateManifest(ctx, key, data); err != nil {
		return err
	}
	p.stopAt("after_manifest")
	return nil
}
func (p *continuationKillSnapshotPort) PurgeJournal(ctx context.Context, subject string, before uint64) error {
	if err := p.SnapshotWritePort.PurgeJournal(ctx, subject, before); err != nil {
		return err
	}
	p.stopAt("after_journal_purge")
	return nil
}
func (p *continuationKillSnapshotPort) PurgeSignals(ctx context.Context, subject string, before uint64) error {
	if err := p.SnapshotWritePort.PurgeSignals(ctx, subject, before); err != nil {
		return err
	}
	p.stopAt("after_signal_purge")
	return nil
}

type continuationKillResultPort struct {
	worker.ResultBlobPort
	stop *continuationKillSnapshotPort
}

func (p *continuationKillResultPort) PutBytes(ctx context.Context, name string, data []byte) error {
	var frame struct {
		Stage string `json:"stage"`
	}
	isFrame := json.Unmarshal(data, &frame) == nil && frame.Stage == "finish_v1"
	if isFrame && (p.stop.cut == "before_frame" || p.stop.cut == "after_frame") {
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
		p.stop.stopAt("before_frame")
	}
	if err := p.ResultBlobPort.PutBytes(ctx, name, data); err != nil {
		return err
	}
	if isFrame {
		p.stop.stopAt("after_frame")
	}
	return nil
}

func continuationKillLog(path, line string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := fmt.Fprintln(file, line); err != nil {
		return err
	}
	return file.Sync()
}
func continuationKillHandlers(path string) (map[string]worker.Handler, map[string]worker.ContinuationHandler) {
	initial := map[string]worker.Handler{continuationKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := continuationKillLog(path, "initial"); err != nil {
			return nil, err
		}
		if err := c.SetState("total", 23); err != nil {
			return nil, err
		}
		if _, err := wf.RunOnce(c, "prefix", 23, func(context.Context, string) (int, error) { return 46, continuationKillLog(path, "prefix_effect") }); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "finish_v1", 45)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
		if string(input) != "23" || string(locals) != "45" {
			return nil, fmt.Errorf("wrong stage data")
		}
		value, err := wf.AwaitSignal(c, "buffered")
		if err != nil {
			return nil, err
		}
		if string(value) != "7" {
			return nil, fmt.Errorf("buffered signal %q", value)
		}
		var total int
		if found, err := c.GetState("total", &total); err != nil || !found || total != 23 {
			return nil, fmt.Errorf("state=%d err=%v", total, err)
		}
		if _, err := wf.RunOnce(c, "suffix", 23, func(context.Context, string) (int, error) { return 46, continuationKillLog(path, "suffix_effect") }); err != nil {
			return nil, err
		}
		return json.RawMessage(`46`), nil
	}}
	return initial, stages
}

func TestContinuationKillChild(t *testing.T) {
	if os.Getenv("WF_CONTINUATION_KILL_CHILD") != "1" {
		t.Skip("continuation worker child helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_CONTINUATION_KILL_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	port := &continuationKillSnapshotPort{SnapshotWritePort: journal.NewSnapshotPort(js), cut: os.Getenv("WF_CONTINUATION_KILL_CUT"), marker: os.Getenv("WF_CONTINUATION_KILL_MARKER")}
	initial, stages := continuationKillHandlers(os.Getenv("WF_CONTINUATION_KILL_LOG"))
	w, err := worker.New(context.Background(), js, "checkpoint-killed", initial, worker.WithContinuations(continuationKillType, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(js, port)), worker.WithResultBlobPort(&continuationKillResultPort{ResultBlobPort: worker.NewResultBlobPort(js), stop: port}))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.RunPartition(context.Background(), identity.Partition(continuationKillType, continuationKillID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestContinuationWorkerSIGKILLPublicationCuts(t *testing.T) {
	testContinuationWorkerSIGKILLCuts(t, []string{"before_manifest", "after_manifest", "after_journal_purge", "after_signal_purge"})
}

func TestContinuationWorkerSIGKILLFrameCuts(t *testing.T) {
	testContinuationWorkerSIGKILLCuts(t, []string{"before_frame", "after_frame"})
}

func testContinuationWorkerSIGKILLCuts(t *testing.T, cuts []string) {
	t.Helper()
	for _, cut := range cuts {
		t.Run(cut, func(t *testing.T) {
			all, cluster := setup(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			c := client.New(all[0])
			handle, err := c.Start(ctx, continuationKillType, continuationKillID, []byte(`23`))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Signal(ctx, continuationKillType, continuationKillID, "buffered", []byte(`7`), "buffered"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			marker := filepath.Join(root, "cut")
			effectLog := filepath.Join(root, "effects")
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
			child := exec.Command(executable, "-test.run=^TestContinuationKillChild$")
			child.Env = append(os.Environ(), "WF_CONTINUATION_KILL_CHILD=1", "WF_CONTINUATION_KILL_URL="+cluster.Servers[0].ClientURL(), "WF_CONTINUATION_KILL_CUT="+cut, "WF_CONTINUATION_KILL_MARKER="+marker, "WF_CONTINUATION_KILL_LOG="+effectLog)
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
			before, _, err := store.Read(ctx, continuationKillType, continuationKillID)
			if err != nil {
				t.Fatal(err)
			}
			pendingFrame := cut == "before_frame" || cut == "after_frame"
			unpublished := pendingFrame || cut == "before_manifest"
			wantEntries, wantKind := 8, journal.StepCompleted
			if pendingFrame {
				wantEntries, wantKind = 7, journal.StepRequested
			}
			if len(before) != wantEntries || before[len(before)-1].Kind != wantKind {
				t.Fatalf("cut journal entries=%d want=%d tail=%v", len(before), wantEntries, before)
			}
			lastBefore := before[len(before)-1]
			firstEpoch := lastBefore.Epoch
			var abandonedFrame struct {
				Name string
				Data []byte
			}
			if pendingFrame {
				var declaration struct{ Kind, Name string }
				if err := json.Unmarshal(lastBefore.Payload, &declaration); err != nil || declaration.Kind != "checkpoint" || declaration.Name != "finish_v1" {
					t.Fatalf("pending checkpoint=%s err=%v", lastBefore.Payload, err)
				}
				markerBytes, err := os.ReadFile(marker + ".frame")
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(markerBytes, &abandonedFrame); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(abandonedFrame.Data)
				hash := hex.EncodeToString(digest[:])
				if abandonedFrame.Name != "step-result-"+hash {
					t.Fatal("frame object name not bound to bytes")
				}
				_, frameInfo, err := wf.NewCheckpointContext(ctx, nil, nil, abandonedFrame.Data, wf.CheckpointLocation{Type: continuationKillType, ID: continuationKillID, InvSeq: handle.InvSeq, Index: lastBefore.Index + 1, Epoch: lastBefore.Epoch, Hash: hash})
				if err != nil || frameInfo.Stage != "finish_v1" || frameInfo.StepPosition != 6 {
					t.Fatalf("prospective frame=%+v err=%v", frameInfo, err)
				}
				objects, err := all[1].ObjectStore(ctx, "WF_BLOB")
				if err != nil {
					t.Fatal(err)
				}
				stored, err := objects.GetBytes(ctx, abandonedFrame.Name)
				if cut == "before_frame" {
					if !errors.Is(err, jetstream.ErrObjectNotFound) {
						t.Fatalf("frame exists before put: %v", err)
					}
				} else if err != nil || !bytes.Equal(stored, abandonedFrame.Data) {
					t.Fatalf("acknowledged frame missing or changed: %v", err)
				}
			}
			view, err := store.ReadCheckpoint(ctx, continuationKillType, continuationKillID, handle.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			if (view == nil) != unpublished {
				t.Fatalf("unexpected manifest at %s: %+v", cut, view)
			}
			for _, name := range []string{"WF_JRN", "WF_SIG"} {
				stream, err := all[1].Stream(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				info, err := stream.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				want := uint64(1)
				if name == "WF_JRN" && (unpublished || cut == "after_manifest") {
					want = uint64(wantEntries)
				}
				if name == "WF_SIG" && cut == "after_signal_purge" {
					want = 0
				}
				if info.State.Msgs != want {
					t.Fatalf("cut=%s %s messages=%d want=%d", cut, name, info.State.Msgs, want)
				}
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
			repair, err := reconcile.NewSuspendedScan(all[1]).Scan(ctx, handle.InvSeq, 1, false)
			wantRepairs := 1
			if pendingFrame {
				wantRepairs = 0
			}
			if err != nil || repair.Reenqueued != wantRepairs || len(repair.Candidates) != wantRepairs {
				t.Fatalf("checkpoint repair=%+v err=%v", repair, err)
			}
			if !pendingFrame && (repair.Candidates[0].Reason != "continuation" || repair.Candidates[0].JournalSeq != lastBefore.Sequence) {
				t.Fatalf("checkpoint repair=%+v err=%v", repair, err)
			}
			initial, stages := continuationKillHandlers(effectLog)
			guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
			options := []worker.Option{worker.WithContinuations(continuationKillType, stages)}
			if !unpublished {
				options = append(options, worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
			}
			successor, err := worker.New(ctx, all[2], "checkpoint-successor", initial, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer successor.Close()
			runCtx, stopRun := context.WithCancel(ctx)
			defer stopRun()
			done := make(chan error, 1)
			go func() {
				done <- successor.RunPartition(runCtx, identity.Partition(continuationKillType, continuationKillID, provision.Partitions))
			}()
			result, err := c.Await(ctx, continuationKillType, continuationKillID)
			latency := time.Since(killedAt)
			stopRun()
			if runErr := <-done; runErr != nil {
				t.Fatal(runErr)
			}
			if err != nil || string(result) != "46" {
				t.Fatalf("result=%s err=%v", result, err)
			}
			if latency >= 30*time.Second {
				t.Fatalf("kill recovery exceeded 30s: %v", latency)
			}
			data, err := os.ReadFile(effectLog)
			if err != nil {
				t.Fatal(err)
			}
			expectedInitial := 1
			if unpublished {
				expectedInitial = 2
			}
			if strings.Count(string(data), "initial\n") != expectedInitial || strings.Count(string(data), "prefix_effect\n") != 1 || strings.Count(string(data), "suffix_effect\n") != 1 {
				t.Fatalf("unexpected handler/effect counts: %s", data)
			}
			if !unpublished && (guard.archives != 0 || guard.frames == 0) {
				t.Fatalf("archive=%d frame=%d", guard.archives, guard.frames)
			}
			records, _, err := store.Read(ctx, continuationKillType, continuationKillID)
			if err != nil {
				t.Fatal(err)
			}
			if records[len(records)-1].Kind != journal.Completed || records[len(records)-1].Epoch <= firstEpoch {
				t.Fatal("successor did not complete under a higher epoch")
			}
			beforeBytes, _ := json.Marshal(before)
			restoredPrefix, _ := json.Marshal(records[:len(before)])
			if !bytes.Equal(beforeBytes, restoredPrefix) {
				t.Fatal("successor changed the confirmed pre-kill journal prefix")
			}
			if cut == "after_frame" {
				current, err := store.ReadCheckpoint(ctx, continuationKillType, continuationKillID, handle.InvSeq)
				if err != nil || current == nil || current.Snapshot.Runtime.Object == abandonedFrame.Name {
					t.Fatalf("replacement did not publish its own higher-epoch frame: view=%+v err=%v", current, err)
				}
				objects, err := all[1].ObjectStore(ctx, "WF_BLOB")
				if err != nil {
					t.Fatal(err)
				}
				old, err := objects.GetBytes(ctx, abandonedFrame.Name)
				if err != nil || !bytes.Equal(old, abandonedFrame.Data) {
					t.Fatalf("unreferenced confirmed object changed: %v", err)
				}
				// Child is dead and replacement loop has joined: all writers are quiescent.
				sweep, err := retention.SweepBlobsQuiescent(ctx, all[1], 0, time.Now().Add(time.Minute))
				if err != nil || sweep.Deleted != 1 {
					t.Fatalf("orphan collection=%+v err=%v", sweep, err)
				}
				if _, err := objects.GetBytes(ctx, abandonedFrame.Name); !errors.Is(err, jetstream.ErrObjectNotFound) {
					t.Fatalf("orphan still exists: %v", err)
				}
				kept, err := store.ReadCheckpoint(ctx, continuationKillType, continuationKillID, handle.InvSeq)
				if err != nil || kept == nil || !bytes.Equal(kept.Frame, current.Frame) {
					t.Fatalf("collection damaged live checkpoint: %v", err)
				}
			}
			report, err := integrity.Check(ctx, all[1])
			if err != nil || report.Invocations != 1 || report.Terminal != 1 {
				t.Fatalf("integrity=%+v err=%v", report, err)
			}
			for _, peer := range all {
				value, err := client.New(peer).Await(ctx, continuationKillType, continuationKillID)
				if err != nil || string(value) != "46" {
					t.Fatalf("peer result=%s err=%v", value, err)
				}
			}
			settled, err := reconcile.NewSuspendedScan(all[1]).Scan(ctx, handle.InvSeq, 1, false)
			if err != nil || settled.Reenqueued != 0 {
				t.Fatalf("terminal checkpoint repaired again: %+v err=%v", settled, err)
			}
			objects, err := all[1].ObjectStore(ctx, "WF_BLOB")
			if err != nil {
				t.Fatal(err)
			}
			replayObjects := make(map[string][]byte)
			for _, record := range records {
				if record.Kind != journal.StepCompleted {
					continue
				}
				var reference struct {
					Name string `json:"result_ref"`
				}
				if json.Unmarshal(record.Payload, &reference) != nil {
					t.Fatal("invalid completion")
				}
				if reference.Name != "" {
					data, err := objects.GetBytes(ctx, reference.Name)
					if err != nil {
						t.Fatal(err)
					}
					replayObjects[reference.Name] = data
				}
			}
			offlineLog := filepath.Join(root, "offline.log")
			offlineInitial, offlineStages := continuationKillHandlers(offlineLog)
			journalBytes, err := json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			var observation wf.ReplayObservation
			offline, err := wf.ReplayWithContinuations(journalBytes, func(c *wf.Context) (json.RawMessage, error) {
				return offlineInitial[continuationKillType](c, json.RawMessage(`23`))
			}, map[string]wf.ReplayContinuation[json.RawMessage]{"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
				return offlineStages["finish_v1"](c, json.RawMessage(`23`), locals)
			}}, wf.ReplayOptions{Type: continuationKillType, ID: continuationKillID, InvSeq: handle.InvSeq, Objects: replayObjects, Observation: &observation})
			if err != nil || string(offline) != "46" || observation.Continuations != 1 || observation.PlayedSteps != observation.RecordedSteps {
				t.Fatalf("offline result=%s observation=%+v err=%v", offline, observation, err)
			}
			offlineData, err := os.ReadFile(offlineLog)
			if err != nil || string(offlineData) != "initial\n" {
				t.Fatalf("offline effects ran: %q err=%v", offlineData, err)
			}
			t.Logf("SIGKILL cut=%s repaired=%d recovery=%v entries=%d first_epoch=%d final_epoch=%d archive_reads=%d frame_reads=%d offline_steps=%d", cut, repair.Reenqueued, latency, len(records), firstEpoch, records[len(records)-1].Epoch, guard.archives, guard.frames, observation.PlayedSteps)
		})
	}
}
