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
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	"js-wf/retention"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

const retirementKillType = "checkpoint-retire-kill"
const retirementKillID = "reused"

func retirementKillHandlers(log string) (map[string]worker.Handler, map[string]worker.ContinuationHandler) {
	payload := strings.Repeat("x", wf.MaxInlineResult)
	initial := map[string]worker.Handler{retirementKillType: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if err := continuationKillLog(log, "initial:"+string(input)); err != nil {
			return nil, err
		}
		if err := c.SetState("value", input); err != nil {
			return nil, err
		}
		if _, err := wf.Run(c, "shared", 0, func(context.Context) (string, error) {
			return payload, continuationKillLog(log, "effect:"+string(input))
		}); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "finish_v1", input)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
		var value int
		found, err := c.GetState("value", &value)
		if err != nil || !found || string(input) != string(locals) || fmt.Sprint(value) != string(input) {
			return nil, fmt.Errorf("restored retirement state=%d found=%v err=%v", value, found, err)
		}
		if err := continuationKillLog(log, "stage:"+string(input)); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}}
	return initial, stages
}

func TestRetirementKillChild(t *testing.T) {
	if os.Getenv("WF_RETIREMENT_KILL_CHILD") != "1" {
		t.Skip("retirement worker child helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_RETIREMENT_KILL_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	initial, stages := retirementKillHandlers(os.Getenv("WF_RETIREMENT_KILL_LOG"))
	port := &continuationKillSnapshotPort{SnapshotWritePort: journal.NewSnapshotPort(js), cut: "after_manifest", marker: os.Getenv("WF_RETIREMENT_KILL_MARKER")}
	w, err := worker.New(context.Background(), js, "retirement-killed", initial, worker.WithContinuations(retirementKillType, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(js, port)))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.RunPartition(context.Background(), identity.Partition(retirementKillType, retirementKillID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func retirementKillFileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func TestContinuationRetirementReuseAcrossWorkerSIGKILL(t *testing.T) {
	root := os.Getenv("WF_RETIREMENT_KILL_ROOT")
	if root == "" {
		t.Skip("set WF_RETIREMENT_KILL_ROOT to a fresh absolute artifact directory")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("artifact root must be absolute")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.StartProcesses(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	all := make([]jetstream.JetStream, 3)
	for node := range all {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		all[node], err = jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
	}
	ready, finishReady := context.WithTimeout(context.Background(), 30*time.Second)
	defer finishReady()
	for ready.Err() == nil {
		attempt, finish := context.WithTimeout(ready, 4*time.Second)
		err = provision.Ensure(attempt, all[0], 3)
		finish()
		if err == nil {
			break
		}
		select {
		case <-ready.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := waitMatrixWorkflowReplicas(ready, all[0]); err != nil {
		t.Fatal(err)
	}
	serverProof := []map[string]any{}
	for node, command := range cluster.Commands {
		path := fmt.Sprintf("/proc/%d/exe", command.Process.Pid)
		hash, err := retirementKillFileHash(path)
		if err != nil {
			t.Fatal(err)
		}
		info, err := exec.Command("go", "version", "-m", path).Output()
		if err != nil {
			t.Fatal(err)
		}
		serverProof = append(serverProof, map[string]any{"node": node, "pid": command.Process.Pid, "sha256": hash, "build_info": string(info), "args": command.Args})
	}
	write := func(name string, value any) {
		t.Helper()
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("actual-servers.json", serverProof)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	effectLog := filepath.Join(root, "effects.log")
	initial, stages := retirementKillHandlers(effectLog)
	run := func(w *worker.Worker, ids ...string) (context.CancelFunc, []chan error) {
		runCtx, stop := context.WithCancel(ctx)
		partitions := map[uint32]bool{}
		for _, id := range ids {
			partitions[identity.Partition(retirementKillType, id, provision.Partitions)] = true
		}
		var done []chan error
		for p := range partitions {
			ch := make(chan error, 1)
			done = append(done, ch)
			go func() { ch <- w.RunPartition(runCtx, p) }()
		}
		return stop, done
	}
	join := func(stop context.CancelFunc, done []chan error) {
		t.Helper()
		stop()
		for _, ch := range done {
			if err := <-ch; err != nil {
				t.Fatal(err)
			}
		}
	}
	c := client.New(all[0])
	first, err := c.Start(ctx, retirementKillType, retirementKillID, []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	survivor, err := c.Start(ctx, retirementKillType, "survivor", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := worker.New(ctx, all[1], "retirement-before-kill", initial, worker.WithContinuations(retirementKillType, stages))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	stop, done := run(parent, retirementKillID, "survivor")
	for _, id := range []string{retirementKillID, "survivor"} {
		value, err := c.Await(ctx, retirementKillType, id)
		if err != nil || string(value) != "1" {
			t.Fatalf("initial id=%s value=%s err=%v", id, value, err)
		}
	}
	join(stop, done)
	if err := parent.Close(); err != nil {
		t.Fatal(err)
	}
	store := journal.New(all[2])
	old, err := store.ReadCheckpoint(ctx, retirementKillType, retirementKillID, first.InvSeq)
	if err != nil || old == nil {
		t.Fatalf("old checkpoint=%+v err=%v", old, err)
	}
	kept, err := store.ReadCheckpoint(ctx, retirementKillType, "survivor", survivor.InvSeq)
	if err != nil || kept == nil {
		t.Fatalf("survivor checkpoint=%+v err=%v", kept, err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	snapKey := "snap." + identity.Key(retirementKillType, retirementKillID)
	saved, err := state.Get(ctx, snapKey)
	if err != nil {
		t.Fatal(err)
	}
	oldManifest := append([]byte(nil), saved.Value()...)
	if err := retention.Purge(ctx, all[0], retirementKillType, retirementKillID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Await(ctx, retirementKillType, retirementKillID); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("retired Await=%v", err)
	}
	swept, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute))
	if err != nil || swept.Deleted < 2 {
		t.Fatalf("retired sweep=%+v err=%v", swept, err)
	}
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{old.Snapshot.Runtime.Object, old.Snapshot.Object} {
		if _, err := objects.GetInfo(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("retired object=%s err=%v", name, err)
		}
	}
	sharedJSON, _ := json.Marshal(strings.Repeat("x", wf.MaxInlineResult))
	digest := sha256.Sum256(sharedJSON)
	sharedObject := "step-result-" + hex.EncodeToString(digest[:])
	assertKept := func(names ...string) {
		t.Helper()
		for _, name := range names {
			if _, err := objects.GetInfo(ctx, name); err != nil {
				t.Fatalf("reachable object=%s err=%v", name, err)
			}
		}
		data, err := objects.GetBytes(ctx, sharedObject)
		if err != nil || !bytes.Equal(data, sharedJSON) {
			t.Fatalf("shared bytes lost: %v", err)
		}
	}
	assertKept(kept.Snapshot.Runtime.Object, kept.Snapshot.Object, sharedObject)
	fresh, err := c.Start(ctx, retirementKillType, retirementKillID, []byte(`2`))
	if err != nil || fresh.InvSeq <= first.InvSeq {
		t.Fatalf("reuse=%+v err=%v", fresh, err)
	}
	revision, err := state.Create(ctx, snapKey, oldManifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadCheckpoint(ctx, retirementKillType, retirementKillID, fresh.InvSeq); !errors.Is(err, journal.ErrCheckpointGeneration) {
		t.Fatalf("old generation accepted: %v", err)
	}
	if err := state.Delete(ctx, snapKey, jetstream.LastRevision(revision)); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(root, "cut")
	output, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(executable, "-test.run=^TestRetirementKillChild$")
	child.Env = append(os.Environ(), "WF_RETIREMENT_KILL_CHILD=1", "WF_RETIREMENT_KILL_URL="+cluster.ClientURL(0), "WF_RETIREMENT_KILL_LOG="+effectLog, "WF_RETIREMENT_KILL_MARKER="+marker)
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
	}()
	for {
		if data, err := os.ReadFile(marker); err == nil && string(data) == "after_manifest" {
			break
		}
		select {
		case err := <-exited:
			waited = true
			t.Fatalf("child before cut=%v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	childHash, err := retirementKillFileHash(fmt.Sprintf("/proc/%d/exe", child.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	parentHash, err := retirementKillFileHash(executable)
	if err != nil || childHash != parentHash {
		t.Fatalf("child SDK mismatch %v", err)
	}
	published, err := store.ReadCheckpoint(ctx, retirementKillType, retirementKillID, fresh.InvSeq)
	if err != nil || published == nil || published.Snapshot.Runtime.InvSeq != fresh.InvSeq || published.Snapshot.Runtime.Object == old.Snapshot.Runtime.Object {
		t.Fatalf("fresh publication=%+v err=%v", published, err)
	}
	leases, err := all[0].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	status, err := leases.Status(ctx)
	if err != nil || status.TTL() != provision.LeaseTTL {
		t.Fatalf("lease configuration=%+v err=%v", status, err)
	}
	heldEntry, err := leases.Get(ctx, identity.Key(retirementKillType, retirementKillID))
	if err != nil {
		t.Fatal(err)
	}
	var held lease.Value
	if err := json.Unmarshal(heldEntry.Value(), &held); err != nil || held.Worker != "retirement-killed" || held.Epoch == 0 {
		t.Fatalf("unconfirmed child owner=%+v err=%v", held, err)
	}
	killedAt := time.Now()
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-exited
	waited = true
	waitStatus, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("child did not exit SIGKILL: %v", child.ProcessState)
	}
	write("kill-admission.json", map[string]any{"child_pid": child.Process.Pid, "child_live_sdk_sha256": childHash, "parent_sdk_sha256": parentHash, "signal": "SIGKILL", "cut": "after_manifest", "old_generation": first.InvSeq, "fresh_generation": fresh.InvSeq, "held_epoch": held.Epoch, "lease_ttl": status.TTL().String(), "lease_revision": heldEntry.Revision(), "killed_at": killedAt.UTC(), "reclaimed_objects": swept.Deleted})
	guard := &checkpointNoArchivePort{SnapshotWritePort: journal.NewSnapshotPort(all[2])}
	successor, err := worker.New(ctx, all[2], "retirement-successor", initial, worker.WithContinuations(retirementKillType, stages), worker.WithJournalStore(journal.NewWithJetStreamSnapshotPort(all[2], guard)))
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Close()
	stop, done = run(successor, retirementKillID)
	value, err := c.Await(ctx, retirementKillType, retirementKillID)
	latency := time.Since(killedAt)
	join(stop, done)
	if closeErr := successor.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || string(value) != "2" || latency >= 30*time.Second {
		t.Fatalf("recovery value=%s elapsed=%s err=%v", value, latency, err)
	}
	if guard.archives != 0 || guard.frames == 0 {
		t.Fatalf("archived prefix accessed: archives=%d frames=%d", guard.archives, guard.frames)
	}
	records, _, err := store.Read(ctx, retirementKillType, retirementKillID)
	if err != nil || len(records) == 0 {
		t.Fatalf("fresh records=%d err=%v", len(records), err)
	}
	terminal := records[len(records)-1]
	if terminal.Kind != journal.Completed || terminal.Epoch <= held.Epoch {
		t.Fatalf("terminal did not fence killed owner: %+v held=%d", terminal, held.Epoch)
	}
	for _, peer := range all {
		value, err := client.New(peer).Await(ctx, retirementKillType, retirementKillID)
		if err != nil || string(value) != "2" {
			t.Fatalf("peer result=%s err=%v", value, err)
		}
	}
	if value, err := c.Await(ctx, retirementKillType, "survivor"); err != nil || string(value) != "1" {
		t.Fatalf("survivor=%s err=%v", value, err)
	}
	data, err := os.ReadFile(effectLog)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		counts[line]++
	}
	expected := map[string]int{"initial:1": 2, "effect:1": 2, "stage:1": 2, "initial:2": 1, "effect:2": 1, "stage:2": 1}
	if len(counts) != len(expected) {
		t.Fatalf("unknown effect records: %s", data)
	}
	for line, want := range expected {
		if counts[line] != want {
			t.Fatalf("unexpected %s count=%d want=%d ledger=%s", line, counts[line], want, data)
		}
	}
	report, err := integrity.Check(ctx, all[2])
	if err != nil || report.Invocations != 2 || report.Terminal != 2 {
		t.Fatalf("raw integrity=%+v err=%v", report, err)
	}
	if _, err := retention.SweepBlobsQuiescent(ctx, all[0], 0, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	assertKept(published.Snapshot.Runtime.Object, kept.Snapshot.Runtime.Object, sharedObject)
	write("result.json", map[string]any{"old_generation": first.InvSeq, "fresh_generation": fresh.InvSeq, "held_epoch": held.Epoch, "terminal_epoch": terminal.Epoch, "recovery_seconds": latency.Seconds(), "reclaimed_objects": swept.Deleted, "archive_reads": guard.archives, "frame_reads": guard.frames, "effects": 3, "terminals": report.Terminal, "invocations": report.Invocations, "ledger_counts": counts, "shared_and_survivor_and_fresh_references_verified": true, "all_peer_fresh_results_verified": true})
	t.Logf("retirement SIGKILL recovery=%s old=%d fresh=%d epoch=%d→%d reclaimed=%d archives=%d frames=%d effects=3 terminals=2", latency, first.InvSeq, fresh.InvSeq, held.Epoch, terminal.Epoch, swept.Deleted, guard.archives, guard.frames)
}
