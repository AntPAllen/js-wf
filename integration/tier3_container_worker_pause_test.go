//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const tier3WorkerPauseType = "tier3-worker-pause"
const tier3WorkerPauseID = "stale-effect"

func TestFiveContainerWorkerPauseChild(t *testing.T) {
	if os.Getenv("WF_TIER3_WORKER_PAUSE_CHILD") != "1" {
		t.Skip("five-container worker pause child helper")
	}
	url, marker := os.Getenv("WF_TIER3_WORKER_PAUSE_URL"), os.Getenv("WF_TIER3_WORKER_PAUSE_MARKER")
	release, outcome := os.Getenv("WF_TIER3_WORKER_PAUSE_RELEASE"), os.Getenv("WF_TIER3_WORKER_PAUSE_OUTCOME")
	metricsPath := os.Getenv("WF_TIER3_WORKER_PAUSE_METRICS")
	if url == "" || marker == "" || release == "" || outcome == "" || metricsPath == "" {
		t.Fatal("missing worker pause child configuration")
	}
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", nil, func(context.Context) (string, error) {
			if err := os.WriteFile(marker+".tmp", []byte("entered"), 0600); err != nil {
				return "", err
			}
			if err := os.Rename(marker+".tmp", marker); err != nil {
				return "", err
			}
			for {
				if _, err := os.Stat(release); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					return "", err
				}
				time.Sleep(10 * time.Millisecond)
			}
			return "old-effect", nil
		})
		result := "success:" + value
		if err != nil {
			result = "error:" + err.Error()
		}
		if writeErr := os.WriteFile(outcome, []byte(result), 0600); writeErr != nil {
			return nil, writeErr
		}
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	w, err := worker.New(context.Background(), js, "tier3-paused-worker", map[string]worker.Handler{tier3WorkerPauseType: handler})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	go func() {
		for {
			if count := w.Metrics().FencingEvents; count > 0 {
				_ = os.WriteFile(metricsPath, []byte(fmt.Sprint(count)), 0600)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	if err := w.RunPartition(context.Background(), identity.Partition(tier3WorkerPauseType, tier3WorkerPauseID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestFiveContainerWorkerPausePastLease(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for node := 0; node < 5; node++ {
			if logs, err := cluster.Logs(node); err == nil {
				if len(logs) > 8192 {
					logs = logs[len(logs)-8192:]
				}
				t.Logf("node %d log tail:\n%s", node, logs)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	connect := func(node int) (*nats.Conn, jetstream.JetStream, error) {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			return nil, nil, err
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, nil, err
		}
		return nc, js, nil
	}
	beforeConn, beforeJS, err := connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer beforeConn.Close()
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.Ensure(attempt, beforeJS, 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision five-replica stores: %v", err)
	}
	partition := identity.Partition(tier3WorkerPauseType, tier3WorkerPauseID, provision.Partitions)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	marker, release, outcome := filepath.Join(root, "entered"), filepath.Join(root, "release"), filepath.Join(root, "outcome")
	metricsPath := filepath.Join(root, "fencing-count")
	child := exec.Command(executable, "-test.run=^TestFiveContainerWorkerPauseChild$")
	child.Env = append(os.Environ(), "WF_TIER3_WORKER_PAUSE_CHILD=1", "WF_TIER3_WORKER_PAUSE_URL="+cluster.ClientURL(0), "WF_TIER3_WORKER_PAUSE_MARKER="+marker, "WF_TIER3_WORKER_PAUSE_RELEASE="+release, "WF_TIER3_WORKER_PAUSE_OUTCOME="+outcome, "WF_TIER3_WORKER_PAUSE_METRICS="+metricsPath)
	logFile, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	child.Stdout, child.Stderr = logFile, logFile
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	childWaited := false
	defer func() {
		if !childWaited {
			_ = child.Process.Signal(syscall.SIGCONT)
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	if err := waitFiveReplicaReadiness(ctx, beforeJS, partition); err != nil {
		t.Fatalf("five-replica worker readiness: %v", err)
	}
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_PAUSE_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create worker pause history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write worker pause history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close worker pause history: %v", err)
			}
		}
	}()
	startClient := client.NewObserved(beforeJS, recorder)
	if _, err := startClient.Start(ctx, tier3WorkerPauseType, tier3WorkerPauseID, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		if _, err := os.Stat(marker); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(marker); err != nil {
		logs, _ := os.ReadFile(logFile.Name())
		t.Fatalf("first worker did not enter effect: %v logs=%s", err, logs)
	}
	prefix, _, err := journal.New(beforeJS).Read(ctx, tier3WorkerPauseType, tier3WorkerPauseID)
	if err != nil || len(prefix) != 2 || prefix[0].Kind != journal.Started || prefix[1].Kind != journal.StepRequested {
		t.Fatalf("journal before worker pause: entries=%+v err=%v", prefix, err)
	}
	if err := child.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	pausedAt := time.Now()
	replacementConn, replacementJS, err := connect(1)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementConn.Close()
	replacement, err := worker.New(ctx, replacementJS, "tier3-pause-replacement", map[string]worker.Handler{
		tier3WorkerPauseType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			value, err := wf.Run(c, "effect", nil, func(context.Context) (string, error) { return "new-effect", nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	go func() { workDone <- replacement.RunPartition(workCtx, partition) }()
	defer func() {
		stopWork()
		<-workDone
	}()
	reader := client.NewObserved(replacementJS, recorder)
	value, err := reader.Await(ctx, tier3WorkerPauseType, tier3WorkerPauseID)
	if err != nil || string(value) != `"new-effect"` {
		t.Fatalf("replacement result=%s err=%v", value, err)
	}
	completedAt := time.Now()
	if remaining := 45*time.Second - time.Since(pausedAt); remaining > 0 {
		select {
		case <-time.After(remaining):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err := child.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, []byte("resume"), 0600); err != nil {
		t.Fatal(err)
	}
	var staleOutcome string
	for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		data, err := os.ReadFile(outcome)
		if err == nil {
			staleOutcome = string(data)
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if staleOutcome == "" || !strings.HasPrefix(staleOutcome, "error:") {
		t.Fatalf("resumed old worker result=%q, want fenced error", staleOutcome)
	}
	var fencingCount string
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		data, err := os.ReadFile(metricsPath)
		if err == nil {
			fencingCount = string(data)
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	count, parseErr := strconv.ParseUint(fencingCount, 10, 64)
	if parseErr != nil || count == 0 {
		t.Fatalf("paused worker did not report a fencing event: count=%q", fencingCount)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("paused worker process unexpectedly exited cleanly")
	}
	childWaited = true
	immutable, err := client.NewObserved(beforeJS, recorder).Await(ctx, tier3WorkerPauseType, tier3WorkerPauseID)
	if err != nil || string(immutable) != `"new-effect"` {
		t.Fatalf("terminal result after stale worker resumed=%s err=%v", immutable, err)
	}
	records, _, err := journal.New(beforeJS).Read(ctx, tier3WorkerPauseType, tier3WorkerPauseID)
	if err != nil || len(records) != 4 || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed || records[2].Epoch <= prefix[1].Epoch {
		t.Fatalf("journal after worker pause=%+v err=%v", records, err)
	}
	report, err := integrity.Check(ctx, beforeJS)
	if err != nil || report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 || report.Entries != 4 {
		t.Fatalf("retained audit=%+v err=%v", report, err)
	}
	operations := recorder.Snapshot()
	if result, err := history.CheckStarts(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("worker pause start history=%s: %v", result, err)
	}
	if result, err := history.CheckResults(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("worker pause result history=%s: %v", result, err)
	}
	t.Logf("five-container worker paused=%s successor_terminal_after_pause=%s stale_outcome=%q fencing_events=%s retained=%+v", time.Since(pausedAt), completedAt.Sub(pausedAt), staleOutcome, fencingCount, report)
}
