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

const tier3WorkerKillType = "tier3-worker-kill"
const tier3WorkerKillID = "inflight-effect"

// The child marks the point after StepRequested is durable and blocks inside
// its effect. The parent SIGKILLs it there, then starts a replacement worker.
func TestFiveContainerWorkerKillChild(t *testing.T) {
	if os.Getenv("WF_TIER3_WORKER_KILL_CHILD") != "1" {
		t.Skip("five-container worker kill child helper")
	}
	url, marker := os.Getenv("WF_TIER3_WORKER_KILL_URL"), os.Getenv("WF_TIER3_WORKER_KILL_MARKER")
	if url == "" || marker == "" {
		t.Fatal("missing worker kill child configuration")
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
	w, err := worker.New(context.Background(), js, "tier3-killed-worker", map[string]worker.Handler{
		tier3WorkerKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			_, err := wf.Run(c, "effect", nil, func(context.Context) (string, error) {
				if err := os.WriteFile(marker+".tmp", []byte("entered"), 0600); err != nil {
					return "", err
				}
				if err := os.Rename(marker+".tmp", marker); err != nil {
					return "", err
				}
				select {}
			})
			return json.RawMessage(`"done"`), err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.RunPartition(context.Background(), identity.Partition(tier3WorkerKillType, tier3WorkerKillID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestFiveContainerWorkerSIGKILLRecovers(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatalf("five-node routes: %v", err)
	}
	nc, err := nats.Connect(cluster.ClientURL(0), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.Ensure(attempt, js, 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision five-replica stores: %v", err)
	}
	partition := identity.Partition(tier3WorkerKillType, tier3WorkerKillID, provision.Partitions)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "entered-effect")
	child := exec.Command(executable, "-test.run=^TestFiveContainerWorkerKillChild$")
	child.Env = append(os.Environ(), "WF_TIER3_WORKER_KILL_CHILD=1", "WF_TIER3_WORKER_KILL_URL="+cluster.ClientURL(0), "WF_TIER3_WORKER_KILL_MARKER="+marker)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "worker-child.log"))
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
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	if err := waitFiveReplicaReadiness(ctx, js, partition); err != nil {
		t.Fatalf("five-replica worker readiness: %v", err)
	}
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_WORKER_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create worker kill history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write worker kill history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close worker kill history: %v", err)
			}
		}
	}()
	c := client.NewObserved(js, recorder)
	startedAt := time.Now()
	if _, err := c.Start(ctx, tier3WorkerKillType, tier3WorkerKillID, []byte(`null`)); err != nil {
		t.Fatalf("start in-flight effect: %v", err)
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
		t.Fatalf("child did not enter effect: %v; logs=%s", err, logs)
	}
	first, _, err := journal.New(js).Read(ctx, tier3WorkerKillType, tier3WorkerKillID)
	if err != nil || len(first) != 2 || first[0].Kind != journal.Started || first[1].Kind != journal.StepRequested {
		t.Fatalf("journal before worker kill: entries=%+v err=%v", first, err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := child.Wait()
	childWaited = true
	status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child was not SIGKILLed: state=%v err=%v", child.ProcessState, waitErr)
	}
	killedAt := time.Now()
	peer, err := nats.Connect(cluster.ClientURL(1), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	peerJS, err := jetstream.New(peer)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := worker.New(ctx, peerJS, "tier3-replacement-worker", map[string]worker.Handler{
		tier3WorkerKillType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			result, err := wf.Run(c, "effect", nil, func(context.Context) (string, error) { return "effect-result", nil })
			if err != nil {
				return nil, err
			}
			if result != "effect-result" {
				return nil, fmt.Errorf("replacement effect result=%q", result)
			}
			return json.RawMessage(`"done"`), nil
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
	value, err := c.Await(ctx, tier3WorkerKillType, tier3WorkerKillID)
	if err != nil || string(value) != `"done"` {
		t.Fatalf("result after worker kill=%s err=%v", value, err)
	}
	completedAt := time.Now()
	peerClient := client.NewObserved(peerJS, recorder)
	if _, err := peerClient.Start(ctx, tier3WorkerKillType, tier3WorkerKillID, []byte(`null`)); !errors.Is(err, client.ErrAlreadyStarted) {
		t.Fatalf("start retry after worker kill: %v", err)
	}
	if peerValue, err := peerClient.Await(ctx, tier3WorkerKillType, tier3WorkerKillID); err != nil || string(peerValue) != `"done"` {
		t.Fatalf("peer result after worker kill=%s err=%v", peerValue, err)
	}
	killToTerminal := completedAt.Sub(killedAt)
	if killToTerminal >= 30*time.Second {
		t.Errorf("kill-to-terminal=%s, want <30s", killToTerminal)
	}
	t.Logf("start-to-terminal=%s kill-to-terminal=%s lease_ttl=%s", completedAt.Sub(startedAt), killToTerminal, provision.LeaseTTL)
	if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("worker kill start history=%s: %v", result, err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("worker kill result history=%s: %v", result, err)
	}
	var report integrity.Report
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		report, err = integrity.Check(attempt, js)
		stop()
		if err == nil && report.Invocations == 1 && report.Journals == 1 && report.Terminal == 1 {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if err != nil || report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 {
		t.Fatalf("worker kill retained audit=%+v: %v", report, err)
	}
	final, _, err := journal.New(peerJS).Read(ctx, tier3WorkerKillType, tier3WorkerKillID)
	if err != nil || len(final) != 4 || final[2].Kind != journal.StepCompleted || final[3].Kind != journal.Completed || final[2].Epoch <= first[1].Epoch {
		t.Fatalf("worker kill journal=%+v err=%v", final, err)
	}
	t.Logf("five-container SIGKILL recovery: %+v", report)
}
