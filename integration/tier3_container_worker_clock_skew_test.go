//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

const tier3WorkerSkewType = "tier3-worker-clock-skew"
const tier3WorkerSkewID = "timer"

func TestFiveContainerWorkerClockSkewChild(t *testing.T) {
	if os.Getenv("WF_TIER3_WORKER_SKEW_CHILD") != "1" {
		t.Skip("worker clock skew child helper")
	}
	offsetSeconds, err := strconv.Atoi(os.Getenv("WF_TIER3_WORKER_SKEW_SECONDS"))
	if err != nil || (offsetSeconds != 5 && offsetSeconds != -5) {
		t.Fatal("invalid worker clock skew")
	}
	response, err := http.Get(os.Getenv("WF_TIER3_WORKER_SKEW_MONITOR") + "/varz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var status struct {
		Now time.Time `json:"now"`
	}
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	measured := time.Now().Sub(status.Now)
	want := time.Duration(offsetSeconds) * time.Second
	if measured < want-2*time.Second || measured > want+2*time.Second {
		t.Fatalf("worker clock offset=%s want=%s±2s", measured, want)
	}
	if err := os.WriteFile(os.Getenv("WF_TIER3_WORKER_SKEW_READY"), []byte(measured.String()), 0600); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(os.Getenv("WF_TIER3_WORKER_SKEW_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "worker-skew", 3*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`"done"`), nil
	}
	w, err := worker.New(context.Background(), js, "tier3-skewed-worker", map[string]worker.Handler{tier3WorkerSkewType: handler})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.RunPartition(context.Background(), identity.Partition(tier3WorkerSkewType, tier3WorkerSkewID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestFiveContainerWorkerClockSkewTimer(t *testing.T) {
	if os.Getenv("WF_TIER3_WORKER_CLOCK_SKEW") != "1" {
		t.Skip("set WF_TIER3_WORKER_CLOCK_SKEW=1 for five-container worker clock skew proof")
	}
	for _, offset := range []time.Duration{5 * time.Second, -5 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			root := t.TempDir()
			overlay, err := testcluster.WriteClockOverlay(root, offset)
			if err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(root, "skewed-integration.test")
			build := exec.CommandContext(ctx, "go", "test", "-c", "-overlay="+overlay, "-o", binary, ".")
			if output, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build skewed worker test process: %v: %s", err, output)
			}
			cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
				t.Fatal(err)
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
			ready := filepath.Join(root, "worker-clock-offset")
			child := exec.Command(binary, "-test.run=^TestFiveContainerWorkerClockSkewChild$")
			child.Env = append(os.Environ(), "WF_TIER3_WORKER_SKEW_CHILD=1", "WF_TIER3_WORKER_SKEW_SECONDS="+strconv.Itoa(int(offset/time.Second)), "WF_TIER3_WORKER_SKEW_MONITOR="+cluster.MonitorURL(1), "WF_TIER3_WORKER_SKEW_URL="+cluster.ClientURL(1), "WF_TIER3_WORKER_SKEW_READY="+ready)
			logFile, err := os.Create(filepath.Join(root, "worker.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer logFile.Close()
			child.Stdout, child.Stderr = logFile, logFile
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
			for until := time.Now().Add(15 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			measured, err := os.ReadFile(ready)
			if err != nil {
				logs, _ := os.ReadFile(logFile.Name())
				t.Fatalf("worker clock was not verified: %v logs=%s", err, logs)
			}
			partition := identity.Partition(tier3WorkerSkewType, tier3WorkerSkewID, provision.Partitions)
			if err := waitFiveReplicaReadiness(ctx, js, partition); err != nil {
				t.Fatal(err)
			}
			recorder := &history.Recorder{}
			defer func() {
				if prefix := os.Getenv("WF_TIER3_SKEW_HISTORY_OUT"); prefix != "" {
					name := "plus5"
					if offset < 0 {
						name = "minus5"
					}
					file, err := os.Create(prefix + "-worker-" + name + ".jsonl")
					if err != nil {
						t.Errorf("create worker skew history: %v", err)
						return
					}
					if err := recorder.WriteJSONL(file); err != nil {
						t.Errorf("write worker skew history: %v", err)
					}
					if err := file.Close(); err != nil {
						t.Errorf("close worker skew history: %v", err)
					}
				}
			}()
			c := client.NewObserved(js, recorder)
			startedAt := time.Now()
			if _, err := c.Start(ctx, tier3WorkerSkewType, tier3WorkerSkewID, []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			value, err := c.Await(ctx, tier3WorkerSkewType, tier3WorkerSkewID)
			elapsed := time.Since(startedAt)
			if err != nil || string(value) != `"done"` {
				t.Fatalf("worker-skew timer result=%s err=%v elapsed=%s", value, err, elapsed)
			}
			if elapsed < 3*time.Second || elapsed >= 30*time.Second {
				t.Errorf("worker-skew timer elapsed=%s, want 3s..30s", elapsed)
			}
			records, _, err := journal.New(js).Read(ctx, tier3WorkerSkewType, tier3WorkerSkewID)
			if err != nil || len(records) != 5 || records[4].Kind != journal.Completed {
				t.Fatalf("worker-skew journal=%+v err=%v", records, err)
			}
			var timer struct {
				FireAt time.Time `json:"fire_at"`
			}
			if err := json.Unmarshal(records[1].Payload, &timer); err != nil {
				t.Fatal(err)
			}
			if timer.FireAt.Before(startedAt.Add(3*time.Second)) || timer.FireAt.After(startedAt.Add(15*time.Second)) {
				t.Fatalf("timer fire_at=%s start=%s offset=%s", timer.FireAt, startedAt, offset)
			}
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			serverAtEnd, err := cluster.ServerNow(attempt, 0)
			stop()
			if err != nil || serverAtEnd.Before(timer.FireAt) {
				t.Fatalf("worker-skew timer completed before server deadline: server_now=%s fire_at=%s err=%v", serverAtEnd, timer.FireAt, err)
			}
			lateness := serverAtEnd.Sub(timer.FireAt)
			if lateness >= 2*time.Second {
				t.Errorf("worker-skew timer lateness=%s, want <2s", lateness)
			}
			if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker skew start history=%s err=%v", result, err)
			}
			if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker skew result history=%s err=%v", result, err)
			}
			report, err := integrity.Check(ctx, js)
			if err != nil || report.Invocations != 1 || report.Journals != 1 || report.Entries != 5 || report.Terminal != 1 {
				t.Fatalf("worker skew retained audit=%+v err=%v", report, err)
			}
			t.Logf("worker_skew=%s measured_offset=%s elapsed=%s fire_at=%s lateness=%s retained=%+v", offset, measured, elapsed, timer.FireAt, lateness, report)
		})
	}
}
