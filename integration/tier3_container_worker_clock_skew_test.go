//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
const tier3WorkerSkewSignalCount = 16

func tier3WorkerSkewSignalID() string {
	return tier3ClockSignalID(tier3WorkerSkewType, tier3WorkerSkewID)
}

func tier3ClockSignalID(typ, timerID string) string {
	partition := identity.Partition(typ, timerID, provision.Partitions)
	for i := 0; ; i++ {
		id := fmt.Sprintf("signals-%d", i)
		if identity.Partition(typ, id, provision.Partitions) == partition {
			return id
		}
	}
}

func tier3ClockIDsForPartition(typ, referenceID, prefix string, count int) []string {
	partition := identity.Partition(typ, referenceID, provision.Partitions)
	ids := make([]string, 0, count)
	for i := 0; len(ids) < count; i++ {
		id := fmt.Sprintf("%s-%d", prefix, i)
		if identity.Partition(typ, id, provision.Partitions) == partition {
			ids = append(ids, id)
		}
	}
	return ids
}

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
	handler := func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var request struct {
			Signal bool `json:"signal"`
			Short  bool `json:"short"`
			Fanout bool `json:"fanout"`
			N      int  `json:"n"`
		}
		if err := json.Unmarshal(input, &request); err != nil {
			return nil, err
		}
		if request.Signal {
			for i := 0; i < tier3WorkerSkewSignalCount; i++ {
				payload, err := wf.AwaitSignal(c, "go")
				if err != nil {
					return nil, err
				}
				if string(payload) != strconv.Itoa(i) {
					return nil, fmt.Errorf("skewed worker signal %d carried %q", i, payload)
				}
			}
			return json.RawMessage(`16`), nil
		}
		if request.Short {
			value, err := wf.Run(c, "double", request.N, func(context.Context) (int, error) { return request.N * 2, nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		}
		if request.Fanout {
			return tier3ClockFanoutParent(c)
		}
		if err := wf.Sleep(c, "worker-skew", 3*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`"done"`), nil
	}
	workerID := os.Getenv("WF_TIER3_WORKER_SKEW_WORKER_ID")
	if workerID == "" {
		workerID = "tier3-skewed-worker"
	}
	w, err := worker.New(context.Background(), js, workerID, map[string]worker.Handler{tier3WorkerSkewType: handler, tier3ClockFanoutChildType: tier3ClockFanoutChild})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	partitions := []uint32{identity.Partition(tier3WorkerSkewType, tier3WorkerSkewID, provision.Partitions)}
	if selected := os.Getenv("WF_TIER3_WORKER_SKEW_PARTITIONS"); selected != "" {
		partitions = nil
		for _, raw := range strings.Split(selected, ",") {
			part, err := strconv.ParseUint(raw, 10, 32)
			if err != nil || part >= uint64(provision.Partitions) {
				t.Fatalf("invalid child partition %q: %v", raw, err)
			}
			partitions = append(partitions, uint32(part))
		}
	}
	if err := w.RunPartitions(context.Background(), partitions); err != nil {
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
			signalID := tier3WorkerSkewSignalID()
			if err := waitFiveReplicaReadiness(ctx, js, identity.Partition(tier3WorkerSkewType, signalID, provision.Partitions)); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Start(ctx, tier3WorkerSkewType, signalID, []byte(`{"signal":true}`)); err != nil {
				t.Fatal(err)
			}
			var signalSequence uint64
			for i := 0; i < tier3WorkerSkewSignalCount; i++ {
				sequence, err := c.Signal(ctx, tier3WorkerSkewType, signalID, "go", []byte(strconv.Itoa(i)), fmt.Sprintf("skew-%02d", i))
				if err != nil || sequence <= signalSequence {
					t.Fatalf("worker-skew signal %d sequence=%d previous=%d err=%v", i, sequence, signalSequence, err)
				}
				signalSequence = sequence
			}
			enablingAt := time.Now()
			retrySequence, err := c.Signal(ctx, tier3WorkerSkewType, signalID, "go", []byte("0"), "skew-00")
			if err != nil || retrySequence == 0 {
				t.Fatalf("worker-skew matching signal retry sequence=%d err=%v", retrySequence, err)
			}
			if _, err := c.Signal(ctx, tier3WorkerSkewType, signalID, "go", []byte("changed"), "skew-00"); !errors.Is(err, client.ErrSignalMismatch) {
				t.Fatalf("worker-skew changed signal retry: %v", err)
			}
			signalResult, err := c.Await(ctx, tier3WorkerSkewType, signalID)
			signalLatency := time.Since(enablingAt)
			if err != nil || string(signalResult) != `16` || signalLatency >= 30*time.Second {
				t.Fatalf("worker-skew signal result=%s err=%v latency=%s", signalResult, err, signalLatency)
			}
			signalRecords, _, err := journal.New(js).Read(ctx, tier3WorkerSkewType, signalID)
			if err != nil || len(signalRecords) == 0 {
				t.Fatalf("worker-skew signal journal=%+v err=%v", signalRecords, err)
			}
			var consumed int
			var previous uint64
			for _, record := range signalRecords {
				if record.Kind != journal.SignalConsumed {
					continue
				}
				var signal struct {
					Sequence uint64 `json:"sig_seq"`
					Payload  []byte `json:"payload"`
				}
				if err := json.Unmarshal(record.Payload, &signal); err != nil || signal.Sequence <= previous || string(signal.Payload) != strconv.Itoa(consumed) {
					t.Fatalf("worker-skew consumed signal %d: sequence=%d previous=%d payload=%q err=%v", consumed, signal.Sequence, previous, signal.Payload, err)
				}
				previous = signal.Sequence
				consumed++
			}
			if consumed != tier3WorkerSkewSignalCount || signalRecords[len(signalRecords)-1].Kind != journal.Completed {
				t.Fatalf("worker-skew consumed=%d journal entries=%d", consumed, len(signalRecords))
			}
			if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker-skew combined start history=%s err=%v", result, err)
			}
			if result, err := history.CheckSignals(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker-skew signal history=%s err=%v", result, err)
			}
			if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker-skew combined result history=%s err=%v", result, err)
			}
			const shortCount = 100
			shortIDs := tier3ClockIDsForPartition(tier3WorkerSkewType, tier3WorkerSkewID, "short", shortCount)
			started := make([]time.Time, shortCount)
			for i, shortID := range shortIDs {
				input, _ := json.Marshal(struct {
					Short bool `json:"short"`
					N     int  `json:"n"`
				}{true, i})
				started[i] = time.Now()
				if _, err := c.Start(ctx, tier3WorkerSkewType, shortID, input); err != nil {
					t.Fatalf("worker-skew short start %d: %v", i, err)
				}
			}
			latencies := make([]time.Duration, shortCount)
			for i, shortID := range shortIDs {
				result, err := c.Await(ctx, tier3WorkerSkewType, shortID)
				latencies[i] = time.Since(started[i])
				if err != nil || string(result) != strconv.Itoa(i*2) {
					t.Fatalf("worker-skew short result %d=%s err=%v", i, result, err)
				}
				entries, _, err := journal.New(js).Read(ctx, tier3WorkerSkewType, shortID)
				if err != nil || len(entries) != 4 || entries[0].Kind != journal.Started || entries[1].Kind != journal.StepRequested || entries[2].Kind != journal.StepCompleted || entries[3].Kind != journal.Completed {
					t.Fatalf("worker-skew short journal %d=%+v err=%v", i, entries, err)
				}
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			shortP99 := latencies[98]
			if shortP99 >= 30*time.Second {
				t.Errorf("worker-skew short start-to-result p99=%s, want <30s", shortP99)
			}
			readConn, err := nats.Connect(cluster.ClientURL(2), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
			if err != nil {
				t.Fatal(err)
			}
			defer readConn.Close()
			readJS, err := jetstream.New(readConn)
			if err != nil {
				t.Fatal(err)
			}
			crossNode := client.New(readJS)
			for i, shortID := range shortIDs {
				result, err := crossNode.Await(ctx, tier3WorkerSkewType, shortID)
				if err != nil || string(result) != strconv.Itoa(i*2) {
					t.Fatalf("worker-skew cross-node short result %d=%s err=%v", i, result, err)
				}
			}
			if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker-skew short start history=%s err=%v", result, err)
			}
			if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("worker-skew short result history=%s err=%v", result, err)
			}
			fanoutID := tier3ClockIDsForPartition(tier3WorkerSkewType, tier3WorkerSkewID, "fanout", 1)[0]
			if _, err := c.Start(ctx, tier3WorkerSkewType, fanoutID, []byte(`{"fanout":true}`)); err != nil {
				t.Fatalf("worker-skew fan-out parent start: %v", err)
			}
			childIDs, childParts := tier3ClockFanoutRequests(t, ctx, js, tier3WorkerSkewType, fanoutID)
			var otherParts []uint32
			for _, part := range childParts {
				if part != partition {
					otherParts = append(otherParts, part)
				}
			}
			if len(otherParts) > 0 {
				partNames := make([]string, len(otherParts))
				for i, part := range otherParts {
					partNames[i] = strconv.FormatUint(uint64(part), 10)
				}
				childReady := filepath.Join(root, "child-worker-clock-offset")
				children := exec.Command(binary, "-test.run=^TestFiveContainerWorkerClockSkewChild$")
				children.Env = append(os.Environ(), "WF_TIER3_WORKER_SKEW_CHILD=1", "WF_TIER3_WORKER_SKEW_SECONDS="+strconv.Itoa(int(offset/time.Second)), "WF_TIER3_WORKER_SKEW_MONITOR="+cluster.MonitorURL(2), "WF_TIER3_WORKER_SKEW_URL="+cluster.ClientURL(2), "WF_TIER3_WORKER_SKEW_READY="+childReady, "WF_TIER3_WORKER_SKEW_PARTITIONS="+strings.Join(partNames, ","), "WF_TIER3_WORKER_SKEW_WORKER_ID=tier3-skewed-children")
				childLog, err := os.Create(filepath.Join(root, "children.log"))
				if err != nil {
					t.Fatal(err)
				}
				defer childLog.Close()
				children.Stdout, children.Stderr = childLog, childLog
				if err := children.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = children.Process.Kill(); _ = children.Wait() }()
				for until := time.Now().Add(15 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
					if _, err := os.Stat(childReady); err == nil {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				if _, err := os.ReadFile(childReady); err != nil {
					logs, _ := os.ReadFile(childLog.Name())
					t.Fatalf("skewed child worker clock not verified: %v logs=%s", err, logs)
				}
				for _, part := range otherParts {
					if err := waitFiveReplicaReadiness(ctx, js, part); err != nil {
						t.Fatalf("worker-skew child partition %d readiness: %v", part, err)
					}
				}
			}
			fanoutLatency, fanoutEntries := tier3ClockFanoutVerify(t, ctx, js, c, recorder, tier3WorkerSkewType, fanoutID, childIDs)
			parentCrossNode, err := crossNode.Await(ctx, tier3WorkerSkewType, fanoutID)
			if err != nil || string(parentCrossNode) != `30` {
				t.Fatalf("worker-skew cross-node fan-out parent=%s err=%v", parentCrossNode, err)
			}
			for i, childID := range childIDs {
				childCrossNode, err := crossNode.Await(ctx, tier3ClockFanoutChildType, childID)
				if err != nil || string(childCrossNode) != strconv.Itoa(i*2) {
					t.Fatalf("worker-skew cross-node child %d=%s err=%v", i, childCrossNode, err)
				}
			}
			report, err := integrity.Check(ctx, js)
			if err != nil || report.Invocations != shortCount+2+1+len(childIDs) || report.Journals != shortCount+2+1+len(childIDs) || report.Entries != len(signalRecords)+5+4*shortCount+fanoutEntries || report.Terminal != shortCount+2+1+len(childIDs) {
				t.Fatalf("worker skew retained audit=%+v err=%v", report, err)
			}
			t.Logf("worker_skew=%s measured_offset=%s timer_elapsed=%s timer_lateness=%s ordered_signals=%d signal_latency=%s short_count=%d short_p99=%s fanout_children=%d last_child_to_parent=%s retained=%+v", offset, measured, elapsed, lateness, consumed, signalLatency, shortCount, shortP99, len(childIDs), fanoutLatency, report)
		})
	}
}
