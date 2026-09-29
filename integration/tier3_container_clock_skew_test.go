//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
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

func TestFiveContainerServerClockSkewTimer(t *testing.T) {
	if os.Getenv("WF_TIER3_CLOCK_SKEW") != "1" {
		t.Skip("set WF_TIER3_CLOCK_SKEW=1 for five-container server clock skew proof")
	}
	for _, offset := range []time.Duration{60 * time.Second, -60 * time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			t.Setenv("WF_TIER3_SERVER_SKEW", fmt.Sprintf("4:%s", offset))
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
				t.Fatal(err)
			}
			var measuredOffset time.Duration
			for node := 0; node < 5; node++ {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				serverNow, err := cluster.ServerNow(attempt, node)
				stop()
				if err != nil {
					t.Fatalf("node %d server time: %v", node, err)
				}
				got := serverNow.Sub(time.Now())
				want := time.Duration(0)
				if node == 4 {
					want = offset
				}
				if got < want-2*time.Second || got > want+2*time.Second {
					t.Fatalf("node %d clock offset=%s want=%s±2s", node, got, want)
				}
				if node == 4 {
					measuredOffset = got
				}
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
			controlConn, controlJS, err := connect(0)
			if err != nil {
				t.Fatal(err)
			}
			defer controlConn.Close()
			for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
				attempt, stop := context.WithTimeout(ctx, 5*time.Second)
				err = provision.Ensure(attempt, controlJS, 5)
				stop()
				if err == nil {
					break
				}
				time.Sleep(200 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("provision five-replica stores: %v", err)
			}
			workerConn, workerJS, err := connect(1)
			if err != nil {
				t.Fatal(err)
			}
			defer workerConn.Close()
			const typ, id = "tier3-clock-skew", "timer"
			partition := identity.Partition(typ, id, provision.Partitions)
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
					for i := 0; i < 16; i++ {
						payload, err := wf.AwaitSignal(c, "go")
						if err != nil {
							return nil, err
						}
						if string(payload) != strconv.Itoa(i) {
							return nil, fmt.Errorf("skewed server signal %d carried %q", i, payload)
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
				if err := wf.Sleep(c, "skewed-server", 3*time.Second); err != nil {
					return nil, err
				}
				return json.RawMessage(`"done"`), nil
			}
			w, err := worker.New(ctx, workerJS, "tier3-clock-skew-worker", map[string]worker.Handler{typ: handler, tier3ClockFanoutChildType: tier3ClockFanoutChild})
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			workCtx, stopWork := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- w.RunPartition(workCtx, partition) }()
			defer func() {
				stopWork()
				<-done
			}()
			if err := waitFiveReplicaReadiness(ctx, controlJS, partition); err != nil {
				t.Fatalf("five-replica readiness: %v", err)
			}
			run, err := controlJS.Stream(ctx, "WF_RUN")
			if err != nil {
				t.Fatal(err)
			}
			info, err := run.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			skewedLeader := cluster.NodeName(4)
			if info.Cluster == nil || info.Cluster.Leader != skewedLeader {
				request, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": skewedLeader}})
				attempt, stop := context.WithTimeout(ctx, 5*time.Second)
				reply, err := controlConn.RequestWithContext(attempt, "$JS.API.STREAM.LEADER.STEPDOWN.WF_RUN", request)
				stop()
				if err != nil {
					t.Fatalf("step down WF_RUN leader: %v", err)
				}
				var response struct {
					Success bool            `json:"success"`
					Error   json.RawMessage `json:"error"`
				}
				if err := json.Unmarshal(reply.Data, &response); err != nil || !response.Success {
					t.Fatalf("preferred skewed leader response=%s err=%v", reply.Data, err)
				}
			}
			elected := false
			for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				info, err = run.Info(attempt)
				stop()
				if err == nil && info.Cluster != nil && info.Cluster.Leader == skewedLeader {
					elected = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !elected {
				t.Fatalf("WF_RUN did not elect skewed server: info=%+v err=%v", info, err)
			}
			sigStream, err := controlJS.Stream(ctx, "WF_SIG")
			if err != nil {
				t.Fatal(err)
			}
			sigInfo, err := sigStream.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if sigInfo.Cluster == nil || sigInfo.Cluster.Leader != skewedLeader {
				request, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": skewedLeader}})
				attempt, stop := context.WithTimeout(ctx, 5*time.Second)
				reply, err := controlConn.RequestWithContext(attempt, "$JS.API.STREAM.LEADER.STEPDOWN.WF_SIG", request)
				stop()
				if err != nil {
					t.Fatalf("step down WF_SIG leader: %v", err)
				}
				var response struct {
					Success bool            `json:"success"`
					Error   json.RawMessage `json:"error"`
				}
				if err := json.Unmarshal(reply.Data, &response); err != nil || !response.Success {
					t.Fatalf("preferred WF_SIG skewed leader response=%s err=%v", reply.Data, err)
				}
			}
			signalLeaderReady := false
			for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				sigInfo, err = sigStream.Info(attempt)
				stop()
				if err == nil && sigInfo.Cluster != nil && sigInfo.Cluster.Leader == skewedLeader {
					signalLeaderReady = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !signalLeaderReady {
				t.Fatalf("WF_SIG did not elect skewed server: info=%+v err=%v", sigInfo, err)
			}
			preferLeader := func(name string) jetstream.Stream {
				stream, err := controlJS.Stream(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				info, err := stream.Info(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if info.Cluster == nil || info.Cluster.Leader != skewedLeader {
					request, _ := json.Marshal(map[string]any{"placement": map[string]string{"preferred": skewedLeader}})
					attempt, stop := context.WithTimeout(ctx, 5*time.Second)
					reply, err := controlConn.RequestWithContext(attempt, "$JS.API.STREAM.LEADER.STEPDOWN."+name, request)
					stop()
					if err != nil {
						t.Fatalf("step down %s leader: %v", name, err)
					}
					var response struct {
						Success bool            `json:"success"`
						Error   json.RawMessage `json:"error"`
					}
					if err := json.Unmarshal(reply.Data, &response); err != nil || !response.Success {
						t.Fatalf("preferred %s skewed leader response=%s err=%v", name, reply.Data, err)
					}
				}
				for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
					attempt, stop := context.WithTimeout(ctx, 2*time.Second)
					info, err = stream.Info(attempt)
					stop()
					if err == nil && info.Cluster != nil && info.Cluster.Leader == skewedLeader {
						return stream
					}
					time.Sleep(100 * time.Millisecond)
				}
				t.Fatalf("%s did not elect skewed server: info=%+v err=%v", name, info, err)
				return nil
			}
			journalStream := preferLeader("WF_JRN")
			stateStream := preferLeader("KV_WF_STATE")
			recorder := &history.Recorder{}
			defer func() {
				if prefix := os.Getenv("WF_TIER3_SKEW_HISTORY_OUT"); prefix != "" {
					direction := "plus60"
					if offset < 0 {
						direction = "minus60"
					}
					file, err := os.Create(prefix + "-" + direction + ".jsonl")
					if err != nil {
						t.Errorf("create server skew history: %v", err)
						return
					}
					if err := recorder.WriteJSONL(file); err != nil {
						t.Errorf("write server skew history: %v", err)
					}
					if err := file.Close(); err != nil {
						t.Errorf("close server skew history: %v", err)
					}
				}
			}()
			c := client.NewObserved(controlJS, recorder)
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			serverAtStart, err := cluster.ServerNow(attempt, 4)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			startedAt := time.Now()
			if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			value, err := c.Await(ctx, typ, id)
			elapsed := time.Since(startedAt)
			if err != nil || string(value) != `"done"` {
				t.Fatalf("skewed timer result=%s err=%v elapsed=%s", value, err, elapsed)
			}
			if elapsed < 3*time.Second || elapsed >= 30*time.Second {
				t.Errorf("skewed server timer elapsed=%s, want 3s..30s", elapsed)
			}
			records, _, err := journal.New(controlJS).Read(ctx, typ, id)
			if err != nil || len(records) != 5 || records[len(records)-1].Kind != journal.Completed {
				t.Fatalf("skewed timer journal=%+v err=%v", records, err)
			}
			var request struct {
				Kind   string    `json:"kind"`
				FireAt time.Time `json:"fire_at"`
			}
			if err := json.Unmarshal(records[1].Payload, &request); err != nil || request.Kind != "timer" || request.FireAt.Sub(serverAtStart) < 3*time.Second || request.FireAt.Sub(serverAtStart) > 15*time.Second {
				t.Fatalf("journaled fire_at=%s server_at_start=%s payload=%s err=%v", request.FireAt, serverAtStart, records[1].Payload, err)
			}
			attempt, stop = context.WithTimeout(ctx, 2*time.Second)
			serverAtEnd, err := cluster.ServerNow(attempt, 4)
			stop()
			if err != nil || serverAtEnd.Before(request.FireAt) {
				t.Fatalf("timer completed before skewed server deadline: server_now=%s fire_at=%s err=%v", serverAtEnd, request.FireAt, err)
			}
			lateness := serverAtEnd.Sub(request.FireAt)
			if lateness >= 2*time.Second {
				t.Errorf("timer lateness on skewed server=%s, want <2s", lateness)
			}
			attempt, stop = context.WithTimeout(ctx, 2*time.Second)
			finalRunInfo, err := run.Info(attempt)
			stop()
			if err != nil || finalRunInfo == nil || finalRunInfo.Cluster == nil || finalRunInfo.Cluster.Leader != skewedLeader {
				t.Fatalf("WF_RUN leader changed during skew test: info=%+v err=%v", finalRunInfo, err)
			}
			if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("skewed timer start history=%s err=%v", result, err)
			}
			if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("skewed timer result history=%s err=%v", result, err)
			}
			signalID := tier3ClockSignalID(typ, id)
			if _, err := c.Start(ctx, typ, signalID, []byte(`{"signal":true}`)); err != nil {
				t.Fatal(err)
			}
			var previousPublished uint64
			for i := 0; i < 16; i++ {
				sequence, err := c.Signal(ctx, typ, signalID, "go", []byte(strconv.Itoa(i)), fmt.Sprintf("skew-%02d", i))
				if err != nil || sequence <= previousPublished {
					t.Fatalf("server-skew signal %d sequence=%d previous=%d err=%v", i, sequence, previousPublished, err)
				}
				previousPublished = sequence
			}
			enablingAt := time.Now()
			retrySequence, err := c.Signal(ctx, typ, signalID, "go", []byte("0"), "skew-00")
			if err != nil || retrySequence == 0 {
				t.Fatalf("server-skew matching signal retry sequence=%d err=%v", retrySequence, err)
			}
			if _, err := c.Signal(ctx, typ, signalID, "go", []byte("changed"), "skew-00"); !errors.Is(err, client.ErrSignalMismatch) {
				t.Fatalf("server-skew changed signal retry: %v", err)
			}
			signalResult, err := c.Await(ctx, typ, signalID)
			signalLatency := time.Since(enablingAt)
			if err != nil || string(signalResult) != `16` || signalLatency >= 30*time.Second {
				t.Fatalf("server-skew signal result=%s err=%v latency=%s", signalResult, err, signalLatency)
			}
			signalRecords, _, err := journal.New(controlJS).Read(ctx, typ, signalID)
			if err != nil || len(signalRecords) == 0 {
				t.Fatalf("server-skew signal journal=%+v err=%v", signalRecords, err)
			}
			var consumed int
			var previousConsumed uint64
			for _, record := range signalRecords {
				if record.Kind != journal.SignalConsumed {
					continue
				}
				var signal struct {
					Sequence uint64 `json:"sig_seq"`
					Payload  []byte `json:"payload"`
				}
				if err := json.Unmarshal(record.Payload, &signal); err != nil || signal.Sequence <= previousConsumed || string(signal.Payload) != strconv.Itoa(consumed) {
					t.Fatalf("server-skew consumed signal %d: sequence=%d previous=%d payload=%q err=%v", consumed, signal.Sequence, previousConsumed, signal.Payload, err)
				}
				previousConsumed = signal.Sequence
				consumed++
			}
			if consumed != 16 || signalRecords[len(signalRecords)-1].Kind != journal.Completed {
				t.Fatalf("server-skew consumed=%d journal entries=%d", consumed, len(signalRecords))
			}
			attempt, stop = context.WithTimeout(ctx, 2*time.Second)
			finalSigInfo, err := sigStream.Info(attempt)
			stop()
			if err != nil || finalSigInfo == nil || finalSigInfo.Cluster == nil || finalSigInfo.Cluster.Leader != skewedLeader {
				t.Fatalf("WF_SIG leader changed during skewed signals: info=%+v err=%v", finalSigInfo, err)
			}
			if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("server-skew combined start history=%s err=%v", result, err)
			}
			if result, err := history.CheckSignals(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("server-skew signal history=%s err=%v", result, err)
			}
			if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("server-skew combined result history=%s err=%v", result, err)
			}
			const shortCount = 100
			shortIDs := tier3ClockIDsForPartition(typ, id, "short", shortCount)
			started := make([]time.Time, shortCount)
			for i, shortID := range shortIDs {
				input, _ := json.Marshal(struct {
					Short bool `json:"short"`
					N     int  `json:"n"`
				}{true, i})
				started[i] = time.Now()
				if _, err := c.Start(ctx, typ, shortID, input); err != nil {
					t.Fatalf("server-skew short start %d: %v", i, err)
				}
			}
			latencies := make([]time.Duration, shortCount)
			for i, shortID := range shortIDs {
				result, err := c.Await(ctx, typ, shortID)
				latencies[i] = time.Since(started[i])
				if err != nil || string(result) != strconv.Itoa(i*2) {
					t.Fatalf("server-skew short result %d=%s err=%v", i, result, err)
				}
				entries, _, err := journal.New(controlJS).Read(ctx, typ, shortID)
				if err != nil || len(entries) != 4 || entries[0].Kind != journal.Started || entries[1].Kind != journal.StepRequested || entries[2].Kind != journal.StepCompleted || entries[3].Kind != journal.Completed {
					t.Fatalf("server-skew short journal %d=%+v err=%v", i, entries, err)
				}
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			shortP99 := latencies[98]
			if shortP99 >= 30*time.Second {
				t.Errorf("server-skew short start-to-result p99=%s, want <30s", shortP99)
			}
			readConn, readJS, err := connect(2)
			if err != nil {
				t.Fatal(err)
			}
			defer readConn.Close()
			crossNode := client.New(readJS)
			for i, shortID := range shortIDs {
				result, err := crossNode.Await(ctx, typ, shortID)
				if err != nil || string(result) != strconv.Itoa(i*2) {
					t.Fatalf("server-skew cross-node short result %d=%s err=%v", i, result, err)
				}
			}
			for _, item := range []struct {
				Name   string
				Stream jetstream.Stream
			}{{"WF_JRN", journalStream}, {"KV_WF_STATE", stateStream}} {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				info, err := item.Stream.Info(attempt)
				stop()
				if err != nil || info == nil || info.Cluster == nil || info.Cluster.Leader != skewedLeader {
					t.Fatalf("%s leader changed during short workload: info=%+v err=%v", item.Name, info, err)
				}
			}
			if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("server-skew short start history=%s err=%v", result, err)
			}
			if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
				t.Fatalf("server-skew short result history=%s err=%v", result, err)
			}
			fanoutID := tier3ClockIDsForPartition(typ, id, "fanout", 1)[0]
			if _, err := c.Start(ctx, typ, fanoutID, []byte(`{"fanout":true}`)); err != nil {
				t.Fatalf("server-skew fan-out parent start: %v", err)
			}
			childIDs, childParts := tier3ClockFanoutRequests(t, ctx, controlJS, typ, fanoutID)
			var otherParts []uint32
			for _, part := range childParts {
				if part != partition {
					otherParts = append(otherParts, part)
				}
			}
			if len(otherParts) > 0 {
				childWorker, err := worker.New(ctx, readJS, "tier3-server-skew-child-worker", map[string]worker.Handler{tier3ClockFanoutChildType: tier3ClockFanoutChild})
				if err != nil {
					t.Fatal(err)
				}
				defer childWorker.Close()
				childCtx, stopChildren := context.WithCancel(ctx)
				childDone := make(chan error, 1)
				go func() { childDone <- childWorker.RunPartitions(childCtx, otherParts) }()
				defer func() { stopChildren(); <-childDone }()
				for _, part := range otherParts {
					if err := waitFiveReplicaReadiness(ctx, controlJS, part); err != nil {
						t.Fatalf("server-skew child partition %d readiness: %v", part, err)
					}
				}
			}
			fanoutLatency, fanoutEntries := tier3ClockFanoutVerify(t, ctx, controlJS, c, recorder, typ, fanoutID, childIDs)
			parentCrossNode, err := crossNode.Await(ctx, typ, fanoutID)
			if err != nil || string(parentCrossNode) != `30` {
				t.Fatalf("server-skew cross-node fan-out parent=%s err=%v", parentCrossNode, err)
			}
			for i, childID := range childIDs {
				childCrossNode, err := crossNode.Await(ctx, tier3ClockFanoutChildType, childID)
				if err != nil || string(childCrossNode) != strconv.Itoa(i*2) {
					t.Fatalf("server-skew cross-node child %d=%s err=%v", i, childCrossNode, err)
				}
			}
			for _, item := range []struct {
				Name   string
				Stream jetstream.Stream
			}{{"WF_RUN", run}, {"WF_SIG", sigStream}, {"WF_JRN", journalStream}, {"KV_WF_STATE", stateStream}} {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				info, err := item.Stream.Info(attempt)
				stop()
				if err != nil || info == nil || info.Cluster == nil || info.Cluster.Leader != skewedLeader {
					t.Fatalf("%s leader changed during skewed fan-out: info=%+v err=%v", item.Name, info, err)
				}
			}
			report, err := integrity.Check(ctx, controlJS)
			if err != nil || report.Invocations != shortCount+2+1+len(childIDs) || report.Journals != shortCount+2+1+len(childIDs) || report.Entries != len(signalRecords)+5+4*shortCount+fanoutEntries || report.Terminal != shortCount+2+1+len(childIDs) {
				t.Fatalf("skewed timer retained audit=%+v err=%v", report, err)
			}
			t.Logf("server_skew=%s measured_offset=%s WF_RUN_SIG_JRN_STATE_leader=%s timer_elapsed=%s timer_lateness=%s ordered_signals=%d signal_latency=%s short_count=%d short_p99=%s fanout_children=%d last_child_to_parent=%s retained=%+v", offset, measuredOffset, skewedLeader, elapsed, lateness, consumed, signalLatency, shortCount, shortP99, len(childIDs), fanoutLatency, report)
		})
	}
}
