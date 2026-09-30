//go:build linux

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFiveContainerRollingUpgradeFallback(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	oldBinary := os.Getenv("WF_NATS_SERVER_BIN")
	if oldBinary == "" {
		t.Skip("set WF_NATS_SERVER_BIN to a static NATS 2.11 server binary")
	}
	cluster, err := testcluster.StartMixedVersionDockerCluster(t.TempDir(), 5, oldBinary)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err == nil {
				if len(logs) > 8192 {
					logs = logs[len(logs)-8192:]
				}
				t.Logf("node %d log tail:\n%s", node, logs)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_UPGRADE_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create rolling-upgrade history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write rolling-upgrade history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close rolling-upgrade history: %v", err)
			}
		}
	}()
	connect := func(node int) (*nats.Conn, jetstream.JetStream) {
		t.Helper()
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatal(err)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			t.Fatal(err)
		}
		return nc, js
	}
	all := make([]jetstream.JetStream, 5)
	for node := range all {
		nc, js := connect(node)
		defer nc.Close()
		all[node] = js
	}
	if version := all[0].Conn().ConnectedServerVersion(); !strings.HasPrefix(version, "2.11.") {
		t.Fatalf("old container version=%q", version)
	}
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.EnsureFallback(attempt, all[1], 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("five-replica fallback provision: %v", err)
	}
	if mode, err := provision.EnsureAuto(ctx, all[0], 5); err != nil || mode != provision.FallbackTimers {
		t.Fatalf("old peer fallback mode=%q err=%v", mode, err)
	}
	const typ, firstID, signalType, fanoutType, childType = "tier3-rolling-upgrade", "timer", "tier3-upgrade-signal", "tier3-upgrade-fanout", "tier3-upgrade-child"
	partition := identity.Partition(typ, firstID, provision.Partitions)
	var repairedID, afterID, signalID, fanoutID string
	for candidate := 0; candidate < 10_000 && afterID == ""; candidate++ {
		id := fmt.Sprintf("same-part-%d", candidate)
		if identity.Partition(typ, id, provision.Partitions) != partition {
			continue
		}
		if repairedID == "" {
			repairedID = id
		} else {
			afterID = id
		}
	}
	if afterID == "" {
		t.Fatal("could not find same-partition IDs")
	}
	for candidate := 0; candidate < 10_000; candidate++ {
		id := fmt.Sprintf("signal-part-%d", candidate)
		if identity.Partition(signalType, id, provision.Partitions) == partition {
			signalID = id
			break
		}
	}
	if signalID == "" {
		t.Fatal("could not find signal ID on worker partition")
	}
	for candidate := 0; candidate < 10_000; candidate++ {
		id := fmt.Sprintf("fanout-part-%d", candidate)
		if identity.Partition(fanoutType, id, provision.Partitions) == partition {
			fanoutID = id
			break
		}
	}
	if fanoutID == "" {
		t.Fatal("could not find fan-out ID on worker partition")
	}
	w, err := worker.New(ctx, all[3], "tier3-upgrade-worker", map[string]worker.Handler{
		typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			if err := wf.Sleep(c, "wait", time.Second); err != nil {
				return nil, err
			}
			return json.RawMessage(`"done"`), nil
		},
		signalType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			first, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			second, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			return json.Marshal([]json.RawMessage{first, second})
		},
		fanoutType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			promises := make([]wf.Promise, 6)
			for i := range promises {
				promise, err := wf.CallAsync(c, childType, json.RawMessage(fmt.Sprint(i)))
				if err != nil {
					return nil, err
				}
				promises[i] = promise
			}
			var sum int
			for _, promise := range promises {
				value, err := wf.AwaitPromise(c, promise)
				if err != nil {
					return nil, err
				}
				var n int
				if err := json.Unmarshal(value, &n); err != nil {
					return nil, err
				}
				sum += n
			}
			return json.Marshal(sum)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	go func() { workDone <- w.RunPartition(workCtx, partition) }()
	defer func() { stopWork(); <-workDone }()
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunFallbackTimerLoop(loopCtx, all[2], "tier3-upgrade-poller", 100*time.Millisecond, 100)
	}()
	defer func() {
		stopLoop()
		if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	oldClient := client.NewObserved(all[0], recorder)
	if _, err := oldClient.Start(ctx, typ, firstID, []byte(`null`)); err != nil {
		t.Fatalf("start through old container: %v", err)
	}
	if value, err := client.NewObserved(all[4], recorder).Await(ctx, typ, firstID); err != nil || string(value) != `"done"` {
		t.Fatalf("first result=%s err=%v", value, err)
	}
	payload := []byte(`null`)
	digest := sha256.Sum256(payload)
	msg := &nats.Msg{Subject: identity.InvocationSubject(typ, repairedID), Data: payload, Header: nats.Header{}}
	msg.Header.Set("Wf-Input-SHA256", hex.EncodeToString(digest[:]))
	ack, err := all[0].PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("retain invocation without run: %v", err)
	}
	if result, err := reconcile.NewStartScan(all[1]).Scan(ctx, ack.Sequence, 1, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("repair from new container: result=%+v err=%v", result, err)
	}
	if value, err := client.NewObserved(all[4], recorder).Await(ctx, typ, repairedID); err != nil || string(value) != `"done"` {
		t.Fatalf("repaired result=%s err=%v", value, err)
	}
	if retry, err := oldClient.Start(ctx, typ, repairedID, payload); !errors.Is(err, client.ErrAlreadyStarted) || retry.InvSeq != ack.Sequence {
		t.Fatalf("old-container retry: handle=%+v err=%v want=%d", retry, err, ack.Sequence)
	}
	if _, err := oldClient.Start(ctx, signalType, signalID, payload); err != nil {
		t.Fatalf("start signal wait through old container: %v", err)
	}
	for until := time.Now().Add(15 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		records, _, readErr := journal.New(all[3]).Read(attempt, signalType, signalID)
		stop()
		if readErr == nil && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	records, _, err := journal.New(all[3]).Read(ctx, signalType, signalID)
	if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
		t.Fatalf("signal workflow was not suspended before upgrade: records=%d err=%v", len(records), err)
	}
	if _, err := oldClient.Start(ctx, fanoutType, fanoutID, payload); err != nil {
		t.Fatalf("start fan-out through old container: %v", err)
	}
	var childIDs []string
	for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		parentRecords, _, readErr := journal.New(all[3]).Read(attempt, fanoutType, fanoutID)
		stop()
		if readErr == nil {
			ids := map[string]bool{}
			for _, record := range parentRecords {
				if record.Kind != journal.StepRequested {
					continue
				}
				var request struct {
					ChildID string `json:"child_id"`
				}
				if json.Unmarshal(record.Payload, &request) == nil && request.ChildID != "" {
					ids[request.ChildID] = true
				}
			}
			if len(ids) == 6 && len(parentRecords) > 0 && parentRecords[len(parentRecords)-1].Kind == journal.Suspended {
				childIDs = childIDs[:0]
				for id := range ids {
					childIDs = append(childIDs, id)
				}
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(childIDs) != 6 {
		t.Fatalf("fan-out did not durably suspend with six child requests: children=%d", len(childIDs))
	}
	if err := cluster.UpgradeNode(0); err != nil {
		t.Fatalf("upgrade old container: %v", err)
	}
	if _, err := waitMixedVersionReplicaCatchup(ctx, all[1], 5); err != nil {
		t.Fatalf("five-replica catch-up: %v", err)
	}
	upgradedConn, upgraded := connect(0)
	defer upgradedConn.Close()
	if version := upgradedConn.ConnectedServerVersion(); strings.HasPrefix(version, "2.11.") {
		t.Fatalf("upgraded container version=%q", version)
	}
	if mode, err := provision.EnsureAuto(ctx, upgraded, 5); err != nil || mode != provision.FallbackTimers {
		t.Fatalf("upgraded peer fallback mode=%q err=%v", mode, err)
	}
	upgradedClient := client.NewObserved(upgraded, recorder)
	for _, id := range []string{firstID, repairedID} {
		readCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		value, err := upgradedClient.Await(readCtx, typ, id)
		stop()
		if err != nil || string(value) != `"done"` {
			t.Fatalf("retained result %s=%s err=%v probe=%s", id, value, err, mixedVersionResultProbe(ctx, upgraded, typ, id))
		}
	}
	if _, err := upgradedClient.Signal(ctx, signalType, signalID, "go", []byte(`1`), "upgrade-first"); err != nil {
		t.Fatalf("first post-upgrade signal: %v", err)
	}
	if _, err := client.NewObserved(all[1], recorder).Signal(ctx, signalType, signalID, "go", []byte(`2`), "upgrade-second"); err != nil {
		t.Fatalf("second post-upgrade signal: %v", err)
	}
	if value, err := upgradedClient.Await(ctx, signalType, signalID); err != nil || string(value) != `[1,2]` {
		t.Fatalf("ordered post-upgrade signals=%s err=%v", value, err)
	}
	childWorker, err := worker.New(ctx, all[4], "tier3-upgrade-child-worker", map[string]worker.Handler{childType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "child-effect", 0, func(context.Context) (int, error) { return 7, nil })
		return json.RawMessage(fmt.Sprint(value)), err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer childWorker.Close()
	childCtx, stopChildren := context.WithCancel(ctx)
	childParts := map[uint32]bool{}
	for _, id := range childIDs {
		childParts[identity.Partition(childType, id, provision.Partitions)] = true
	}
	childDone := make(chan error, len(childParts))
	for part := range childParts {
		go func(part uint32) { childDone <- childWorker.RunPartition(childCtx, part) }(part)
	}
	defer func() {
		stopChildren()
		for range childParts {
			if err := <-childDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}
	}()
	if value, err := upgradedClient.Await(ctx, fanoutType, fanoutID); err != nil || string(value) != `42` {
		t.Fatalf("post-upgrade fan-out result=%s err=%v", value, err)
	}
	for _, id := range childIDs {
		if value, err := upgradedClient.Await(ctx, childType, id); err != nil || string(value) != `7` {
			t.Fatalf("post-upgrade child %s=%s err=%v", id, value, err)
		}
	}
	if _, err := upgradedClient.Start(ctx, typ, afterID, payload); err != nil {
		t.Fatalf("post-upgrade start: %v", err)
	}
	if value, err := upgradedClient.Await(ctx, typ, afterID); err != nil || string(value) != `"done"` {
		t.Fatalf("post-upgrade result=%s err=%v", value, err)
	}
	if report, err := integrity.Check(ctx, upgraded); err != nil || report.Invocations != 11 || report.Journals != 11 || report.Terminal != 11 {
		t.Fatalf("five-container upgrade audit: report=%+v err=%v", report, err)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("rolling-upgrade Start history=%v err=%v", result, err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("rolling-upgrade Await history=%v err=%v", result, err)
	}
	if result, err := history.CheckSignals(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("rolling-upgrade Signal history=%v err=%v", result, err)
	}
}
