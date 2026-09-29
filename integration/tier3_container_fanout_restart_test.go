//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
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

func TestFiveContainerFanoutSurvivesFullRestart(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	const childCount = 6
	const typ, id, childType = "tier3-fanout", "parent", "tier3-fanout-child"
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	t.Logf("file_store_sync_interval=%s", cluster.SyncInterval())
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatalf("five-node routes: %v", err)
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
	workerConn, workerJS, err := connect(1)
	if err != nil {
		t.Fatal(err)
	}
	defer workerConn.Close()
	handlers := map[string]worker.Handler{
		typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			promises := make([]wf.Promise, childCount)
			for i := range promises {
				p, err := wf.CallAsync(c, childType, json.RawMessage(fmt.Sprint(i)))
				if err != nil {
					return nil, err
				}
				promises[i] = p
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
		childType: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			value, err := wf.Run(c, "effect", 0, func(context.Context) (int, error) { return 7, nil })
			return json.RawMessage(fmt.Sprint(value)), err
		},
	}
	first, err := worker.New(ctx, workerJS, "tier3-before-restart", handlers)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	firstCtx, stopFirst := context.WithCancel(ctx)
	parentPart := identity.Partition(typ, id, provision.Partitions)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, parentPart) }()
	defer func() {
		stopFirst()
		select {
		case <-firstDone:
		default:
		}
	}()
	if err := waitFiveReplicaReadiness(ctx, beforeJS, parentPart); err != nil {
		t.Fatalf("five-replica readiness before fan-out: %v", err)
	}
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_FANOUT_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create fan-out history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write fan-out history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close fan-out history: %v", err)
			}
		}
	}()
	if _, err := client.NewObserved(beforeJS, recorder).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatalf("start fan-out parent: %v", err)
	}
	journalBefore := journal.New(beforeJS)
	var childIDs []string
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		records, _, readErr := journalBefore.Read(attempt, typ, id)
		stop()
		if readErr != nil {
			t.Fatalf("parent journal before restart: %v", readErr)
		}
		childIDs = childIDs[:0]
		for _, record := range records {
			if record.Kind != journal.StepRequested {
				continue
			}
			var req struct {
				Kind    string `json:"kind"`
				ChildID string `json:"child_id"`
			}
			if err := json.Unmarshal(record.Payload, &req); err != nil {
				t.Fatal(err)
			}
			if req.Kind == "call_async" {
				childIDs = append(childIDs, req.ChildID)
			}
		}
		if len(childIDs) == childCount && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if len(childIDs) != childCount {
		t.Fatalf("committed child IDs=%d, want %d", len(childIDs), childCount)
	}
	unique := make(map[string]bool, childCount)
	for _, childID := range childIDs {
		if childID == "" || unique[childID] {
			t.Fatalf("duplicate or empty child ID: %q", childID)
		}
		unique[childID] = true
	}
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first worker: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not stop before cluster restart")
	}
	beforeConn.Close()
	workerConn.Close()
	for i := 0; i < 5; i++ {
		if err := cluster.KillNode(i); err != nil {
			t.Fatalf("kill node %d: %v", i, err)
		}
	}
	killedAt := time.Now()
	for i := 0; i < 5; i++ {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	afterConn, afterJS, err := connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer afterConn.Close()
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		_, err = afterJS.AccountInfo(attempt)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("five-container metadata after restart: %v", err)
	}
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatalf("five-node routes after full restart: %v", err)
	}
	healedAt := time.Now()
	if _, err := client.NewObserved(afterJS, recorder).Start(ctx, typ, id, []byte(`null`)); !errors.Is(err, client.ErrAlreadyStarted) {
		t.Fatalf("parent start retry after full restart: %v", err)
	}
	replacementConn, replacementJS, err := connect(2)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementConn.Close()
	var successor *worker.Worker
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, replacementJS, "tier3-after-restart", handlers)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("replacement worker after restart: %v", err)
	}
	defer successor.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	parts := map[uint32]bool{parentPart: true}
	for _, childID := range childIDs {
		parts[identity.Partition(childType, childID, provision.Partitions)] = true
	}
	var workers sync.WaitGroup
	for part := range parts {
		workers.Add(1)
		go func(part uint32) {
			defer workers.Done()
			_ = successor.RunPartition(workCtx, part)
		}(part)
	}
	defer func() {
		stopWork()
		workers.Wait()
	}()
	result, err := client.NewObserved(afterJS, recorder).Await(ctx, typ, id)
	if err != nil || string(result) != "42" {
		t.Fatalf("parent after five-container restart: result=%s err=%v", result, err)
	}
	completedAt := time.Now()
	for _, childID := range childIDs {
		result, err := client.NewObserved(afterJS, recorder).Await(ctx, childType, childID)
		if err != nil || string(result) != "7" {
			t.Fatalf("child %s after restart: result=%s err=%v", childID, result, err)
		}
	}
	peerConn, peerJS, err := connect(4)
	if err != nil {
		t.Fatal(err)
	}
	defer peerConn.Close()
	if peerResult, err := client.NewObserved(peerJS, recorder).Await(ctx, typ, id); err != nil || string(peerResult) != "42" {
		t.Fatalf("node four parent result after restart=%s err=%v", peerResult, err)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("fan-out start history=%s: %v", result, err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("fan-out result history=%s: %v", result, err)
	}
	var report integrity.Report
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		report, err = integrity.Check(attempt, afterJS)
		stop()
		if err == nil && report.Invocations == childCount+1 && report.Journals == childCount+1 && report.Terminal == childCount+1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || report.Invocations != childCount+1 || report.Journals != childCount+1 || report.Terminal != childCount+1 {
		t.Fatalf("five-container fan-out integrity=%+v err=%v", report, err)
	}
	jrnStream, err := afterJS.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	terminalTime := func(kind, invocationID string) time.Time {
		t.Helper()
		records, _, err := journal.New(afterJS).Read(ctx, kind, invocationID)
		if err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
			t.Fatalf("terminal journal %s.%s: entries=%d err=%v", kind, invocationID, len(records), err)
		}
		last := records[len(records)-1]
		raw, err := jrnStream.GetMsg(ctx, last.Sequence, jetstream.WithGetMsgSubject(identity.JournalSubject(kind, invocationID)))
		if err != nil || raw.Time.IsZero() {
			t.Fatalf("terminal timestamp %s.%s: %v", kind, invocationID, err)
		}
		return raw.Time
	}
	var lastChildTime time.Time
	var slowestChild time.Duration
	for _, childID := range childIDs {
		at := terminalTime(childType, childID)
		if at.After(lastChildTime) {
			lastChildTime = at
		}
		if delay := at.Sub(healedAt); delay > slowestChild {
			slowestChild = delay
		}
	}
	parentEnabledAt := healedAt
	if lastChildTime.After(parentEnabledAt) {
		parentEnabledAt = lastChildTime
	}
	parentAfterLastChild := terminalTime(typ, id).Sub(parentEnabledAt)
	p99 := slowestChild
	if parentAfterLastChild > p99 {
		p99 = parentAfterLastChild
	}
	t.Logf("five-container full restart recovered %d children: kill-to-terminal=%s post-heal=%s slowest-child=%s parent-after-last-child=%s p99=%s target-met=%t retained=%+v", childCount, completedAt.Sub(killedAt), completedAt.Sub(healedAt), slowestChild, parentAfterLastChild, p99, p99 < 30*time.Second, report)
	if p99 >= 30*time.Second {
		t.Fatalf("five-container fan-out recovery p99=%s exceeds 30s target", p99)
	}
}
