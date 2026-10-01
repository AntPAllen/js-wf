//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// TestFanoutSurvivesFullServerRestart covers the full-cluster restart row of
// the fan-out chaos matrix after all child invocations are committed.
func TestFanoutSurvivesFullServerRestart(t *testing.T) {
	if os.Getenv("WF_FULL_RESTART_FANOUT") != "1" {
		t.Skip("set WF_FULL_RESTART_FANOUT=1 for the process restart proof")
	}
	runFanoutFullServerRestart(t, 6)
}

func TestFiveHundredChildFanoutSurvivesFullServerRestart(t *testing.T) {
	if os.Getenv("WF_FULL_RESTART_FANOUT_500") != "1" {
		t.Skip("set WF_FULL_RESTART_FANOUT_500=1 for the 500-child process restart proof")
	}
	runFanoutFullServerRestart(t, 500)
}

func TestFiveHundredChildFanoutSurvivesConsumerLeaderKill(t *testing.T) {
	if os.Getenv("WF_FANOUT_CONSUMER_500") != "1" {
		t.Skip("set WF_FANOUT_CONSUMER_500=1 for the 500-child consumer-leader proof")
	}
	runFanoutServerFault(t, 500, true)
}

func runFanoutFullServerRestart(t *testing.T, childCount int) {
	runFanoutServerFault(t, childCount, false)
}

func runFanoutServerFault(t *testing.T, childCount int, consumerFault bool) {
	t.Helper()
	cluster, err := testcluster.StartPartitionableProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, 4*time.Second)
		err = provision.Ensure(attempt, js[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	const typ, id, childType = "restartfanout", "parent", "restartchild"
	release := make(chan struct{})
	var once sync.Once
	parentEntered := make(chan struct{})
	handlers := map[string]worker.Handler{
		typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			once.Do(func() { close(parentEntered) })
			promises := make([]wf.Promise, childCount)
			for i := range promises {
				p, err := wf.CallAsync(c, childType, json.RawMessage(fmt.Sprint(i)))
				if err != nil {
					return nil, err
				}
				promises[i] = p
			}
			var sum int
			for _, p := range promises {
				value, err := wf.AwaitPromise(c, p)
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
			value, err := wf.Run(c, "held", 0, func(effectCtx context.Context) (int, error) {
				select {
				case <-release:
					return 7, nil
				case <-effectCtx.Done():
					return 0, effectCtx.Err()
				}
			})
			return json.RawMessage(fmt.Sprint(value)), err
		},
	}
	first, err := worker.New(ctx, js[1], "fanout-before-restart", handlers)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	parentPart := identity.Partition(typ, id, provision.Partitions)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, parentPart) }()
	c := client.New(js[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-parentEntered:
	case <-time.After(20 * time.Second):
		t.Fatal("parent did not begin fan-out")
	}
	j := journal.New(js[0])
	var childIDs []string
	until = time.Now().Add(time.Minute)
	for time.Now().Before(until) {
		records, _, err := j.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
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
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first worker: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not stop before cluster restart")
	}
	var beforeFault []journal.Record
	var queuedBefore map[string]uint64
	if consumerFault {
		beforeFault, _, err = j.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		queuedBefore = fanoutQueuedChildRuns(t, ctx, js[0], childType, childIDs)
		killFanoutBacklogConsumer(t, ctx, cluster, js, parentPart)
	} else {
		for i := range cluster.Commands {
			if err := cluster.KillNode(i); err != nil {
				t.Fatalf("kill node %d: %v", i, err)
			}
		}
		for i := range cluster.Commands {
			if err := cluster.RestartNode(i); err != nil {
				t.Fatalf("restart node %d: %v", i, err)
			}
		}
	}
	js = make([]jetstream.JetStream, 3)
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		_, err = js[0].AccountInfo(attempt)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("cluster metadata after restart: %v", err)
	}
	if consumerFault {
		after, _, err := journal.New(js[0]).Read(ctx, typ, id)
		if err != nil || !reflect.DeepEqual(beforeFault, after) {
			t.Fatalf("parent prefix changed across consumer fault: %v", err)
		}
		queuedAfter := fanoutQueuedChildRuns(t, ctx, js[0], childType, childIDs)
		if !reflect.DeepEqual(queuedBefore, queuedAfter) {
			t.Fatal("queued child run identities/sequences changed across consumer fault")
		}
	}
	var operationMu sync.Mutex
	var operations []worker.OperationEvent
	var successorOptions []worker.Option
	if consumerFault {
		successorOptions = append(successorOptions, worker.WithOperationObserver(func(event worker.OperationEvent) {
			if event.Type == typ && event.ID == id {
				operationMu.Lock()
				operations = append(operations, event)
				operationMu.Unlock()
			}
		}))
		defer func() {
			operationMu.Lock()
			snapshot := append([]worker.OperationEvent(nil), operations...)
			operationMu.Unlock()
			saveFanoutParentOperations(t, snapshot)
			if prefix := os.Getenv("WF_FANOUT_CONSUMER_REPORT"); prefix != "" {
				for _, err := range saveMixedJetStreamDiagnostics(cluster, prefix, "final") {
					t.Errorf("fanout server diagnostics: %v", err)
				}
			}
		}()
	}
	var successor *worker.Worker
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, js[2], "fanout-after-restart", handlers, successorOptions...)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("worker after full server restart: %v (context: %v)", err, ctx.Err())
	}
	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	parts := map[uint32]bool{parentPart: true}
	for _, childID := range childIDs {
		parts[identity.Partition(childType, childID, provision.Partitions)] = true
	}
	for part := range parts {
		go func(part uint32) { _ = successor.RunPartition(workCtx, part) }(part)
	}
	close(release)
	result, err := client.New(js[0]).Await(ctx, typ, id)
	if err != nil || string(result) != fmt.Sprint(childCount*7) {
		t.Fatalf("parent after full restart: result=%s err=%v", result, err)
	}
	seen := map[string]bool{}
	for _, childID := range childIDs {
		if childID == "" || seen[childID] {
			t.Fatalf("duplicate or empty child ID: %q", childID)
		}
		seen[childID] = true
		result, err := client.New(js[1]).Await(ctx, childType, childID)
		if err != nil || string(result) != "7" {
			t.Fatalf("child %s after restart: result=%s err=%v", childID, result, err)
		}
	}
	auditAttemptTimeout := 3 * time.Second
	auditDeadline := 30 * time.Second
	if childCount >= 500 {
		auditAttemptTimeout = time.Minute
		auditDeadline = 2 * time.Minute
	}
	until = time.Now().Add(auditDeadline)
	var report integrity.Report
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, auditAttemptTimeout)
		report, err = integrity.Check(attempt, js[0])
		stop()
		if err == nil && report.Invocations == childCount+1 && report.Journals == childCount+1 && report.Terminal == childCount+1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || report.Invocations != childCount+1 || report.Journals != childCount+1 || report.Terminal != childCount+1 {
		t.Fatalf("full-restart fan-out retained integrity: report=%+v err=%v", report, err)
	}
	if consumerFault {
		assertFanoutFaultLatencies(t, ctx, js, typ, id, childType, childIDs)
		assertFanoutRunDrain(t, ctx, js[0], parts)
	}
	if consumerFault {
		t.Logf("consumer leader kill recovered parent and %d children; retained integrity=%+v", childCount, report)
	} else {
		t.Logf("full server restart recovered parent and %d children; retained integrity=%+v", childCount, report)
	}
}
