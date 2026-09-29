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
	cluster, err := testcluster.StartPartitionableProcesses(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
			promises := make([]wf.Promise, 6)
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
	until = time.Now().Add(20 * time.Second)
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
		if len(childIDs) == 6 && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if len(childIDs) != 6 {
		t.Fatalf("committed child IDs=%d, want 6", len(childIDs))
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
	var successor *worker.Worker
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, js[2], "fanout-after-restart", handlers)
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
	if err != nil || string(result) != "42" {
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
	until = time.Now().Add(20 * time.Second)
	var report integrity.Report
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		report, err = integrity.Check(attempt, js[0])
		stop()
		if err == nil && report.Invocations == 7 && report.Journals == 7 && report.Terminal == 7 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || report.Invocations != 7 || report.Journals != 7 || report.Terminal != 7 {
		t.Fatalf("full-restart fan-out retained integrity: report=%+v err=%v", report, err)
	}
}
