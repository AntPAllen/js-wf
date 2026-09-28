package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// A blocked handler on one partition leaves a large queued backlog. A worker
// assigned to another partition must still complete promptly.
func TestHotPartitionDoesNotDelayOtherPartition(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const hotType, coldType = "hot", "cold"
	hotPartition := identity.Partition(hotType, "job-0", provision.Partitions)
	var hotIDs []string
	for candidate := 0; len(hotIDs) < 200; candidate++ {
		id := fmt.Sprintf("job-%d", candidate)
		if identity.Partition(hotType, id, provision.Partitions) == hotPartition {
			hotIDs = append(hotIDs, id)
		}
	}
	coldID := ""
	for candidate := 0; coldID == ""; candidate++ {
		id := fmt.Sprintf("job-%d", candidate)
		if identity.Partition(coldType, id, provision.Partitions) != hotPartition {
			coldID = id
		}
	}
	coldPartition := identity.Partition(coldType, coldID, provision.Partitions)
	starters := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	for index, id := range hotIDs {
		if _, err := starters[index%len(starters)].Start(ctx, hotType, id, []byte(`null`)); err != nil {
			t.Fatalf("queue hot invocation %d: %v", index, err)
		}
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	state, err := run.Info(ctx)
	if err != nil || state.State.Msgs != uint64(len(hotIDs)) {
		t.Fatalf("hot backlog: info=%+v err=%v", state, err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	hotWorker, err := worker.New(ctx, all[1], "hot-worker", map[string]worker.Handler{hotType: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	coldWorker, err := worker.New(ctx, all[2], "cold-worker", map[string]worker.Handler{coldType: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`2`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	hotDone := make(chan error, 1)
	coldDone := make(chan error, 1)
	go func() { hotDone <- hotWorker.RunPartition(workerCtx, hotPartition) }()
	select {
	case <-entered:
	case err := <-hotDone:
		t.Fatalf("hot worker exited before handler: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	go func() { coldDone <- coldWorker.RunPartition(workerCtx, coldPartition) }()
	coldCtx, coldCancel := context.WithTimeout(ctx, 5*time.Second)
	defer coldCancel()
	start := time.Now()
	if _, err := starters[0].Start(coldCtx, coldType, coldID, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	result, err := starters[0].Await(coldCtx, coldType, coldID)
	latency := time.Since(start)
	if err != nil || string(result) != "2" {
		t.Fatalf("cold partition result=%s latency=%s err=%v", result, latency, err)
	}
	state, err = run.Info(ctx)
	if err != nil || state.State.Msgs < uint64(len(hotIDs)) {
		t.Fatalf("hot backlog disappeared during cold completion: info=%+v err=%v", state, err)
	}
	t.Logf("cold partition completed in %s while %d hot invocations were queued and the hot handler was blocked", latency, len(hotIDs))
	close(release)
	stop()
	select {
	case err := <-hotDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hot worker did not stop")
	}
	select {
	case err := <-coldDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cold worker did not stop")
	}
}
