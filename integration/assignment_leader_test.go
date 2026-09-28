package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestAssignmentWatcherSurvivesKVLeaderKill(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	stream, err := all[0].Stream(ctx, "KV_WF_ASSIGN")
	if err != nil {
		t.Fatal(err)
	}
	waitReady := func(wantLeader string) *jetstream.StreamInfo {
		until := time.Now().Add(15 * time.Second)
		for {
			attempt, done := context.WithTimeout(ctx, time.Second)
			info, infoErr := stream.Info(attempt)
			done()
			ready := infoErr == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2
			if ready && wantLeader != "" {
				ready = info.Cluster.Leader == wantLeader
			}
			if ready {
				for _, replica := range info.Cluster.Replicas {
					ready = ready && replica.Current && !replica.Offline
				}
			}
			for node, server := range cluster.Servers {
				routes, routeErr := server.Routez(nil)
				if routeErr != nil {
					ready = false
					continue
				}
				peers := map[string]bool{}
				for _, route := range routes.Routes {
					peers[route.RemoteName] = true
				}
				for other, peer := range cluster.Servers {
					if other != node {
						ready = ready && peers[peer.Name()]
					}
				}
			}
			if ready {
				return info
			}
			if time.Now().After(until) || ctx.Err() != nil {
				t.Fatalf("assignment replicas not ready: info=%+v err=%v routes=%d/%d/%d", info, infoErr, cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	info := waitReady("")
	leader := -1
	for index, server := range cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			leader = index
			break
		}
	}
	if leader < 0 {
		t.Fatalf("unknown assignment leader %q", info.Cluster.Leader)
	}
	firstNode, secondNode := (leader+1)%3, (leader+2)%3
	stream, err = all[firstNode].Stream(ctx, "KV_WF_ASSIGN")
	if err != nil {
		t.Fatal(err)
	}
	const typ = "assignleader"
	id := "warmup"
	partition := identity.Partition(typ, id, provision.Partitions)
	assignments, err := assignment.New(ctx, all[firstNode])
	if err != nil {
		t.Fatal(err)
	}
	revision, err := assignments.Assign(ctx, partition, "leader-first", 0)
	if err != nil {
		t.Fatal(err)
	}
	var firstCalls atomic.Int64
	first, err := worker.New(ctx, all[firstNode], "leader-first", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		firstCalls.Add(1)
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := worker.New(ctx, all[secondNode], "leader-second", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`2`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { firstDone <- first.RunKVAssignments(workerCtx) }()
	go func() { secondDone <- second.RunKVAssignments(workerCtx) }()
	defer func() {
		stop()
		if err := <-firstDone; err != nil {
			t.Error(err)
		}
		if err := <-secondDone; err != nil {
			t.Error(err)
		}
	}()
	c := client.New(all[firstNode])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, typ, id); err != nil || string(value) != "1" {
		t.Fatalf("warmup result=%s err=%v", value, err)
	}
	waitReady(info.Cluster.Leader)
	cluster.KillNode(leader)
	electedUntil := time.Now().Add(20 * time.Second)
	for {
		attempt, done := context.WithTimeout(ctx, time.Second)
		current, err := stream.Info(attempt)
		done()
		if err == nil && current.Cluster != nil && current.Cluster.Leader != "" && current.Cluster.Leader != info.Cluster.Leader {
			break
		}
		if ctx.Err() != nil || time.Now().After(electedUntil) {
			t.Fatalf("assignment leader did not change: info=%+v err=%v routes=%d/%d/%d", current, err, cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := assignments.Assign(ctx, partition, "leader-second", revision); err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		t.Fatal("assignment watch did not recover in time")
	}
	var followups []string
	for candidate := 0; len(followups) < 40; candidate++ {
		candidateID := fmt.Sprintf("followup-%04d", candidate)
		if identity.Partition(typ, candidateID, provision.Partitions) == partition {
			followups = append(followups, candidateID)
		}
	}
	for _, followup := range followups {
		if _, err := c.Start(ctx, typ, followup, []byte(`null`)); err != nil {
			t.Fatalf("start %s: %v", followup, err)
		}
	}
	for _, followup := range followups {
		value, err := c.Await(ctx, typ, followup)
		if err != nil || string(value) != "2" {
			t.Fatalf("followup %s result=%s err=%v", followup, value, err)
		}
	}
	if firstCalls.Load() != 1 {
		t.Fatalf("old owner handled %d invocations", firstCalls.Load())
	}
	if report, err := integrity.Check(ctx, all[secondNode]); err != nil || report.Invocations != len(followups)+1 || report.Terminal != len(followups)+1 {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
}
