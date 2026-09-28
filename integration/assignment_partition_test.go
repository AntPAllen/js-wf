package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/assignment"
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

func TestAssignmentHandoffWhileOldOwnerIsNetworkIsolated(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "kvhandoff", "isolated"
	partition := identity.Partition(typ, id, provision.Partitions)
	assignments, err := assignment.New(ctx, all[1])
	if err != nil {
		t.Fatal(err)
	}
	revision, err := assignments.Assign(ctx, partition, "old-owner", 0)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var oldCalls atomic.Int64
	old, err := worker.New(ctx, proxied, "old-owner", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		oldCalls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	newOwner, err := worker.New(ctx, all[2], "new-owner", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`2`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	oldCtx, stopOld := context.WithCancel(ctx)
	newCtx, stopNew := context.WithCancel(ctx)
	oldDone, newDone := make(chan error, 1), make(chan error, 1)
	oldFinished, newFinished := false, false
	go func() { oldDone <- old.RunKVAssignments(oldCtx) }()
	go func() { newDone <- newOwner.RunKVAssignments(newCtx) }()
	defer func() {
		proxy.Heal()
		releaseOnce.Do(func() { close(release) })
		stopOld()
		stopNew()
		if !oldFinished {
			if err := <-oldDone; err != nil {
				t.Error(err)
			}
		}
		if !newFinished {
			if err := <-newDone; err != nil {
				t.Error(err)
			}
		}
	}()
	c := client.New(all[1])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("old owner did not enter handler")
	}
	proxy.Block()
	for nc.IsConnected() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("old owner did not disconnect")
	}
	if _, err := assignments.Assign(ctx, partition, "new-owner", revision); err != nil {
		t.Fatal(err)
	}
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "2" {
		t.Fatalf("new owner result=%s err=%v", value, err)
	}
	proxy.Heal()
	for !nc.IsConnected() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("old owner did not reconnect")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		t.Fatal("old owner did not recover in time")
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
		result, err := c.Await(ctx, typ, followup)
		if err != nil || string(result) != "2" {
			t.Fatalf("followup %s result=%s err=%v", followup, result, err)
		}
	}
	if oldCalls.Load() != 1 {
		t.Fatalf("old owner handled %d invocations after reassignment", oldCalls.Load())
	}
	stopOld()
	stopNew()
	oldErr := <-oldDone
	oldFinished = true
	if err := oldErr; err != nil {
		t.Fatalf("old worker after reconnect: %v", err)
	}
	newErr := <-newDone
	newFinished = true
	if err := newErr; err != nil {
		t.Fatalf("new worker: %v", err)
	}
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Kind != journal.Started || records[1].Kind != journal.Completed || records[1].Epoch <= records[0].Epoch {
		t.Fatalf("handoff journal=%+v", records)
	}
	if report, err := integrity.Check(ctx, all[0]); err != nil || report.Invocations != len(followups)+1 || report.Terminal != len(followups)+1 {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
}
