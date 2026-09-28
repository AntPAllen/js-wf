package integration_test

import (
	"context"
	"encoding/json"
	"errors"
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

func TestPartitionedWorkerFencedAfterFortyFiveSeconds(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
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
	const typ, id = "fencing", "long-partition"
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	first, err := worker.New(ctx, proxied, "partitioned-first", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		entered <- struct{}{}
		<-release
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	part := identity.Partition(typ, id, provision.Partitions)
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, part) }()
	c := client.New(all[1])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first worker did not enter handler")
	}
	proxy.Block()
	blockedAt := time.Now()
	for nc.IsConnected() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("first worker connection did not isolate")
	}
	second, err := worker.New(ctx, all[2], "surviving-second", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`2`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, part) }()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "2" {
		t.Fatalf("survivor result=%s err=%v", value, err)
	}
	if remaining := 45*time.Second - time.Since(blockedAt); remaining > 0 {
		select {
		case <-time.After(remaining):
		case <-ctx.Done():
			t.Fatal("test ended before 45-second partition elapsed")
		}
	}
	proxy.Heal()
	for !nc.IsConnected() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("first worker did not reconnect")
	}
	releaseOnce.Do(func() { close(release) })
	stopFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopSecond()
	if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	records, _, err := j.Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Kind != journal.Started || records[1].Kind != journal.Completed || records[1].Epoch <= records[0].Epoch {
		t.Fatalf("fenced journal=%+v", records)
	}
	if _, err := j.Append(ctx, typ, id, journal.Entry{Epoch: records[0].Epoch, Index: 1, Kind: journal.Completed, Payload: records[1].Payload, WorkerID: "partitioned-first"}, records[0].Sequence); !errors.Is(err, journal.ErrStale) {
		t.Fatalf("stale worker append bypassed journal CAS: %v", err)
	}
	if first.Metrics().FencingEvents == 0 || second.Metrics().Redeliveries == 0 {
		t.Fatalf("fencing metrics first=%+v second=%+v", first.Metrics(), second.Metrics())
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("integrity: %v", err)
	}
}
