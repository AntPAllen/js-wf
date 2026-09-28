package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestClientProxyPartitionAndHeal(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
	stream, err := all[1].CreateStream(ctx, jetstream.StreamConfig{Name: "PROXY_TEST", Subjects: []string{"proxy.test"}, Storage: jetstream.FileStorage, Replicas: 3, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	first, err := proxied.Publish(ctx, "proxy.test", []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	proxy.Block()
	for nc.IsConnected() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if nc.IsConnected() {
		t.Fatal("proxied client remained connected during partition")
	}
	probe, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	_, err = proxied.Publish(probe, "proxy.test", []byte("isolated"))
	stop()
	if err == nil {
		t.Fatal("isolated client received a publish ack")
	}
	majority, err := all[2].Publish(ctx, "proxy.test", []byte("majority"))
	if err != nil {
		t.Fatalf("surviving cluster publish: %v", err)
	}
	proxy.Heal()
	for !nc.IsConnected() && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if !nc.IsConnected() {
		t.Fatal("proxied client did not reconnect after heal")
	}
	after, err := proxied.Publish(ctx, "proxy.test", []byte("after"))
	if err != nil {
		t.Fatalf("publish after heal: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs < 3 || info.State.FirstSeq != 1 {
		t.Fatalf("stream after partition=%+v err=%v", info.State, err)
	}
	for seq, want := range map[uint64]string{first.Sequence: "before", majority.Sequence: "majority", after.Sequence: "after"} {
		message, err := stream.GetMsg(ctx, seq)
		if err != nil || string(message.Data) != want {
			t.Fatalf("sequence %d: message=%+v err=%v, want %q", seq, message, err, want)
		}
	}
}

func TestWorkerClientPartitionResumesAfterHeal(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	const typ, id = "test", "partitioned-worker"
	w, err := worker.New(ctx, proxied, "partitioned-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		return wf.AwaitSignal(c, "go")
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[1])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	j := journal.New(all[2])
	for ctx.Err() == nil {
		records, _, err := j.Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("workflow did not suspend")
	}
	// The suspended journal entry precedes the first run-message ack. Wait
	// for that ack so this case isolates an idle worker, not an in-flight ack.
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("worker did not ack the suspended run")
	}
	proxy.Block()
	if _, err := c.Signal(ctx, typ, id, "go", []byte(`42`), "during-partition"); err != nil {
		t.Fatal(err)
	}
	probe, stopProbe := context.WithTimeout(ctx, 350*time.Millisecond)
	_, err = c.Await(probe, typ, id)
	stopProbe()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("workflow completed while worker was isolated: %v", err)
	}
	proxy.Heal()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "42" {
		var workerErr error
		workerExited := false
		select {
		case workerErr = <-done:
			workerExited = true
		default:
		}
		inspectCtx, stopInspect := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopInspect()
		records, _, readErr := j.Read(inspectCtx, typ, id)
		run, runErr := all[1].Stream(inspectCtx, "WF_RUN")
		var state any
		var consumerState any
		if runErr == nil {
			info, infoErr := run.Info(inspectCtx)
			if infoErr == nil {
				state = info.State
			} else {
				runErr = infoErr
			}
			consumer, consumerErr := run.Consumer(inspectCtx, fmt.Sprintf("WF_P_%02d", identity.Partition(typ, id, provision.Partitions)))
			if consumerErr == nil {
				info, infoErr := consumer.Info(inspectCtx)
				if infoErr == nil {
					consumerState = info
				} else {
					consumerState = infoErr
				}
			} else {
				consumerState = consumerErr
			}
		}
		t.Fatalf("result after heal=%s err=%v workerExited=%v worker=%v conn=%v records=%+v readErr=%v run=%+v consumer=%+v runErr=%v", value, err, workerExited, workerErr, nc.Status(), records, readErr, state, consumerState, runErr)
	}
	stopWorker()
	if err := <-done; err != nil {
		t.Fatalf("worker loop after partition: %v", err)
	}
}
