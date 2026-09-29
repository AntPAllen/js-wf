//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestThousandShortWorkflowsSurviveFullServerRestart(t *testing.T) {
	if os.Getenv("WF_FULL_RESTART_SHORT") != "1" {
		t.Skip("set WF_FULL_RESTART_SHORT=1 for the 1,000-workflow process restart proof")
	}
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
	const typ, count = "restartshort", 1000
	release := make(chan struct{})
	var entered atomic.Int64
	handler := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var index int
		if err := json.Unmarshal(raw, &index); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "held", index, func(effectCtx context.Context) (int, error) {
			entered.Add(1)
			select {
			case <-release:
				return index * 2, nil
			case <-effectCtx.Done():
				return 0, effectCtx.Err()
			}
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	first, err := worker.New(ctx, js[1], "short-before-restart", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunAssigned(firstCtx, 0, 1) }()
	clients := [3]*client.Client{client.New(js[0]), client.New(js[1]), client.New(js[2])}
	jobs := make(chan int, count)
	startErrors := make(chan error, 1)
	var starters sync.WaitGroup
	for caller := 0; caller < 32; caller++ {
		starters.Add(1)
		go func(caller int) {
			defer starters.Done()
			for index := range jobs {
				id := fmt.Sprintf("job-%04d", index)
				if _, err := clients[caller%len(clients)].Start(ctx, typ, id, []byte(fmt.Sprint(index))); err != nil {
					select {
					case startErrors <- fmt.Errorf("start %s: %w", id, err):
					default:
					}
					return
				}
			}
		}(caller)
	}
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	starters.Wait()
	select {
	case err := <-startErrors:
		t.Fatal(err)
	default:
	}
	until = time.Now().Add(30 * time.Second)
	for entered.Load() < 32 && time.Now().Before(until) && ctx.Err() == nil {
		select {
		case err := <-firstDone:
			t.Fatalf("first worker exited before restart: %v", err)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if entered.Load() < 32 {
		t.Fatalf("only %d handlers entered before restart", entered.Load())
	}
	inFlightAtKill := entered.Load()
	for i := range cluster.Commands {
		if err := cluster.KillNode(i); err != nil {
			t.Fatalf("kill node %d: %v", i, err)
		}
	}
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Fatalf("first worker: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("first worker did not stop after cluster kill")
	}
	for i := range cluster.Commands {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until = time.Now().Add(30 * time.Second)
	var retained uint64
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		inv, lookupErr := js[2].Stream(attempt, "WF_INV")
		if lookupErr == nil {
			var info *jetstream.StreamInfo
			info, lookupErr = inv.Info(attempt)
			if lookupErr == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2 {
				retained = info.State.Msgs
			}
		}
		stop()
		if retained == count {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if retained != count {
		t.Fatalf("retained invocation count after full restart=%d, want %d (context=%v)", retained, count, ctx.Err())
	}
	var successor *worker.Worker
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, js[2], "short-after-restart", map[string]worker.Handler{typ: handler})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("successor worker: %v (context=%v)", err, ctx.Err())
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- successor.RunAssigned(workerCtx, 0, 1) }()
	close(release)
	resultJobs := make(chan int, count)
	resultErrors := make(chan error, 1)
	var readers sync.WaitGroup
	for reader := 0; reader < 32; reader++ {
		readers.Add(1)
		go func(reader int) {
			defer readers.Done()
			c := client.New(js[reader%len(js)])
			for index := range resultJobs {
				id := fmt.Sprintf("job-%04d", index)
				result, err := c.Await(ctx, typ, id)
				if err != nil || string(result) != fmt.Sprint(index*2) {
					select {
					case resultErrors <- fmt.Errorf("result %s=%s: %v", id, result, err):
					default:
					}
					return
				}
			}
		}(reader)
	}
	for i := 0; i < count; i++ {
		resultJobs <- i
	}
	close(resultJobs)
	readers.Wait()
	select {
	case err := <-resultErrors:
		t.Fatal(err)
	default:
	}
	until = time.Now().Add(30 * time.Second)
	var pending uint64
	drained := false
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		run, lookupErr := js[0].Stream(attempt, "WF_RUN")
		if lookupErr == nil {
			var info *jetstream.StreamInfo
			info, lookupErr = run.Info(attempt)
			if lookupErr == nil {
				pending = info.State.Msgs
			}
		}
		stop()
		if lookupErr == nil && pending == 0 {
			drained = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !drained {
		t.Fatalf("run queue retained %d messages (context=%v)", pending, ctx.Err())
	}
	stopWorker()
	if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("successor worker: %v", err)
	}
	report, err := integrity.Check(ctx, js[1])
	if err != nil || report.Invocations != count || report.Journals != count || report.Entries != 4*count || report.Terminal != count {
		t.Fatalf("full restart integrity=%+v err=%v", report, err)
	}
	t.Logf("full server restart recovered %d short workflows after %d handlers entered before kill; retained integrity=%+v", count, inFlightAtKill, report)
}
