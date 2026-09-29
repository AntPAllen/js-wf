package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/reconcile"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type countedStartJS struct {
	jetstream.JetStream
	entered *atomic.Int64
}

func (c *countedStartJS) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if strings.HasPrefix(msg.Subject, "wf.inv.") {
		c.entered.Add(1)
	}
	return c.JetStream.PublishMsg(ctx, msg, opts...)
}

func TestConcurrentStartDuringInvocationLeaderKill(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	leader := -1
	for i, server := range cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatalf("unknown invocation leader %q", info.Cluster.Leader)
	}
	var entered, completed atomic.Int64
	recorder := &history.Recorder{}
	clients := make([]*client.Client, 3)
	for i := range clients {
		urls := make([]string, 0, 3)
		for offset := 0; offset < 3; offset++ {
			urls = append(urls, cluster.Servers[(i+offset)%3].ClientURL())
		}
		nc, err := nats.Connect(strings.Join(urls, ","), nats.DontRandomize(), nats.ReconnectWait(25*time.Millisecond), nats.MaxReconnects(-1))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = client.NewObserved(&countedStartJS{JetStream: js, entered: &entered}, recorder)
	}
	type outcome struct {
		handle client.Handle
		err    error
	}
	results := make(chan outcome, 500)
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			h, err := clients[i%3].Start(ctx, "leaderkill", "same", []byte(`true`))
			completed.Add(1)
			results <- outcome{h, err}
		}(i)
	}
	cut := make(chan int64, 1)
	go func() {
		for ctx.Err() == nil {
			if entered.Load() >= 100 {
				cut <- completed.Load()
				cluster.KillNode(leader)
				return
			}
			time.Sleep(time.Millisecond)
		}
		cut <- -1
	}()
	close(gate)
	wg.Wait()
	close(results)
	completedAtCut := <-cut
	if completedAtCut < 0 || completedAtCut >= 500 {
		t.Fatalf("leader kill missed the burst: completed=%d", completedAtCut)
	}
	counts := map[string]int{}
	var winner uint64
	for result := range results {
		switch {
		case result.err == nil:
			counts["started"]++
		case errors.Is(result.err, client.ErrAlreadyStarted):
			counts["already"]++
		case errors.Is(result.err, client.ErrEnqueueUnknown):
			counts["enqueue_unknown"]++
		default:
			counts[fmt.Sprintf("%T: %v", result.err, result.err)]++
		}
		if result.handle.InvSeq != 0 {
			if winner != 0 && result.handle.InvSeq != winner {
				t.Fatalf("multiple invocation sequences: %d and %d", winner, result.handle.InvSeq)
			}
			winner = result.handle.InvSeq
		}
	}
	// The winning publish can commit before the leader dies while its ack is
	// lost. In that case even its caller reads the retained row and reports
	// AlreadyStarted. The stream audit below proves that one start committed.
	confirmed := counts["started"] + counts["enqueue_unknown"]
	if confirmed > 1 || confirmed+counts["already"] != 500 {
		t.Fatalf("start outcomes after leader kill: cut_at=%d counts=%v", completedAtCut, counts)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), 45*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("leader-kill start history=%s err=%v", result, err)
	}
	survivor := (leader + 1) % 3
	recoveryCtx, recoveryCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer recoveryCancel()
	// A committed invocation can outlive the burst deadline before its run
	// enqueue is confirmed. Repair that cross-stream gap through the normal
	// reconciler, then audit both retained streams.
	if _, err := reconcile.NewStartScan(all[survivor]).Scan(recoveryCtx, 1, 10, false); err != nil {
		t.Fatalf("repair start after leader kill: %v", err)
	}
	inv, err = all[survivor].Stream(recoveryCtx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err = inv.Info(recoveryCtx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("invocation stream: info=%+v err=%v", info, err)
	}
	run, err := all[survivor].Stream(recoveryCtx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	runInfo, err := run.Info(recoveryCtx)
	if err != nil || runInfo.State.Msgs != 1 {
		t.Fatalf("run stream: info=%+v err=%v", runInfo, err)
	}
}
