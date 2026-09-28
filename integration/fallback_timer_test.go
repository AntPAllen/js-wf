package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

func TestAutoTimerBackendNative(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var backend provision.TimerBackend
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 3*time.Second)
		backend, err = provision.EnsureAuto(attempt, js, 3)
		done()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || backend != provision.NativeTimers {
		t.Fatalf("automatic native provision: backend=%q err=%v", backend, err)
	}
	if backend, err := provision.EnsureAuto(ctx, js, 3); err != nil || backend != provision.NativeTimers {
		t.Fatalf("automatic native reprovision: backend=%q err=%v", backend, err)
	}
	if _, err := js.Stream(ctx, "WF_TIMER"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("native mode created fallback timer stream: %v", err)
	}
}

func TestFallbackTimerSurvivesPollerDowntime(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	all := make([]jetstream.JetStream, 3)
	for i, nc := range cluster.Clients {
		all[i], err = jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		attempt, done := context.WithTimeout(ctx, 2*time.Second)
		_, readyErr := all[0].AccountInfo(attempt)
		done()
		if readyErr == nil {
			attempt, done = context.WithTimeout(ctx, 3*time.Second)
			err = provision.EnsureFallback(attempt, all[0], 3)
			done()
			if err == nil {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("fallback provision: %v (last error: %v)", ctx.Err(), err)
	}
	if err := provision.EnsureFallback(ctx, all[1], 3); err != nil {
		t.Fatalf("fallback reprovision: %v", err)
	}
	if backend, err := provision.EnsureAuto(ctx, all[1], 3); err != nil || backend != provision.FallbackTimers {
		t.Fatalf("automatic fallback reprovision: backend=%q err=%v", backend, err)
	}
	if err := provision.Ensure(ctx, all[2], 3); err == nil {
		t.Fatal("native provisioning silently adopted fallback WF_RUN")
	}
	const typ, id = "fallback", "timer"
	w, err := worker.New(ctx, all[1], "fallback-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "wait", time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(all[0])
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
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
		t.Fatal("fallback workflow did not suspend")
	}
	timers, err := all[2].Stream(ctx, "WF_TIMER")
	if err != nil {
		t.Fatal(err)
	}
	timerSubject := identity.TimerSubject(typ, id, handle.InvSeq, 0)
	if _, err := timers.GetLastMsgForSubject(ctx, timerSubject); err != nil {
		t.Fatalf("retained fallback timer: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	stallCtx, stopStall := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stopStall()
	if _, err := c.Await(stallCtx, typ, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timer completed without poller: %v", err)
	}
	if _, err := timers.GetLastMsgForSubject(ctx, timerSubject); err != nil {
		t.Fatalf("timer expired while poller was down: %v", err)
	}
	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunFallbackTimerLoop(loopCtx, all[2], "fallback-poller", 100*time.Millisecond, 100)
	}()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "true" {
		t.Fatalf("fallback result=%s err=%v", value, err)
	}
	stopLoop()
	if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if _, err := timers.GetLastMsgForSubject(ctx, timerSubject); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("fired fallback timer retained: %v", err)
	}
	if m := w.Metrics(); m.TimersScheduled != 1 || m.TimersFired != 1 {
		t.Fatalf("fallback timer metrics: %+v", m)
	}
}
