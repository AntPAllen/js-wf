//go:build !windows

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// A rolling cluster must keep the two-write Start and fallback timer path
// available while one peer still runs NATS 2.11.
func TestMixedVersionRollingUpgradeFallback(t *testing.T) {
	oldBinary := os.Getenv("WF_NATS_SERVER_BIN")
	if oldBinary == "" {
		t.Skip("set WF_NATS_SERVER_BIN to a NATS 2.11 server binary")
	}
	cluster, err := testcluster.StartMixedVersionProcesses(t.TempDir(), []string{oldBinary, "", ""})
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
	if version := cluster.Clients[0].ConnectedServerVersion(); !strings.HasPrefix(version, "2.11.") {
		t.Fatalf("old peer version=%q, want 2.11", version)
	}
	if version := cluster.Clients[1].ConnectedServerVersion(); strings.HasPrefix(version, "2.11.") {
		t.Fatalf("new peer version=%q, want 2.12+", version)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	var backend provision.TimerBackend
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		backend, err = provision.EnsureAuto(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || backend != provision.FallbackTimers {
		t.Fatalf("old peer fallback provision: backend=%q err=%v", backend, err)
	}
	if backend, err := provision.EnsureAuto(ctx, all[1], 3); err != nil || backend != provision.FallbackTimers {
		t.Fatalf("new peer changed fallback mode: backend=%q err=%v", backend, err)
	}
	const typ, id = "mixed-upgrade", "timer"
	w, err := worker.New(ctx, all[2], "upgrade-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "wait", time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`"done"`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	go func() { workDone <- w.RunPartition(workCtx, identity.Partition(typ, id, provision.Partitions)) }()
	defer func() { stopWork(); <-workDone }()
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- reconcile.RunFallbackTimerLoop(loopCtx, all[1], "upgrade-poller", 100*time.Millisecond, 100)
	}()
	defer func() {
		stopLoop()
		if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatalf("start through old peer: %v", err)
	}
	result, err := client.New(all[2]).Await(ctx, typ, id)
	if err != nil || string(result) != `"done"` {
		t.Fatalf("result through new peer=%s err=%v", result, err)
	}
	if report, err := integrity.Check(ctx, all[1]); err != nil || report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 {
		t.Fatalf("mixed-version retained audit: report=%+v err=%v", report, err)
	}
}
