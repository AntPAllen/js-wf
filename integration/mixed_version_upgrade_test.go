//go:build !windows

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A rolling cluster must keep the two-write Start and fallback timer path
// available while one peer still runs NATS 2.11.
func TestMixedVersionRollingUpgradeFallback(t *testing.T) {
	oldBinary := os.Getenv("WF_NATS_SERVER_BIN")
	if oldBinary == "" {
		t.Skip("set WF_NATS_SERVER_BIN to a NATS 2.11 server binary")
	}
	for _, test := range []struct {
		name     string
		newFirst bool
	}{
		{name: "old-peer-first"},
		{name: "explicit-fallback-on-new-peer", newFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runMixedVersionRollingUpgradeFallback(t, oldBinary, test.newFirst)
		})
	}
}

func runMixedVersionRollingUpgradeFallback(t *testing.T, oldBinary string, newFirst bool) {
	t.Helper()
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
		if newFirst {
			err = provision.EnsureFallback(attempt, all[1], 3)
			backend = provision.FallbackTimers
		} else {
			backend, err = provision.EnsureAuto(attempt, all[0], 3)
		}
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || backend != provision.FallbackTimers {
		t.Fatalf("mixed-version fallback provision: new_first=%t backend=%q err=%v", newFirst, backend, err)
	}
	if backend, err := provision.EnsureAuto(ctx, all[0], 3); err != nil || backend != provision.FallbackTimers {
		t.Fatalf("old peer changed fallback mode: backend=%q err=%v", backend, err)
	}
	if backend, err := provision.EnsureAuto(ctx, all[1], 3); err != nil || backend != provision.FallbackTimers {
		t.Fatalf("new peer changed fallback mode: backend=%q err=%v", backend, err)
	}
	const typ, id = "mixed-upgrade", "timer"
	partition := identity.Partition(typ, id, provision.Partitions)
	repairedID := ""
	for candidate := 0; candidate < 10_000; candidate++ {
		value := fmt.Sprintf("repaired-%d", candidate)
		if identity.Partition(typ, value, provision.Partitions) == partition {
			repairedID = value
			break
		}
	}
	if repairedID == "" {
		t.Fatal("no second ID on the same partition")
	}
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
	go func() { workDone <- w.RunPartition(workCtx, partition) }()
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
	payload := []byte(`null`)
	digest := sha256.Sum256(payload)
	invocation := &nats.Msg{Subject: identity.InvocationSubject(typ, repairedID), Data: payload, Header: nats.Header{}}
	invocation.Header.Set("Wf-Input-SHA256", hex.EncodeToString(digest[:]))
	ack, err := all[0].PublishMsg(ctx, invocation)
	if err != nil {
		t.Fatalf("retain invocation without run through old peer: %v", err)
	}
	scan := reconcile.NewStartScan(all[1])
	if dry, err := scan.Scan(ctx, ack.Sequence, 1, true); err != nil || dry.Reenqueued != 1 {
		t.Fatalf("mixed-version dry start repair: result=%+v err=%v", dry, err)
	}
	if repaired, err := scan.Scan(ctx, ack.Sequence, 1, false); err != nil || repaired.Reenqueued != 1 {
		t.Fatalf("mixed-version start repair: result=%+v err=%v", repaired, err)
	}
	result, err = client.New(all[2]).Await(ctx, typ, repairedID)
	if err != nil || string(result) != `"done"` {
		t.Fatalf("repaired result through new peer=%s err=%v", result, err)
	}
	if retry, err := c.Start(ctx, typ, repairedID, payload); !errors.Is(err, client.ErrAlreadyStarted) || retry.InvSeq != ack.Sequence {
		t.Fatalf("repaired invocation matching retry: handle=%+v err=%v want seq=%d", retry, err, ack.Sequence)
	}
	if report, err := integrity.Check(ctx, all[1]); err != nil || report.Invocations != 2 || report.Journals != 2 || report.Terminal != 2 {
		t.Fatalf("mixed-version retained audit: report=%+v err=%v", report, err)
	}
}
