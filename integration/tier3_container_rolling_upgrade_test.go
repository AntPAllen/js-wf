//go:build linux

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

func TestFiveContainerRollingUpgradeFallback(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	oldBinary := os.Getenv("WF_NATS_SERVER_BIN")
	if oldBinary == "" {
		t.Skip("set WF_NATS_SERVER_BIN to a static NATS 2.11 server binary")
	}
	cluster, err := testcluster.StartMixedVersionDockerCluster(t.TempDir(), 5, oldBinary)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for node := 0; node < 5; node++ {
			logs, err := cluster.Logs(node)
			if err == nil {
				if len(logs) > 8192 {
					logs = logs[len(logs)-8192:]
				}
				t.Logf("node %d log tail:\n%s", node, logs)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	connect := func(node int) (*nats.Conn, jetstream.JetStream) {
		t.Helper()
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			t.Fatal(err)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			t.Fatal(err)
		}
		return nc, js
	}
	all := make([]jetstream.JetStream, 5)
	for node := range all {
		nc, js := connect(node)
		defer nc.Close()
		all[node] = js
	}
	if version := all[0].Conn().ConnectedServerVersion(); !strings.HasPrefix(version, "2.11.") {
		t.Fatalf("old container version=%q", version)
	}
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.EnsureFallback(attempt, all[1], 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("five-replica fallback provision: %v", err)
	}
	if mode, err := provision.EnsureAuto(ctx, all[0], 5); err != nil || mode != provision.FallbackTimers {
		t.Fatalf("old peer fallback mode=%q err=%v", mode, err)
	}
	const typ, firstID = "tier3-rolling-upgrade", "timer"
	partition := identity.Partition(typ, firstID, provision.Partitions)
	var repairedID, afterID string
	for candidate := 0; candidate < 10_000 && afterID == ""; candidate++ {
		id := fmt.Sprintf("same-part-%d", candidate)
		if identity.Partition(typ, id, provision.Partitions) != partition {
			continue
		}
		if repairedID == "" {
			repairedID = id
		} else {
			afterID = id
		}
	}
	if afterID == "" {
		t.Fatal("could not find same-partition IDs")
	}
	w, err := worker.New(ctx, all[3], "tier3-upgrade-worker", map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
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
		loopDone <- reconcile.RunFallbackTimerLoop(loopCtx, all[2], "tier3-upgrade-poller", 100*time.Millisecond, 100)
	}()
	defer func() {
		stopLoop()
		if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	oldClient := client.New(all[0])
	if _, err := oldClient.Start(ctx, typ, firstID, []byte(`null`)); err != nil {
		t.Fatalf("start through old container: %v", err)
	}
	if value, err := client.New(all[4]).Await(ctx, typ, firstID); err != nil || string(value) != `"done"` {
		t.Fatalf("first result=%s err=%v", value, err)
	}
	payload := []byte(`null`)
	digest := sha256.Sum256(payload)
	msg := &nats.Msg{Subject: identity.InvocationSubject(typ, repairedID), Data: payload, Header: nats.Header{}}
	msg.Header.Set("Wf-Input-SHA256", hex.EncodeToString(digest[:]))
	ack, err := all[0].PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("retain invocation without run: %v", err)
	}
	if result, err := reconcile.NewStartScan(all[1]).Scan(ctx, ack.Sequence, 1, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("repair from new container: result=%+v err=%v", result, err)
	}
	if value, err := client.New(all[4]).Await(ctx, typ, repairedID); err != nil || string(value) != `"done"` {
		t.Fatalf("repaired result=%s err=%v", value, err)
	}
	if retry, err := oldClient.Start(ctx, typ, repairedID, payload); !errors.Is(err, client.ErrAlreadyStarted) || retry.InvSeq != ack.Sequence {
		t.Fatalf("old-container retry: handle=%+v err=%v want=%d", retry, err, ack.Sequence)
	}
	if err := cluster.UpgradeNode(0); err != nil {
		t.Fatalf("upgrade old container: %v", err)
	}
	if _, err := waitMixedVersionReplicaCatchup(ctx, all[1], 5); err != nil {
		t.Fatalf("five-replica catch-up: %v", err)
	}
	upgradedConn, upgraded := connect(0)
	defer upgradedConn.Close()
	if version := upgradedConn.ConnectedServerVersion(); strings.HasPrefix(version, "2.11.") {
		t.Fatalf("upgraded container version=%q", version)
	}
	if mode, err := provision.EnsureAuto(ctx, upgraded, 5); err != nil || mode != provision.FallbackTimers {
		t.Fatalf("upgraded peer fallback mode=%q err=%v", mode, err)
	}
	upgradedClient := client.New(upgraded)
	for _, id := range []string{firstID, repairedID} {
		readCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		value, err := upgradedClient.Await(readCtx, typ, id)
		stop()
		if err != nil || string(value) != `"done"` {
			t.Fatalf("retained result %s=%s err=%v probe=%s", id, value, err, mixedVersionResultProbe(ctx, upgraded, typ, id))
		}
	}
	if _, err := upgradedClient.Start(ctx, typ, afterID, payload); err != nil {
		t.Fatalf("post-upgrade start: %v", err)
	}
	if value, err := upgradedClient.Await(ctx, typ, afterID); err != nil || string(value) != `"done"` {
		t.Fatalf("post-upgrade result=%s err=%v", value, err)
	}
	if report, err := integrity.Check(ctx, upgraded); err != nil || report.Invocations != 3 || report.Journals != 3 || report.Terminal != 3 {
		t.Fatalf("five-container upgrade audit: report=%+v err=%v", report, err)
	}
}
