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
	defer func() {
		if !t.Failed() {
			return
		}
		for node := 0; node < 3; node++ {
			logs, err := os.ReadFile(cluster.LogPath(node))
			if err != nil {
				t.Logf("node %d log read: %v", node, err)
				continue
			}
			if len(logs) > 8192 {
				logs = logs[len(logs)-8192:]
			}
			t.Logf("node %d log tail:\n%s", node, logs)
		}
	}()
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	matchingIDs := make([]string, 0, 2)
	for candidate := 0; candidate < 10_000 && len(matchingIDs) < 2; candidate++ {
		value := fmt.Sprintf("repaired-%d", candidate)
		if identity.Partition(typ, value, provision.Partitions) == partition {
			matchingIDs = append(matchingIDs, value)
		}
	}
	if len(matchingIDs) != 2 {
		t.Fatal("could not find two more IDs on the same partition")
	}
	repairedID, postUpgradeID := matchingIDs[0], matchingIDs[1]
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
	invocations, err := all[1].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	retained, err := invocations.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, repairedID))
	if err != nil || retained.Sequence != ack.Sequence {
		t.Fatalf("matching retry changed invocation generation: retained=%+v err=%v want seq=%d", retained, err, ack.Sequence)
	}
	if report, err := integrity.Check(ctx, all[1]); err != nil || report.Invocations != 2 || report.Journals != 2 || report.Terminal != 2 {
		t.Fatalf("mixed-version retained audit: report=%+v err=%v", report, err)
	}
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("stop old peer for upgrade: %v", err)
	}
	if err := cluster.UpgradeNode(0); err != nil {
		t.Fatalf("restart old peer on pinned binary: %v", err)
	}
	readyAt, err := waitMixedVersionReplicaCatchup(ctx, all[1])
	if err != nil {
		t.Fatalf("upgraded replica catch-up: %v", err)
	}
	if version := cluster.Clients[0].ConnectedServerVersion(); strings.HasPrefix(version, "2.11.") {
		t.Fatalf("upgraded peer still reports %q", version)
	}
	upgraded, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	var upgradedBackend provision.TimerBackend
	for until := time.Now().Add(15 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		upgradedBackend, err = provision.EnsureAuto(attempt, upgraded, 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || upgradedBackend != provision.FallbackTimers {
		t.Fatalf("upgraded peer changed fallback mode: backend=%q err=%v", upgradedBackend, err)
	}
	upgradedClient := client.New(upgraded)
	for _, completedID := range []string{id, repairedID} {
		t.Logf("pre-read %s: upgraded=%s survivor=%s", completedID, mixedVersionResultProbe(ctx, upgraded, typ, completedID), mixedVersionResultProbe(ctx, all[1], typ, completedID))
		readCtx, stopRead := context.WithTimeout(ctx, 30*time.Second)
		value, err := upgradedClient.Await(readCtx, typ, completedID)
		stopRead()
		if err != nil || string(value) != `"done"` {
			t.Fatalf("upgraded peer retained result %s=%s err=%v after_ready=%s upgraded=%s survivor=%s", completedID, value, err, time.Since(readyAt), mixedVersionResultProbe(ctx, upgraded, typ, completedID), mixedVersionResultProbe(ctx, all[1], typ, completedID))
		}
	}
	if _, err := upgradedClient.Start(ctx, typ, postUpgradeID, payload); err != nil {
		t.Fatalf("start through upgraded peer: %v", err)
	}
	result, err = upgradedClient.Await(ctx, typ, postUpgradeID)
	if err != nil || string(result) != `"done"` {
		t.Fatalf("post-upgrade result=%s err=%v", result, err)
	}
	if report, err := integrity.Check(ctx, upgraded); err != nil || report.Invocations != 3 || report.Journals != 3 || report.Terminal != 3 {
		t.Fatalf("post-upgrade retained audit: report=%+v err=%v", report, err)
	}
}

func mixedVersionResultProbe(ctx context.Context, js jetstream.JetStream, typ, id string) string {
	attempt, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	inv, err := js.Stream(attempt, "WF_INV")
	if err != nil {
		return fmt.Sprintf("inv-stream-error=%v", err)
	}
	message, err := inv.GetLastMsgForSubject(attempt, identity.InvocationSubject(typ, id))
	if err != nil {
		return fmt.Sprintf("inv-read-error=%v", err)
	}
	state, err := js.KeyValue(attempt, "WF_STATE")
	if err != nil {
		return fmt.Sprintf("inv-seq=%d state-bucket-error=%v", message.Sequence, err)
	}
	entry, err := state.Get(attempt, identity.Key(typ, id))
	if err != nil {
		return fmt.Sprintf("inv-seq=%d state-read-error=%v", message.Sequence, err)
	}
	return fmt.Sprintf("inv-seq=%d state-rev=%d state=%s", message.Sequence, entry.Revision(), entry.Value())
}

func waitMixedVersionReplicaCatchup(ctx context.Context, js jetstream.JetStream) (time.Time, error) {
	var lastErr error
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		ready := true
		for _, name := range []string{"WF_INV", "WF_RUN", "WF_JRN", "KV_WF_STATE"} {
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			stream, err := js.Stream(attempt, name)
			if err == nil {
				var info *jetstream.StreamInfo
				info, err = stream.Info(attempt)
				if err == nil && (info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2) {
					err = fmt.Errorf("%s replica status: %+v", name, info.Cluster)
				}
				if err == nil {
					for _, replica := range info.Cluster.Replicas {
						if !replica.Current || replica.Offline {
							err = fmt.Errorf("%s replica not current: %+v", name, replica)
							break
						}
					}
				}
			}
			stop()
			if err != nil {
				lastErr = err
				ready = false
				break
			}
		}
		if ready {
			return time.Now(), nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return time.Time{}, fmt.Errorf("replicas did not catch up: %v (context: %v)", lastErr, ctx.Err())
}
