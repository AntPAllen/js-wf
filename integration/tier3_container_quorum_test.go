//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestFiveContainerPublishRequiresQuorum cuts the route network down to two
// reachable replicas, then restores a third and audits acknowledged writes.
func TestFiveContainerPublishRequiresQuorum(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatalf("five-node routes: %v", err)
	}
	nc, err := nats.Connect(cluster.ClientURL(0), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for until := time.Now().Add(60 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.Ensure(attempt, js, 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision workflow stores: %v", err)
	}
	recorder := &history.Recorder{}
	defer func() {
		path := os.Getenv("WF_TIER3_QUORUM_HISTORY_OUT")
		if path == "" {
			return
		}
		file, err := os.Create(path)
		if err != nil {
			t.Errorf("create quorum-cut client history: %v", err)
			return
		}
		if err := recorder.WriteJSONL(file); err != nil {
			t.Errorf("write quorum-cut client history: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Errorf("close quorum-cut client history: %v", err)
		}
	}()
	observed := client.NewObserved(js, recorder)
	config := jetstream.StreamConfig{Name: "TIER3_QUORUM", Subjects: []string{"tier3.quorum"}, Storage: jetstream.FileStorage, Replicas: 5, Discard: jetstream.DiscardNew}
	var stream jetstream.Stream
	for until := time.Now().Add(60 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		stream, err = js.CreateStream(attempt, config)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create five-replica stream: %v", err)
	}
	preCtx, stopPre := context.WithTimeout(ctx, 5*time.Second)
	before, err := js.Publish(preCtx, "tier3.quorum", []byte("before"))
	stopPre()
	if err != nil {
		t.Fatalf("publish before route cut: %v", err)
	}
	for _, node := range []int{2, 3, 4} {
		if err := cluster.DisconnectNode(node); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range []int{2, 3, 4} {
		if _, err := waitRouteCounts(ctx, cluster, node, 0, 2*time.Minute); err != nil {
			t.Fatalf("node %d route drain: %v", node, err)
		}
	}
	// All three disconnected nodes have zero routes. The remaining two can
	// communicate with each other but cannot form a five-replica majority.
	noQuorumCtx, stopNoQuorum := context.WithTimeout(ctx, 3*time.Second)
	ack, publishErr := js.Publish(noQuorumCtx, "tier3.quorum", []byte("during"))
	stopNoQuorum()
	if publishErr == nil {
		t.Fatalf("publish acknowledged without quorum at sequence %d", ack.Sequence)
	}
	const typ, id = "tier3", "quorum-start"
	startCtx, stopStart := context.WithTimeout(ctx, 3*time.Second)
	_, startErr := observed.Start(startCtx, typ, id, []byte(`42`))
	stopStart()
	if !errors.Is(startErr, client.ErrStartUnknown) {
		t.Fatalf("start without quorum returned %v, want unknown outcome", startErr)
	}
	if err := cluster.ConnectNode(2); err != nil {
		t.Fatal(err)
	}
	var after *jetstream.PubAck
	for until := time.Now().Add(60 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		after, err = js.Publish(attempt, "tier3.quorum", []byte("after"))
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("publish after restoring quorum: %v", err)
	}
	if before.Sequence == after.Sequence {
		t.Fatalf("before and after both acknowledged sequence %d", before.Sequence)
	}
	var handle client.Handle
	for until := time.Now().Add(60 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		handle, err = observed.Start(attempt, typ, id, []byte(`42`))
		stop()
		if err == nil || errors.Is(err, client.ErrAlreadyStarted) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil && !errors.Is(err, client.ErrAlreadyStarted) || handle.InvSeq == 0 {
		t.Fatalf("start after restoring quorum: handle=%+v err=%v", handle, err)
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("quorum-cut start history=%s: %v", result, err)
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	invInfo, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var retained *jetstream.RawStreamMsg
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		retained, err = inv.GetLastMsgForSubject(attempt, identity.InvocationSubject(typ, id))
		stop()
		if err == nil {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if err != nil || invInfo.State.Msgs != 1 || retained.Sequence != handle.InvSeq {
		t.Fatalf("write-once invocation: count=%d retained=%+v handle=%+v err=%v", invInfo.State.Msgs, retained, handle, err)
	}
	acked := map[uint64]string{before.Sequence: "before", after.Sequence: "after"}
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = auditPlainStream(attempt, stream, acked)
		stop()
		if err == nil {
			t.Logf("quorum cut: no acknowledgment with two reachable replicas; acknowledged sequences %d and %d before and after heal; start history=%d operations invocation=%d", before.Sequence, after.Sequence, len(recorder.Snapshot()), handle.InvSeq)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal(fmt.Errorf("acknowledged writes missing after quorum heal: %w", err))
}
