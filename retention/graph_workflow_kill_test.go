//go:build linux

package retention_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

func retentionKillConfig() journal.NativeGraphConfig {
	return journal.NativeGraphConfig{AuthorityStream: "RETENTION_KILL_AUTH", AuthorityPrefix: "wf.graph.retentionkill", ObjectBucket: "RETENTION_KILL_OBJECTS", ExpectedReplicas: 3, CanonicalStarts: true, CanonicalSignals: true}
}

func retentionKillHandlers(graph *journal.GraphStore) map[string]worker.Handler {
	return map[string]worker.Handler{
		"source":    func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) { return input, nil },
		"retention": retention.GraphHandler(nil, graph, time.Minute),
	}
}

// This helper is a separate process. Its only cut hook runs AFTER an accepted
// journal append, then blocks until the controller sends SIGKILL.
func TestGraphRetentionSIGKILLChild(t *testing.T) {
	if os.Getenv("WF_RETENTION_KILL_CHILD") != "1" {
		t.Skip("retention process helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_RETENTION_KILL_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := journal.OpenNativeGraphStore(context.Background(), js, retentionKillConfig())
	if err != nil {
		t.Fatal(err)
	}
	cut, err := strconv.ParseUint(os.Getenv("WF_RETENTION_KILL_CUT"), 10, 64)
	if err != nil || cut < 1 || cut > 4 {
		t.Fatal("invalid append cut", cut, err)
	}
	handlers := retentionKillHandlers(graph)
	handlers["retention"] = retention.GraphHandler(js, graph, time.Minute)
	w, err := worker.New(context.Background(), js, "retention-killed", handlers, worker.WithGraphJournal(graph), worker.WithOperationObserver(func(e worker.OperationEvent) {
		if e.Type != "retention" || e.Operation != "journal_append" || e.JournalIndex != cut || e.Error != "" {
			return
		}
		data, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}
		if err := os.WriteFile(os.Getenv("WF_RETENTION_KILL_PATH")+".cut", data, 0600); err != nil {
			panic(err)
		}
		select {}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.RunPartition(context.Background(), identity.Partition("retention", os.Getenv("WF_RETENTION_KILL_ID"), provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestNativeGraphRetentionSDKBoundarySIGKILL(t *testing.T) {
	if os.Getenv("WF_RETENTION_SIGKILL") != "1" {
		t.Skip("set WF_RETENTION_SIGKILL=1 for four SDK cuts and two ID-reuse process controls")
	}
	for cut := uint64(1); cut <= 4; cut++ {
		for _, reuse := range []bool{false, true} {
			if reuse && cut < 3 {
				continue
			}
			t.Run(fmt.Sprintf("cut%d/reuse%v", cut, reuse), func(t *testing.T) { retentionSDKKill(t, cut, reuse) })
		}
	}
}

func retentionSDKKill(t *testing.T, cut uint64, reuse bool) {
	t.Helper()
	root := t.TempDir()
	cluster, err := testcluster.Start(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for {
		ready := false
		for _, s := range cluster.Servers {
			ready = ready || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == 3
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := provision.Ensure(ctx, js, 3); err != nil {
		t.Fatal(err)
	}
	cfg := retentionKillConfig()
	configs, err := journal.NativeGraphStreamConfigs(cfg, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range configs {
		if _, err := js.CreateStream(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	open := func() *journal.GraphStore {
		g, err := journal.OpenNativeGraphStore(ctx, js, cfg)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	graph := open()
	c, err := client.NewWithGraphJournal(js, graph)
	if err != nil {
		t.Fatal(err)
	}
	handlers := retentionKillHandlers(graph)
	handlers["retention"] = retention.GraphHandler(js, graph, time.Minute)
	operationID := "operation"
	for identity.Partition("retention", operationID, provision.Partitions) == identity.Partition("source", "target", provision.Partitions) {
		operationID += "x"
	}
	run := func(typ, id, name string, g *journal.GraphStore) {
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		w, err := worker.New(ctx, js, name, handlers, worker.WithGraphJournal(g), worker.WithDispatchObserver(func(e worker.DispatchEvent) {
			if e.Type == typ && e.ID == id && e.Stage == "ack" {
				stop()
			}
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		if err := w.RunPartition(runCtx, identity.Partition(typ, id, provision.Partitions)); err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	source, err := c.Start(ctx, "source", "target", []byte(`42`))
	if err != nil {
		t.Fatal(err)
	}
	run("source", "target", "source-before-kill", graph)
	if result, err := c.Await(ctx, "source", "target"); err != nil || !bytes.Equal(result, []byte(`42`)) {
		t.Fatal(string(result), err)
	}
	input, _ := json.Marshal(retention.Request{Type: "source", ID: "target"})
	operation, err := c.Start(ctx, "retention", operationID, input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "child")
	child := exec.Command(os.Args[0], "-test.run=^TestGraphRetentionSIGKILLChild$", "-test.timeout=3m")
	child.Env = append(os.Environ(), "WF_RETENTION_KILL_CHILD=1", "WF_RETENTION_KILL_URL="+cluster.Clients[1].ConnectedUrl(), "WF_RETENTION_KILL_PATH="+path, "WF_RETENTION_KILL_CUT="+strconv.FormatUint(cut, 10), "WF_RETENTION_KILL_ID="+operationID)
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
		t.Logf("child output: %s", output.String())
	}()
	var event worker.OperationEvent
	for {
		data, err := os.ReadFile(path + ".cut")
		if err == nil && json.Unmarshal(data, &event) == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("child did not reach cut", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	wantKind := journal.StepRequested
	if cut%2 == 0 {
		wantKind = journal.StepCompleted
	}
	if event.JournalKind != wantKind || event.JournalIndex != cut || event.ID != operationID || event.Error != "" {
		t.Fatal("wrong append cut", event)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = child.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatal("child did not exit by signal", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("wrong child exit", status)
	}
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	heldEntry, err := kv.Get(ctx, identity.Key("retention", operationID))
	if err != nil {
		t.Fatal("killed worker released lease", err)
	}
	var held lease.Value
	if json.Unmarshal(heldEntry.Value(), &held) != nil || held.Worker != "retention-killed" {
		t.Fatal("wrong held lease", held)
	}
	before, _, err := graph.Read(ctx, "retention", operationID, operation.InvSeq)
	if err != nil || len(before) != int(cut)+1 {
		t.Fatal("committed cut differs", len(before), err)
	}
	if cut >= 2 {
		var lookup struct {
			Result uint64 `json:"result"`
		}
		if before[2].Kind != journal.StepCompleted || json.Unmarshal(before[2].Payload, &lookup) != nil || lookup.Result != source.InvSeq {
			t.Fatal("durable lookup is not bound to original target", string(before[2].Payload))
		}
	}
	var replacement client.Handle
	var replacementStatus journal.GraphRetirement
	if reuse {
		if err := retention.PurgeGraphInvocation(ctx, js, graph, "source", "target", source.InvSeq, time.Minute); err != nil {
			t.Fatal(err)
		}
		replacement, err = c.Start(ctx, "source", "target", []byte(`43`))
		if err != nil || replacement.InvSeq <= source.InvSeq {
			t.Fatal("replacement start", replacement, err)
		}
		run("source", "target", "source-replacement", graph)
		replacementStatus, err = graph.InspectRetirement(ctx, "source", "target")
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := open()
	handlers["retention"] = retention.GraphHandler(js, fresh, time.Minute)
	run("retention", operationID, "retention-successor", fresh)
	after, _, err := fresh.Read(ctx, "retention", operationID, operation.InvSeq)
	if err != nil || len(after) <= len(before) || !reflect.DeepEqual(before, after[:len(before)]) {
		t.Fatal("accepted prefix changed", err)
	}
	last := after[len(after)-1]
	if reuse {
		if cut == 3 {
			if last.Kind != journal.Failed {
				t.Fatal("uncompleted old purge did not fail stale", last.Kind)
			}
			var outcome wf.Outcome
			if json.Unmarshal(last.Payload, &outcome) != nil || !strings.Contains(outcome.Error, journal.ErrStale.Error()) {
				t.Fatal("wrong stale terminal", string(last.Payload))
			}
		} else {
			// The effect and its completion were accepted before SIGKILL.
			// SDK replay must return that success without invoking purge again.
			if last.Kind != journal.Completed {
				t.Fatal("recorded purge success was not replayed", last.Kind)
			}
			if result, err := c.Await(ctx, "retention", operationID); err != nil || !bytes.Equal(result, []byte(`true`)) {
				t.Fatal("recorded purge result", string(result), err)
			}
		}
		current, err := fresh.InspectRetirement(ctx, "source", "target")
		if err != nil || !reflect.DeepEqual(current, replacementStatus) {
			t.Fatal("replacement changed", current, err)
		}
		if result, err := c.Await(ctx, "source", "target"); err != nil || !bytes.Equal(result, []byte(`43`)) {
			t.Fatal("replacement result", string(result), err)
		}
	} else {
		if last.Kind != journal.Completed {
			t.Fatal("purge did not complete", last.Kind)
		}
		if result, err := c.Await(ctx, "retention", operationID); err != nil || !bytes.Equal(result, []byte(`true`)) {
			t.Fatal("purge result", string(result), err)
		}
		retired, err := fresh.InspectRetirement(ctx, "source", "target")
		if err != nil || !retired.Retired || retired.Invocation != source.InvSeq {
			t.Fatal("source not retired", retired, err)
		}
	}
	t.Logf("SIGKILL pid=%d cut=%d kind=%s reuse=%v leaseTTL=%s ackWait=%s accepted_prefix=%d final_entries=%d", child.Process.Pid, cut, wantKind, reuse, provision.LeaseTTL, worker.DefaultAckWait, len(before), len(after))
}
