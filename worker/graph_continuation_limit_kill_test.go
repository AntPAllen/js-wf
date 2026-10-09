//go:build linux

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	"js-wf/testcluster"
)

func graphLimitKillConfig() journal.NativeGraphConfig {
	return journal.NativeGraphConfig{AuthorityStream: "KILL_LIMIT_AUTH", AuthorityPrefix: "wf.graph.killlimit", ObjectBucket: "KILL_LIMIT_OBJECTS", ExpectedReplicas: 3, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true}
}

func TestGraphContinuationLimitKillChild(t *testing.T) {
	if os.Getenv("WF_GRAPH_LIMIT_CHILD") != "1" {
		t.Skip("graph continuation process helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_GRAPH_LIMIT_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.NewWithDomain(nc, "GRAPH_KILL")
	if err != nil {
		t.Fatal(err)
	}
	graph, err := journal.OpenNativeGraphStore(context.Background(), js, graphLimitKillConfig())
	if err != nil {
		t.Fatal(err)
	}
	path, cut := os.Getenv("WF_GRAPH_LIMIT_PATH"), os.Getenv("WF_GRAPH_LIMIT_CUT")
	handlers, stages := limitKillHandlers(path+".handlers", 16, false)
	index := map[string]uint64{"after_signal": 13, "after_completion": 14, "after_failed": 15}[cut]
	kind := map[string]journal.Kind{"after_signal": journal.SignalConsumed, "after_completion": journal.StepCompleted, "after_failed": journal.Failed}[cut]
	if index == 0 {
		t.Fatal("invalid graph kill cut")
	}
	observer := func(event OperationEvent) {
		if event.Operation != "journal_append" || event.Error != "" {
			return
		}
		if event.JournalIndex == 12 && event.JournalKind == journal.Suspended {
			if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
				panic(err)
			}
		}
		if event.JournalIndex == index && event.JournalKind == kind {
			raw, err := json.Marshal(event)
			if err != nil {
				panic(err)
			}
			if err := os.WriteFile(path+".cut", raw, 0600); err != nil {
				panic(err)
			}
			select {} // Controller sends SIGKILL; no cancellation or lease release.
		}
	}
	w, err := New(context.Background(), js, "graph-limit-killed", handlers, WithGraphJournal(graph), WithOperationObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.maxEntries != journal.MaxEntries {
		t.Fatal("production cap changed")
	}
	w.maxEntries = 16
	w.continuations = map[string]map[string]ContinuationHandler{limitKillType: stages}
	if err := w.RunPartition(context.Background(), identity.Partition(limitKillType, limitKillID, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func TestNativeGraphContinuationLimitSIGKILLRecovery(t *testing.T) {
	for _, cut := range []string{"after_signal", "after_completion", "after_failed"} {
		t.Run(cut, func(t *testing.T) { graphContinuationLimitKill(t, cut) })
	}
}

func graphContinuationLimitKill(t *testing.T, cut string) {
	t.Helper()
	root := t.TempDir()
	if artifact := os.Getenv("GRAPH_LIMIT_KILL_ARTIFACT_ROOT"); artifact != "" {
		root = filepath.Join(artifact, cut)
		if err := os.MkdirAll(root, 0755); err != nil {
			t.Fatal(err)
		}
	}
	save := func(name string, value any) {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	cluster, err := testcluster.StartWithDomain(filepath.Join(root, "cluster"), 3, "GRAPH_KILL")
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for ctx.Err() == nil {
		ready := false
		for _, server := range cluster.Servers {
			ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == 3
		}
		if ready {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	js, err := jetstream.NewWithDomain(cluster.Clients[0], "GRAPH_KILL")
	if err != nil {
		t.Fatal(err)
	}
	if err = provision.Ensure(ctx, js, 3); err != nil {
		t.Fatal(err)
	}
	cfg := graphLimitKillConfig()
	configs, err := journal.NativeGraphStreamConfigs(cfg, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs {
		if _, err = js.CreateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(js).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, limitKillType, limitKillID, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "child")
	child := exec.Command(os.Args[0], "-test.run=^TestGraphContinuationLimitKillChild$", "-test.timeout=3m")
	child.Env = append(os.Environ(), "WF_GRAPH_LIMIT_CHILD=1", "WF_GRAPH_LIMIT_URL="+cluster.Clients[1].ConnectedUrl(), "WF_GRAPH_LIMIT_PATH="+path, "WF_GRAPH_LIMIT_CUT="+cut)
	log, err := os.Create(path + ".log")
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child.Stdout, child.Stderr = log, log
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			<-exited
		}
	}()
	waitFile := func(name string) {
		for {
			if _, err := os.Stat(name); err == nil {
				return
			}
			select {
			case err := <-exited:
				waited = true
				t.Fatalf("child exited before durable cut: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	waitFile(path + ".ready")
	if _, err = c.Signal(ctx, h.Type, h.ID, "gate", []byte(`true`), "gate-key"); err != nil {
		t.Fatal(err)
	}
	waitFile(path + ".cut")
	cutBytes, err := os.ReadFile(path + ".cut")
	if err != nil {
		t.Fatal(err)
	}
	var cutEvent OperationEvent
	if err := json.Unmarshal(cutBytes, &cutEvent); err != nil || cutEvent.RunSequence == 0 || cutEvent.Delivery == 0 || cutEvent.Error != "" || cutEvent.Operation != "journal_append" {
		t.Fatal("invalid cut operation", cutEvent, err)
	}
	prefix, _, err := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	wantCount := map[string]int{"after_signal": 14, "after_completion": 15, "after_failed": 16}[cut]
	wantKind := map[string]journal.Kind{"after_signal": journal.SignalConsumed, "after_completion": journal.StepCompleted, "after_failed": journal.Failed}[cut]
	if len(prefix) != wantCount || prefix[len(prefix)-1].Index != uint64(wantCount-1) || prefix[len(prefix)-1].Kind != wantKind {
		t.Fatal("wrong durable cut", prefix)
	}
	before, err := os.ReadFile(path + ".handlers")
	if err != nil || bytes.Contains(before, []byte("effect\n")) {
		t.Fatal("effect before kill", string(before), err)
	}
	leaseKV, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	status, err := leaseKV.Status(ctx)
	if err != nil || status.TTL() != provision.LeaseTTL {
		t.Fatal("lease configuration mismatch", err)
	}
	entry, err := leaseKV.Get(ctx, identity.Key(h.Type, h.ID))
	if err != nil {
		t.Fatal(err)
	}
	var held lease.Value
	if err = json.Unmarshal(entry.Value(), &held); err != nil || held.Worker != "graph-limit-killed" {
		t.Fatal("cut lease differs", held, err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.Get(ctx, identity.Key(h.Type, h.ID)); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal("terminal projection before kill", err)
	}
	save("prefix", prefix)
	save("held-lease", held)
	killed := time.Now()
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := <-exited
	waited = true
	signal, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !signal.Signaled() || signal.Signal() != syscall.SIGKILL {
		t.Fatal("not SIGKILL", waitErr, child.ProcessState)
	}
	if _, err := lease.NewWithKeyValue(leaseKV).Acquire(ctx, h.Type, h.ID, "premature"); !errors.Is(err, lease.ErrHeld) {
		t.Fatal("dead owner lease disappeared early", err)
	}
	handlers, stages := limitKillHandlers(path+".handlers", 16, true)
	matchedAck := make(chan DispatchEvent, 1)
	w, err := New(ctx, js, "graph-limit-successor", handlers, WithGraphJournal(graph), WithDispatchObserver(func(event DispatchEvent) {
		if event.Type == h.Type && event.ID == h.ID && event.Stage == "ack" && event.Error == "" && event.RunSequence == cutEvent.RunSequence && event.Delivery > cutEvent.Delivery {
			select {
			case matchedAck <- event:
			default:
			}
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.maxEntries != journal.MaxEntries {
		t.Fatal("successor production cap changed")
	}
	w.maxEntries = 16
	w.continuations = map[string]map[string]ContinuationHandler{h.Type: stages}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(runCtx, identity.Partition(h.Type, h.ID, provision.Partitions)) }()
	joined := false
	defer func() {
		stop()
		if !joined {
			<-done
		}
	}()
	_, resultErr := c.Await(ctx, h.Type, h.ID)
	// Await may observe the immutable canonical Failed before a worker repairs
	// WF_STATE. Require that repair too before stopping the successor.
	for ctx.Err() == nil {
		terminal, err := state.Get(ctx, identity.Key(h.Type, h.ID))
		if err == nil && len(terminal.Value()) > 0 {
			break
		}
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var ack DispatchEvent
	select {
	case ack = <-matchedAck:
	case <-ctx.Done():
		t.Fatal("successor did not acknowledge exact killed delivery", ctx.Err())
	}
	recovery := time.Since(killed)
	stop()
	runErr := <-done
	joined = true
	if runErr != nil || resultErr == nil || resultErr.Error() != journal.ErrTooLong.Error() {
		t.Fatal("recovery result differs", runErr, resultErr)
	}
	if recovery >= 30*time.Second {
		t.Fatal("kill recovery exceeds strict 30s target", recovery)
	}
	records, _, err := graph.Read(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil || len(records) != 16 || records[15].Kind != journal.Failed || !reflect.DeepEqual(prefix, records[:wantCount]) {
		t.Fatal("acknowledged prefix/global cap changed", len(records), err)
	}
	if cut != "after_failed" && records[15].Epoch <= held.Epoch {
		t.Fatal("successor terminal epoch did not advance", held.Epoch, records[15].Epoch)
	}
	after, err := os.ReadFile(path + ".handlers")
	if err != nil || bytes.Contains(after, []byte("effect\n")) || strings.Count(string(before), "initial\n") != strings.Count(string(after), "initial\n") || strings.Count(string(before), "middle\n") != strings.Count(string(after), "middle\n") {
		t.Fatal("prefix/effect reran", string(before), string(after), err)
	}
	terminal, err := state.Get(ctx, identity.Key(h.Type, h.ID))
	if err != nil || !bytes.Equal(terminal.Value(), records[15].Payload) {
		t.Fatal("projection differs from canonical failure", err)
	}
	for _, connection := range cluster.Clients {
		peer, err := jetstream.NewWithDomain(connection, "GRAPH_KILL")
		if err != nil {
			t.Fatal(err)
		}
		store, err := journal.OpenNativeGraphStore(ctx, peer, cfg)
		if err != nil {
			t.Fatal(err)
		}
		bound, err := client.New(peer).WithGraphJournal(store)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = bound.Await(ctx, h.Type, h.ID); err == nil || err.Error() != resultErr.Error() {
			t.Fatal("peer failure differs", err)
		}
	}
	save("journal", records)
	save("ack", ack)
	save("recovery", map[string]any{"cut": cut, "sigkill": true, "recovery_ns": recovery.Nanoseconds(), "lease_ttl_ns": status.TTL().Nanoseconds(), "held_epoch": held.Epoch, "terminal_epoch": records[15].Epoch, "entries": len(records), "cut_run_sequence": cutEvent.RunSequence, "cut_delivery": cutEvent.Delivery, "ack_run_sequence": ack.RunSequence, "ack_delivery": ack.Delivery})
	t.Logf("GRAPH_LIMIT_SIGKILL cut=%s recovery=%s lease_ttl=%s entries=%d old_epoch=%d terminal_epoch=%d effects=0 archive=true prefix_unchanged=true exact_delivery_ack=%d/%d", cut, recovery, status.TTL(), len(records), held.Epoch, records[15].Epoch, ack.RunSequence, ack.Delivery)
}
