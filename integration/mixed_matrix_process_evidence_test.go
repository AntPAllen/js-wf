//go:build linux

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestMixedMatrixProcessRetainsActualFencingAndFinalCounters(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	root := os.Getenv("WF_PROCESS_EVIDENCE_ARTIFACT_ROOT")
	if root == "" {
		root = t.TempDir()
	}
	if err = os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	child, err := startMatrixProcessWorker(ctx, root, []string{cluster.Servers[0].ClientURL()}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stopMatrixProcessWorker(child)
	// The existing acquisition handoff holds the real lease before any effect.
	// Let its renewal freshness expire, revoke exactly its retained revision,
	// then release the handoff. Production execution must report the lost lease.
	if err = os.WriteFile(child.base+"-isolation-arm", []byte("fencing-proof"), 0600); err != nil {
		t.Fatal(err)
	}
	c := client.New(js)
	if _, err = c.Start(ctx, "matrixshort", "process-evidence", json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	var target matrixIsolationTarget
	for ctx.Err() == nil {
		data, readErr := os.ReadFile(child.base + "-isolation-ready.json")
		if readErr == nil {
			if err = json.Unmarshal(data, &target); err != nil {
				t.Fatal(err)
			}
			break
		}
		if !os.IsNotExist(readErr) {
			t.Fatal(readErr)
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	if target.Token != "fencing-proof" || target.Delivery.Type != "matrixshort" || target.Delivery.ID != "process-evidence" {
		t.Fatalf("wrong target: %+v ctx=%v", target, ctx.Err())
	}
	kv, err := js.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	key := identity.Key("matrixshort", "process-evidence")
	entry, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var heldEpoch struct {
		Epoch uint64 `json:"epoch"`
	}
	if err = json.Unmarshal(entry.Value(), &heldEpoch); err != nil || heldEpoch.Epoch == 0 {
		t.Fatalf("held lease epoch=%d err=%v", heldEpoch.Epoch, err)
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(4 * time.Second):
	}
	if err = kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(child.base + "-isolation-arm"); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, "matrixshort", "process-evidence")
	if err != nil || string(result) != "42" {
		t.Fatalf("result=%s err=%v", result, err)
	}
	entries, _, err := journal.New(js).Read(ctx, "matrixshort", "process-evidence")
	if err != nil || len(entries) != 4 {
		t.Fatalf("recovered journal=%+v err=%v", entries, err)
	}
	for _, entry := range entries {
		if entry.Entry.Epoch <= heldEpoch.Epoch {
			t.Fatalf("retired epoch wrote recovered history: %+v", entry)
		}
	}
	if report, err := integrity.Check(ctx, js); err != nil {
		t.Fatalf("retained audit=%+v err=%v", report, err)
	}
	if err = child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-child.exited:
		if err != nil {
			t.Fatalf("graceful exit: %v; log=%s", err, child.base+".log")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	file, err := os.Open(child.base + "-fencing.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var count uint64
	var matched bool
	for scanner.Scan() {
		var r matrixProcessFencingRecord
		if err = json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatal(err)
		}
		count++
		if r.PID != child.cmd.Process.Pid || r.Sequence != count || r.Event.Worker != child.id {
			t.Fatalf("identity/sequence: %+v", r)
		}
		if r.Event.RunSequence == target.Delivery.RunSequence && r.Event.Delivery == target.Delivery.Delivery && r.Event.Type == target.Delivery.Type && r.Event.ID == target.Delivery.ID {
			matched = r.Event.Epoch != 0 && r.Event.Error != "" && (r.Event.Reason == "lease_execution_lost" || r.Event.Reason == "lease_heartbeat_lost")
		}
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatalf("no typed fencing for held delivery: %+v", target.Delivery)
	}
	var final matrixProcessMetrics
	data, err := os.ReadFile(child.base + "-metrics.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &final); err != nil {
		t.Fatal(err)
	}
	if final.PID != child.cmd.Process.Pid || final.Worker != child.id || final.Metrics.FencingEvents != count {
		t.Fatalf("metrics=%+v observed=%d", final, count)
	}
	t.Logf("pid=%d fencing=%d final_counters_match=true result=%s artifacts=%s", final.PID, count, result, filepath.Base(root))
}
