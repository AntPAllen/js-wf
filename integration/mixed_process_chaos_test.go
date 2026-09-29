//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

// TestMixedWorkflowsRecoverFromFourServerFaults is a ten-invocation slice of
// the Tier 2 mix. It runs the four workload classes together on one cluster.
func TestMixedWorkflowsRecoverFromFourServerFaults(t *testing.T) {
	if os.Getenv("WF_MIXED_CHAOS") != "1" {
		t.Skip("set WF_MIXED_CHAOS=1 for mixed real-cluster chaos")
	}
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cluster, err := testcluster.StartPartitionableProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, 4*time.Second)
		err = provision.Ensure(attempt, js[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision: %v", err)
	}
	stream, err := js[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatalf("journal leader: %+v %v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("invalid journal leader %q: %v", info.Cluster.Leader, err)
	}
	killed, other := (leader+1)%3, (leader+2)%3
	kinds := []string{"mixedshort", "mixedshort", "mixedshort", "mixedshort", "mixedtimer", "mixedtimer", "mixedtimer", "mixedsignal", "mixedsignal"}
	rand.New(rand.NewSource(seed)).Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
	kinds = append(kinds, "mixedfanout")
	type invocation struct {
		typ, id   string
		partition uint32
	}
	invocations := make([]invocation, len(kinds))
	parts := map[uint32]bool{}
	for i, typ := range kinds {
		for nonce := 0; ; nonce++ {
			id := fmt.Sprintf("mixed-%02d-%d", i, nonce)
			part := identity.Partition(typ, id, provision.Partitions)
			if !parts[part] {
				parts[part] = true
				invocations[i] = invocation{typ, id, part}
				break
			}
		}
	}
	started := make(chan int, len(invocations))
	var entered [10]sync.Once
	release := make(chan struct{})
	mark := func(input json.RawMessage) (int, error) {
		var index int
		if err := json.Unmarshal(input, &index); err != nil || index < 0 || index >= len(invocations) {
			return 0, fmt.Errorf("invalid mixed input %s: %v", input, err)
		}
		entered[index].Do(func() { started <- index })
		return index, nil
	}
	handlers := map[string]worker.Handler{}
	handlers["mixedshort"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if _, err := mark(input); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "held", 0, func(effectCtx context.Context) (int, error) {
			select {
			case <-release:
				return 42, nil
			case <-effectCtx.Done():
				return 0, effectCtx.Err()
			}
		})
		return json.RawMessage(fmt.Sprint(value)), err
	}
	handlers["mixedtimer"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if _, err := mark(input); err != nil {
			return nil, err
		}
		if err := wf.Sleep(c, "quorum-loss", 10*time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`42`), nil
	}
	handlers["mixedsignal"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if _, err := mark(input); err != nil {
			return nil, err
		}
		value, err := wf.AwaitSignal(c, "go")
		return json.RawMessage(value), err
	}
	handlers["mixedfanout"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if _, err := mark(input); err != nil {
			return nil, err
		}
		promises := make([]wf.Promise, 6)
		for i := range promises {
			p, err := wf.CallAsync(c, "mixedchild", json.RawMessage(fmt.Sprint(i)))
			if err != nil {
				return nil, err
			}
			promises[i] = p
		}
		var sum int
		for _, p := range promises {
			value, err := wf.AwaitPromise(c, p)
			if err != nil {
				return nil, err
			}
			var n int
			if err := json.Unmarshal(value, &n); err != nil {
				return nil, err
			}
			sum += n
		}
		return json.Marshal(sum)
	}
	handlers["mixedchild"] = func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "held-child", 0, func(effectCtx context.Context) (int, error) {
			select {
			case <-release:
				return 7, nil
			case <-effectCtx.Done():
				return 0, effectCtx.Err()
			}
		})
		return json.RawMessage(fmt.Sprint(value)), err
	}
	first, err := worker.New(ctx, js[other], "mixed-before", handlers)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	for _, inv := range invocations {
		go func(part uint32) { _ = first.RunPartition(firstCtx, part) }(inv.partition)
	}
	c := client.New(js[other])
	for i, inv := range invocations {
		if i == len(invocations)-1 {
			for n := 0; n < i; n++ {
				select {
				case <-started:
				case <-time.After(20 * time.Second):
					t.Fatal("mixed workload did not start before fan-out")
				}
			}
		}
		if _, err := c.Start(ctx, inv.typ, inv.id, []byte(fmt.Sprint(i))); err != nil {
			t.Fatalf("start %s/%s: %v", inv.typ, inv.id, err)
		}
	}
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("fan-out did not start")
	}
	j := journal.New(js[other])
	var childIDs []string
	var allSuspended bool
	until = time.Now().Add(20 * time.Second)
	for time.Now().Before(until) {
		allSuspended = true
		for _, inv := range invocations {
			if inv.typ == "mixedshort" {
				continue
			}
			records, _, err := j.Read(ctx, inv.typ, inv.id)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
				allSuspended = false
			}
			if inv.typ == "mixedfanout" {
				childIDs = childIDs[:0]
				for _, record := range records {
					if record.Kind != journal.StepRequested {
						continue
					}
					var req struct {
						Kind    string `json:"kind"`
						ChildID string `json:"child_id"`
					}
					if err := json.Unmarshal(record.Payload, &req); err != nil {
						t.Fatal(err)
					}
					if req.Kind == "call_async" {
						childIDs = append(childIDs, req.ChildID)
					}
				}
			}
		}
		if len(childIDs) == 6 && allSuspended {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if len(childIDs) != 6 || !allSuspended {
		t.Fatalf("before faults: fan-out children=%d suspended=%t", len(childIDs), allSuspended)
	}
	path := filepath.Join(root, "mixed-four-faults.json")
	if output := os.Getenv("FAULT_SCHEDULE_OUT"); output != "" {
		path = output
	}
	schedule := testcluster.FaultSchedule{Seed: seed, Events: []testcluster.FaultEvent{
		{Op: testcluster.SlowDisk, A: other, LatencyMillis: 50},
		{AtMillis: 10, Op: testcluster.PartitionNodes, A: killed, B: other},
		{AtMillis: 20, Op: testcluster.PauseNode, A: leader},
		{AtMillis: 30, Op: testcluster.KillNode, A: killed},
	}}
	if err := schedule.Save(path); err != nil {
		t.Fatal(err)
	}
	t.Logf("FAULT_SEED=%d FAULT_SCHEDULE=%s", seed, path)
	replay, err := testcluster.LoadFaultSchedule(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := replay.Run(ctx, cluster.ApplyFault); err != nil {
		t.Fatal(err)
	}
	time.Sleep(12 * time.Second)
	cluster.RouteMesh().Heal()
	if err := cluster.ResumeNode(leader); err != nil {
		t.Fatal(err)
	}
	conn, err := cluster.Dial(leader)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resumed, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	successor, err := worker.New(ctx, resumed, "mixed-after", handlers)
	if err != nil {
		t.Fatal(err)
	}
	successorCtx, stopSuccessor := context.WithCancel(ctx)
	defer stopSuccessor()
	for part := range parts {
		go func(part uint32) { _ = successor.RunPartition(successorCtx, part) }(part)
	}
	for _, childID := range childIDs {
		part := identity.Partition("mixedchild", childID, provision.Partitions)
		if !parts[part] {
			parts[part] = true
			go func() { _ = successor.RunPartition(successorCtx, part) }()
		}
	}
	close(release)
	for i, inv := range invocations {
		if inv.typ == "mixedsignal" {
			until = time.Now().Add(20 * time.Second)
			for time.Now().Before(until) {
				attempt, stop := context.WithTimeout(ctx, 2*time.Second)
				_, err = c.Signal(attempt, inv.typ, inv.id, "go", []byte(`42`), fmt.Sprintf("mixed-signal-%d", i))
				stop()
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("signal %d after heal: %v", i, err)
			}
		}
	}
	for _, inv := range invocations {
		result, err := c.Await(ctx, inv.typ, inv.id)
		if err != nil || string(result) != "42" {
			t.Fatalf("mixed %s/%s result=%s err=%v", inv.typ, inv.id, result, err)
		}
	}
	if err := cluster.StopSlowDisk(other); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(cluster.DiskTracePath(other))
	if err != nil || !strings.Contains(string(trace), "(DELAYED)") {
		t.Fatalf("disk trace missing delayed store syscall: %v", err)
	}
	if err := cluster.RestartNode(killed); err != nil {
		t.Fatal(err)
	}
	third, err := jetstream.New(cluster.Clients[killed])
	if err != nil {
		t.Fatal(err)
	}
	for _, inv := range invocations {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		result, err := client.New(third).Await(attempt, inv.typ, inv.id)
		stop()
		if err != nil || string(result) != "42" {
			t.Fatalf("restarted node mixed %s/%s result=%s err=%v", inv.typ, inv.id, result, err)
		}
	}
	seen := map[string]bool{}
	for _, childID := range childIDs {
		if childID == "" || seen[childID] {
			t.Fatalf("duplicate or empty child ID: %q", childID)
		}
		seen[childID] = true
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		result, err := client.New(third).Await(attempt, "mixedchild", childID)
		stop()
		if err != nil || string(result) != "7" {
			t.Fatalf("restarted node child %s result=%s err=%v", childID, result, err)
		}
	}
	for _, inv := range invocations {
		if inv.typ != "mixedsignal" {
			continue
		}
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		records, _, readErr := journal.New(third).Read(attempt, inv.typ, inv.id)
		stop()
		if readErr != nil {
			t.Fatalf("signal journal %s: %v", inv.id, readErr)
		}
		var consumed int
		for _, record := range records {
			if record.Kind == journal.SignalConsumed {
				consumed++
			}
		}
		if consumed != 1 {
			t.Fatalf("signal %s consumed entries=%d, want 1", inv.id, consumed)
		}
	}
	until = time.Now().Add(20 * time.Second)
	var report integrity.Report
	for time.Now().Before(until) {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		report, err = integrity.Check(attempt, third)
		stop()
		if err == nil && report.Invocations == 16 && report.Journals == 16 && report.Terminal == 16 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || report.Invocations != 16 || report.Journals != 16 || report.Terminal != 16 {
		t.Fatalf("mixed retained integrity: report=%+v err=%v", report, err)
	}
}
