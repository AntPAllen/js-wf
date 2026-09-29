//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestWorkflowRecoversFromFourServerFaults(t *testing.T) {
	runWorkflowWithFourServerFaults(t, false, false, false)
}

func TestTimerWorkflowRecoversFromFourServerFaults(t *testing.T) {
	runWorkflowWithFourServerFaults(t, true, false, false)
}

func TestSignalWorkflowRecoversFromFourServerFaults(t *testing.T) {
	runWorkflowWithFourServerFaults(t, false, true, false)
}

func TestFanoutWorkflowRecoversFromFourServerFaults(t *testing.T) {
	runWorkflowWithFourServerFaults(t, false, false, true)
}

func runWorkflowWithFourServerFaults(t *testing.T, timerWorkflow, signalWorkflow, fanoutWorkflow bool) {
	t.Helper()
	if os.Getenv("WF_PROCESS_WORKFLOW") != "1" {
		t.Skip("set WF_PROCESS_WORKFLOW=1 for the four-fault workflow proof")
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
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	js := make([]jetstream.JetStream, 3)
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	provisionDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(provisionDeadline) {
		attempt, stop := context.WithTimeout(ctx, 4*time.Second)
		err = provision.Ensure(attempt, js[0], 3)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision after metadata readiness retries: %v", err)
	}
	journalStream, err := js[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := journalStream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatalf("journal leader: info=%+v err=%v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("invalid journal leader %q: %v", info.Cluster.Leader, err)
	}
	killed, other := (leader+1)%3, (leader+2)%3
	const typ, id = "fourfault", "held-effect"
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if fanoutWorkflow {
			once.Do(func() { close(entered) })
			promises := make([]wf.Promise, 6)
			for i := range promises {
				p, err := wf.CallAsync(c, "fourfaultchild", json.RawMessage(fmt.Sprint(i)))
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
		if signalWorkflow {
			once.Do(func() { close(entered) })
			value, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			return json.RawMessage(value), nil
		}
		if timerWorkflow {
			once.Do(func() { close(entered) })
			if err := wf.Sleep(c, "during-quorum-loss", 3*time.Second); err != nil {
				return nil, err
			}
			return json.RawMessage(`42`), nil
		}
		value, err := wf.Run(c, "held", 0, func(effectCtx context.Context) (int, error) {
			once.Do(func() { close(entered) })
			select {
			case <-release:
				return 42, nil
			case <-effectCtx.Done():
				return 0, effectCtx.Err()
			}
		})
		if err != nil {
			return nil, err
		}
		return json.RawMessage(fmt.Sprint(value)), nil
	}
	handlers := map[string]worker.Handler{typ: handler}
	if fanoutWorkflow {
		handlers["fourfaultchild"] = func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			value, err := wf.Run(c, "held-child", 0, func(effectCtx context.Context) (int, error) {
				select {
				case <-release:
					return 7, nil
				case <-effectCtx.Done():
					return 0, effectCtx.Err()
				}
			})
			if err != nil {
				return nil, err
			}
			return json.RawMessage(fmt.Sprint(value)), nil
		}
	}
	w, err := worker.New(ctx, js[other], "four-fault-before", handlers)
	if err != nil {
		t.Fatal(err)
	}
	workCtx, stopWork := context.WithCancel(ctx)
	defer stopWork()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workCtx, identity.Partition(typ, id, provision.Partitions)) }()
	c := client.New(js[other])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("worker exited before effect: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("handler effect did not start")
	}
	var childIDs []string
	if timerWorkflow || signalWorkflow || fanoutWorkflow {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			records, _, readErr := journal.New(js[other]).Read(ctx, typ, id)
			if readErr != nil {
				t.Fatalf("read workflow suspension: %v", readErr)
			}
			if fanoutWorkflow {
				childIDs = childIDs[:0]
				for _, record := range records {
					if record.Kind != journal.StepRequested {
						continue
					}
					var request struct {
						Kind    string `json:"kind"`
						ChildID string `json:"child_id"`
					}
					if err := json.Unmarshal(record.Payload, &request); err != nil {
						t.Fatal(err)
					}
					if request.Kind == "call_async" {
						childIDs = append(childIDs, request.ChildID)
					}
				}
			}
			if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended && (!fanoutWorkflow || len(childIDs) == 6) {
				break
			}
			time.Sleep(30 * time.Millisecond)
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow did not suspend before faults; child IDs=%d", len(childIDs))
		}
	}
	path := filepath.Join(root, "workflow-four-faults.json")
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
	replayed, err := testcluster.LoadFaultSchedule(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := replayed.Run(ctx, cluster.ApplyFault); err != nil {
		t.Fatal(err)
	}
	// Leave the two available servers without a quorum long enough for a
	// heartbeat to encounter the fault while the effect is still blocked.
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
	successor, err := worker.New(ctx, resumed, "four-fault-after", handlers)
	if err != nil {
		t.Fatal(err)
	}
	successorCtx, stopSuccessor := context.WithCancel(ctx)
	defer stopSuccessor()
	successorDone := make(chan error, 1)
	go func() {
		successorDone <- successor.RunPartition(successorCtx, identity.Partition(typ, id, provision.Partitions))
	}()
	if fanoutWorkflow {
		partitions := map[uint32]bool{identity.Partition(typ, id, provision.Partitions): true}
		for _, childID := range childIDs {
			part := identity.Partition("fourfaultchild", childID, provision.Partitions)
			if partitions[part] {
				continue
			}
			partitions[part] = true
			go func() { _ = successor.RunPartition(successorCtx, part) }()
		}
	}
	close(release)
	if signalWorkflow {
		signalDeadline := time.Now().Add(20 * time.Second)
		var signalErr error
		for time.Now().Before(signalDeadline) {
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			_, signalErr = c.Signal(attempt, typ, id, "go", []byte(`42`), "four-fault-signal")
			stop()
			if signalErr == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if signalErr != nil {
			t.Fatalf("signal after route heal: %v", signalErr)
		}
	}
	result, err := c.Await(ctx, typ, id)
	t.Logf("four-fault workflow result after %s: result=%s err=%v", time.Since(startedAt), result, err)
	if err != nil || string(result) != "42" {
		t.Fatalf("workflow result=%s err=%v, first-worker=%+v successor=%+v", result, err, w.Metrics(), successor.Metrics())
	}
	if err := cluster.StopSlowDisk(other); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(cluster.DiskTracePath(other))
	if err != nil || !strings.Contains(string(trace), "(DELAYED)") {
		t.Fatalf("disk trace did not record a delayed store syscall: %v: %s", err, trace)
	}
	if err := cluster.RestartNode(killed); err != nil {
		t.Fatal(err)
	}
	third, err := jetstream.New(cluster.Clients[killed])
	if err != nil {
		t.Fatal(err)
	}
	catchupDeadline := time.Now().Add(30 * time.Second)
	for ctx.Err() == nil && time.Now().Before(catchupDeadline) {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		result, err = client.New(third).Await(attempt, typ, id)
		stop()
		if err == nil && string(result) == "42" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || string(result) != "42" {
		t.Fatalf("restarted node did not serve immutable result: %s, %v", result, err)
	}
	if fanoutWorkflow {
		seen := make(map[string]bool, len(childIDs))
		for _, childID := range childIDs {
			if childID == "" || seen[childID] {
				t.Fatalf("duplicate or empty child ID: %q", childID)
			}
			seen[childID] = true
			attempt, stop := context.WithTimeout(ctx, 5*time.Second)
			childResult, childErr := client.New(third).Await(attempt, "fourfaultchild", childID)
			stop()
			if childErr != nil || string(childResult) != "7" {
				t.Fatalf("child %s result=%s err=%v", childID, childResult, childErr)
			}
		}
	}
	wantInvocations := 1
	if fanoutWorkflow {
		wantInvocations += len(childIDs)
	}
	var report integrity.Report
	auditDeadline := time.Now().Add(15 * time.Second)
	var firstAuditErr error
	for time.Now().Before(auditDeadline) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		report, err = integrity.Check(attempt, third)
		stop()
		if err == nil && report.Invocations == wantInvocations && report.Journals == wantInvocations && report.Terminal == wantInvocations {
			break
		}
		if firstAuditErr == nil && err != nil {
			firstAuditErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || report.Invocations != wantInvocations || report.Journals != wantInvocations || report.Terminal != wantInvocations {
		t.Fatalf("retained integrity after all four faults healed: report=%+v want=%d first_err=%v last_err=%v elapsed=%s", report, wantInvocations, firstAuditErr, err, time.Since(startedAt))
	}
	if signalWorkflow {
		records, _, err := journal.New(third).Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		var consumed int
		for _, entry := range records {
			if entry.Kind == journal.SignalConsumed {
				consumed++
			}
		}
		if consumed != 1 {
			t.Fatalf("signal consumption entries=%d want=1", consumed)
		}
	}
}
