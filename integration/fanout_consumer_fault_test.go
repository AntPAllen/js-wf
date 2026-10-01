//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/testcluster"
	"js-wf/worker"
)

// The initial worker is stopped at a durable parent suspension. Its partition
// still has queued child deliveries; the consumer fault must hit that backlog.
func killFanoutBacklogConsumer(t *testing.T, ctx context.Context, cluster *testcluster.ProcessCluster, nodes []jetstream.JetStream, partition uint32) {
	t.Helper()
	name := fmt.Sprintf("WF_P_%02d", partition)
	consumer, err := nodes[0].Consumer(ctx, "WF_RUN", name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(ctx)
	if err != nil || info.Cluster == nil || info.Cluster.Leader == "" || info.NumPending == 0 {
		t.Fatalf("consumer has no confirmed leader/backlog: info=%+v err=%v", info, err)
	}
	leader := info.Cluster.Leader
	node, err := strconv.Atoi(strings.TrimPrefix(leader, "wf-process-"))
	if err != nil || node < 0 || node >= len(nodes) {
		t.Fatalf("unknown leader %q", leader)
	}
	control := nodes[(node+1)%len(nodes)]
	before := time.Now()
	t.Logf("FANOUT_CONSUMER_FAULT name=%s leader=%s node=%d pending=%d ack_pending=%d killed=%s", name, leader, node, info.NumPending, info.NumAckPending, before.UTC().Format(time.RFC3339Nano))
	if err := cluster.KillNode(node); err != nil {
		t.Fatal(err)
	}
	state := cluster.Commands[node].ProcessState
	if state == nil {
		t.Fatal("consumer leader process still running after kill")
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("consumer leader did not exit via SIGKILL: %v", state)
	}
	t.Logf("FANOUT_CONSUMER_KILLED pid=%d signal=%s", state.Pid(), status.Signal())
	// A new leader must be confirmed while the old server is still dead.
	bound, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	elected := false
	for bound.Err() == nil {
		attempt, cancel := context.WithTimeout(bound, time.Second)
		c, e := control.Consumer(attempt, "WF_RUN", name)
		if e == nil {
			info, e = c.Info(attempt)
		}
		cancel()
		if e == nil && info.Cluster != nil && info.Cluster.Leader != "" && info.Cluster.Leader != leader {
			elected = true
			break
		}
		select {
		case <-bound.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !elected {
		t.Fatalf("consumer did not elect a successor while old node was dead: %v", bound.Err())
	}
	t.Logf("FANOUT_CONSUMER_ELECTED name=%s old=%s new=%s elapsed=%s", name, leader, info.Cluster.Leader, time.Since(before))
	if err := cluster.RestartNode(node); err != nil {
		t.Fatal(err)
	}
	ready, done := context.WithTimeout(ctx, 45*time.Second)
	defer done()
	if err := waitMatrixWorkflowReplicas(ready, control); err != nil {
		t.Fatal(err)
	}
	caughtUp := false
	for ready.Err() == nil {
		attempt, cancel := context.WithTimeout(ready, time.Second)
		c, e := control.Consumer(attempt, "WF_RUN", name)
		if e == nil {
			info, e = c.Info(attempt)
		}
		cancel()
		ok := e == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2
		if ok {
			for _, r := range info.Cluster.Replicas {
				ok = ok && r.Current && !r.Offline
			}
		}
		if ok {
			caughtUp = true
			break
		}
		select {
		case <-ready.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !caughtUp {
		t.Fatalf("consumer replicas did not catch up: %v", ready.Err())
	}
	t.Logf("FANOUT_CONSUMER_HEALED name=%s leader=%s elapsed=%s", name, info.Cluster.Leader, time.Since(before))
}

// This process-kill fault keeps quorum. Child p99 stays measured from raw
// invocation commitment; parent delay starts at its last completed child.
func assertFanoutFaultLatencies(t *testing.T, ctx context.Context, nodes []jetstream.JetStream, typ, id, childType string, children []string) {
	t.Helper()
	inv, err := nodes[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	jrn, err := nodes[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	terminal := func(typ, id string) time.Time {
		t.Helper()
		message, err := jrn.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
		if err != nil {
			t.Fatal(err)
		}
		var entry journal.Entry
		if err := json.Unmarshal(message.Data, &entry); err != nil || entry.Kind != journal.Completed {
			t.Fatalf("missing terminal %s/%s: %+v err=%v", typ, id, entry, err)
		}
		return message.Time
	}
	delays := make([]time.Duration, 0, len(children))
	var lastChild time.Time
	for _, child := range children {
		start, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(childType, child))
		if err != nil {
			t.Fatal(err)
		}
		end := terminal(childType, child)
		if end.Before(start.Time) {
			t.Fatal("terminal precedes invocation")
		}
		delays = append(delays, end.Sub(start.Time))
		if end.After(lastChild) {
			lastChild = end
		}
	}
	sort.Slice(delays, func(i, j int) bool { return delays[i] < delays[j] })
	p99 := delays[(len(delays)*99+99)/100-1]
	parentEnd := terminal(typ, id)
	parentDelay := parentEnd.Sub(lastChild)
	t.Logf("FANOUT_CONSUMER_LATENCY children=%d child_start_p99=%s child_start_max=%s parent_last_child_delay=%s", len(children), p99, delays[len(delays)-1], parentDelay)
	if p99 >= 30*time.Second || parentDelay < 0 || parentDelay >= 30*time.Second {
		t.Fatalf("fanout latency child p99=%s parent=%s want <30s", p99, parentDelay)
	}
	for node, js := range nodes {
		result, err := client.New(js).Await(ctx, typ, id)
		if err != nil || string(result) != fmt.Sprint(len(children)*7) {
			t.Fatalf("node=%d immutable parent result=%s err=%v", node, result, err)
		}
	}
}

// Census actual WorkQueue payloads, rather than inferring enqueues from the
// parent's child declarations or a local successful publication count.
func fanoutQueuedChildRuns(t *testing.T, ctx context.Context, js jetstream.JetStream, typ string, children []string) map[string]uint64 {
	t.Helper()
	stream, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	wanted := map[string]bool{}
	for _, id := range children {
		wanted[identity.Key(typ, id)] = true
	}
	found := map[string]uint64{}
	for seq := info.State.FirstSeq; seq != 0 && seq <= info.State.LastSeq; seq++ {
		message, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		key := string(message.Data)
		if !strings.HasPrefix(key, typ+".") {
			continue
		}
		if !wanted[key] || found[key] != 0 {
			t.Fatalf("unexpected or duplicate child run %q", key)
		}
		found[key] = message.Sequence
	}
	if len(found) != len(children) {
		t.Fatalf("physical queued child runs=%d want=%d", len(found), len(children))
	}
	t.Logf("FANOUT_QUEUED_CHILDREN count=%d stream_messages=%d first=%d last=%d", len(found), info.State.Msgs, info.State.FirstSeq, info.State.LastSeq)
	return found
}

func assertFanoutRunDrain(t *testing.T, ctx context.Context, js jetstream.JetStream, parts map[uint32]bool) {
	t.Helper()
	run, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	bound, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	info, err := waitMixedRunDrain(bound, time.Second, 50*time.Millisecond, func(request context.Context) (*jetstream.StreamInfo, error) { return run.Info(request) })
	if err != nil || info == nil || info.State.Msgs != 0 {
		diagnosis, done := context.WithTimeout(context.Background(), 10*time.Second)
		report := collectMixedRunDrainDiagnostic(diagnosis, js, info, parts, err)
		done()
		saveFanoutConsumerReport(t, "queue", report)
		t.Fatalf("fanout run stream did not physically drain: info=%+v err=%v", info, err)
	}
	for part := range parts {
		consumer, err := js.Consumer(bound, "WF_RUN", fmt.Sprintf("WF_P_%02d", part))
		if err != nil {
			t.Fatal(err)
		}
		info, err := consumer.Info(bound)
		if err != nil || info.NumPending != 0 || info.NumAckPending != 0 {
			t.Fatalf("fanout partition %d pending: info=%+v err=%v", part, info, err)
		}
	}
	t.Logf("FANOUT_CONSUMER_DRAIN stream_messages=0 consumers=%d pending=0 ack_pending=0", len(parts))
}

func saveFanoutConsumerReport(t *testing.T, kind string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Errorf("encode %s report: %v", kind, err)
		return
	}
	if prefix := os.Getenv("WF_FANOUT_CONSUMER_REPORT"); prefix != "" {
		if err := os.WriteFile(prefix+"-"+kind+".json", raw, 0600); err != nil {
			t.Errorf("write %s report: %v", kind, err)
		}
	} else if kind == "queue" {
		t.Logf("FANOUT_QUEUE_DIAGNOSTIC %s", raw)
	}
}

func saveFanoutParentOperations(t *testing.T, events []worker.OperationEvent) {
	t.Helper()
	type summary struct {
		Calls    int
		Duration time.Duration
		Errors   int
	}
	totals := map[string]summary{}
	for _, event := range events {
		v := totals[event.Operation]
		v.Calls++
		v.Duration += event.Duration
		if event.Error != "" {
			v.Errors++
		}
		totals[event.Operation] = v
	}
	t.Logf("FANOUT_PARENT_OPERATIONS events=%d totals=%+v", len(events), totals)
	saveFanoutConsumerReport(t, "operations", events)
}
