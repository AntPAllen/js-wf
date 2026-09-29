//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Signals on both sides of a complete server SIGKILL/restart must retain
// their stream order and be consumed once when the suspended worker resumes.
func TestOrderedSignalsSurviveFullServerRestart(t *testing.T) {
	if os.Getenv("WF_FULL_RESTART_SIGNAL") != "1" {
		t.Skip("set WF_FULL_RESTART_SIGNAL=1 for the process restart proof")
	}
	cluster, err := testcluster.StartPartitionableProcesses(t.TempDir(), 3)
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
	const typ, id, count = "restartsignal", "ordered", 100
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < count; i++ {
			payload, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			value, err := strconv.Atoi(string(payload))
			if err != nil || value != i {
				return nil, fmt.Errorf("signal %d carried %q: %v", i, payload, err)
			}
		}
		return json.RawMessage(`100`), nil
	}
	first, err := worker.New(ctx, js[1], "signal-before-restart", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	part := identity.Partition(typ, id, provision.Partitions)
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, part) }()
	if _, err := client.New(js[0]).Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		records, _, readErr := journal.New(js[2]).Read(ctx, typ, id)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	stopFirst()
	select {
	case err := <-firstDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first worker: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first worker did not stop")
	}
	send := func(from, to int, js jetstream.JetStream) {
		t.Helper()
		for i := from; i < to; i++ {
			until := time.Now().Add(30 * time.Second)
			for {
				attempt, stop := context.WithTimeout(ctx, 3*time.Second)
				_, err := client.New(js).Signal(attempt, typ, id, "go", []byte(strconv.Itoa(i)), fmt.Sprintf("ordered-%03d", i))
				stop()
				if err == nil {
					break
				}
				transient := errors.Is(err, client.ErrEnqueueUnknown) || errors.Is(err, client.ErrSignalUnknown) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrNoResponders)
				if time.Now().After(until) || ctx.Err() != nil || !transient {
					t.Fatalf("signal %d: %v", i, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
	send(0, count/2, js[0])
	for i := range cluster.Commands {
		if err := cluster.KillNode(i); err != nil {
			t.Fatalf("kill node %d: %v", i, err)
		}
	}
	for i := range cluster.Commands {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	for i, conn := range cluster.Clients {
		js[i], err = jetstream.New(conn)
		if err != nil {
			t.Fatal(err)
		}
	}
	until = time.Now().Add(30 * time.Second)
	var retained uint64
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		ready := true
		for _, name := range []string{"WF_INV", "WF_JRN", "WF_SIG", "WF_RUN"} {
			stream, lookupErr := js[0].Stream(attempt, name)
			if lookupErr != nil {
				ready = false
				break
			}
			info, lookupErr := stream.Info(attempt)
			if lookupErr != nil || info.Cluster == nil || info.Cluster.Leader == "" || len(info.Cluster.Replicas) != 2 {
				ready = false
				break
			}
			if name == "WF_SIG" {
				retained = info.State.Msgs
			}
		}
		stop()
		if ready && retained == count/2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if retained != count/2 {
		t.Fatalf("pre-restart signals retained=%d, want=%d (context=%v)", retained, count/2, ctx.Err())
	}
	send(count/2, count, js[0])
	var successor *worker.Worker
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, js[1], "signal-after-restart", map[string]worker.Handler{typ: handler})
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("worker after restart: %v (context=%v)", err, ctx.Err())
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- successor.RunPartition(workerCtx, part) }()
	result, err := client.New(js[0]).Await(ctx, typ, id)
	if err != nil || string(result) != "100" {
		t.Fatalf("result after restart=%s err=%v", result, err)
	}
	until = time.Now().Add(30 * time.Second)
	var pending uint64
	drained := false
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		run, lookupErr := js[0].Stream(attempt, "WF_RUN")
		if lookupErr == nil {
			var info *jetstream.StreamInfo
			info, lookupErr = run.Info(attempt)
			if lookupErr == nil {
				pending = info.State.Msgs
			}
		}
		stop()
		if lookupErr == nil && pending == 0 {
			drained = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !drained {
		t.Fatalf("run queue after 100 signals has %d messages (context=%v)", pending, ctx.Err())
	}
	stopWorker()
	if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("successor worker: %v", err)
	}
	records, _, err := journal.New(js[2]).Read(ctx, typ, id)
	if err != nil {
		t.Fatal(err)
	}
	var consumed int
	var previous uint64
	for _, record := range records {
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var signal struct {
			Sequence uint64 `json:"sig_seq"`
			Payload  []byte `json:"payload"`
		}
		if err := json.Unmarshal(record.Payload, &signal); err != nil || signal.Sequence <= previous || string(signal.Payload) != strconv.Itoa(consumed) {
			t.Fatalf("consumed signal %d: sequence=%d previous=%d payload=%q err=%v", consumed, signal.Sequence, previous, signal.Payload, err)
		}
		previous = signal.Sequence
		consumed++
	}
	if consumed != count || records[len(records)-1].Kind != journal.Completed {
		t.Fatalf("consumed=%d journal_entries=%d terminal=%s", consumed, len(records), records[len(records)-1].Kind)
	}
	if report, err := integrity.Check(ctx, js[0]); err != nil || report.Invocations != 1 || report.Terminal != 1 {
		t.Fatalf("retained integrity=%+v err=%v", report, err)
	}
	t.Logf("full server restart retained and consumed %d ordered signals; journal entries=%d", consumed, len(records))
}
