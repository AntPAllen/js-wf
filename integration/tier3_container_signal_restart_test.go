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
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFiveContainerSignalsSurviveFullRestart(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	const typ, id, count = "tier3-restart-signal", "ordered", 32
	cluster, err := testcluster.StartDockerCluster(t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		if !t.Failed() {
			return
		}
		for i := 0; i < 5; i++ {
			if logs, err := cluster.Logs(i); err == nil {
				if len(logs) > 8192 {
					logs = logs[len(logs)-8192:]
				}
				t.Logf("node %d log tail:\n%s", i, logs)
			}
		}
	}()
	t.Logf("file_store_sync_interval=%s", cluster.SyncInterval())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	connect := func(node int) (*nats.Conn, jetstream.JetStream, error) {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			return nil, nil, err
		}
		js, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, nil, err
		}
		return nc, js, nil
	}
	beforeConn, beforeJS, err := connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer beforeConn.Close()
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		err = provision.Ensure(attempt, beforeJS, 5)
		stop()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("provision five-replica stores: %v", err)
	}
	partition := identity.Partition(typ, id, provision.Partitions)
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_SIGNAL_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create signal history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write signal history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close signal history: %v", err)
			}
		}
	}()
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < count; i++ {
			payload, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			if string(payload) != strconv.Itoa(i) {
				return nil, fmt.Errorf("signal %d carried %q", i, payload)
			}
		}
		return json.RawMessage(`32`), nil
	}
	firstConn, firstJS, err := connect(1)
	if err != nil {
		t.Fatal(err)
	}
	defer firstConn.Close()
	first, err := worker.New(ctx, firstJS, "tier3-signals-before-restart", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunPartition(firstCtx, partition) }()
	if err := waitFiveReplicaReadiness(ctx, beforeJS, partition); err != nil {
		t.Fatalf("five-replica readiness: %v", err)
	}
	c := client.NewObserved(beforeJS, recorder)
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	suspended := false
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		records, _, readErr := journal.New(beforeJS).Read(attempt, typ, id)
		stop()
		if readErr == nil && len(records) != 0 && records[len(records)-1].Kind == journal.Suspended {
			suspended = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !suspended {
		t.Fatal("workflow did not suspend before signal publication")
	}
	stopFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first worker: %v", err)
	}
	send := func(from, to int, c *client.Client) {
		t.Helper()
		for i := from; i < to; i++ {
			key := fmt.Sprintf("ordered-%03d", i)
			seq, err := c.Signal(ctx, typ, id, "go", []byte(strconv.Itoa(i)), key)
			if err != nil || seq == 0 {
				t.Fatalf("signal %d sequence=%d: %v", i, seq, err)
			}
		}
	}
	send(0, count/2, c)
	beforeConn.Close()
	firstConn.Close()
	for i := 0; i < 5; i++ {
		if err := cluster.KillNode(i); err != nil {
			t.Fatalf("kill node %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := cluster.RestartNode(i); err != nil {
			t.Fatalf("restart node %d: %v", i, err)
		}
	}
	afterConn, afterJS, err := connect(0)
	if err != nil {
		t.Fatal(err)
	}
	defer afterConn.Close()
	if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
		t.Fatalf("five-node routes after restart: %v", err)
	}
	if err := waitFiveReplicaReadiness(ctx, afterJS, partition); err != nil {
		t.Fatalf("five-replica signal readiness after restart: %v", err)
	}
	attempt, stop := context.WithTimeout(ctx, 5*time.Second)
	stream, err := afterJS.Stream(attempt, "WF_SIG")
	if err == nil {
		var info *jetstream.StreamInfo
		info, err = stream.Info(attempt)
		if err == nil && info.State.Msgs != count/2 {
			err = fmt.Errorf("retained signals=%d want=%d", info.State.Msgs, count/2)
		}
	}
	stop()
	if err != nil {
		t.Fatalf("signals retained after restart: %v", err)
	}
	afterClient := client.NewObserved(afterJS, recorder)
	send(count/2, count, afterClient)
	enablingAt := time.Now()
	retry, err := afterClient.Signal(ctx, typ, id, "go", []byte("0"), "ordered-000")
	if err != nil || retry == 0 {
		t.Fatalf("duplicate signal after restart: sequence=%d err=%v", retry, err)
	}
	if _, err := afterClient.Signal(ctx, typ, id, "go", []byte("changed"), "ordered-000"); !errors.Is(err, client.ErrSignalMismatch) {
		t.Fatalf("changed signal after restart: %v", err)
	}
	replacementConn, replacementJS, err := connect(2)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementConn.Close()
	replacement, err := worker.New(ctx, replacementJS, "tier3-signals-after-restart", map[string]worker.Handler{typ: handler})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	go func() { workDone <- replacement.RunPartition(workCtx, partition) }()
	defer func() {
		stopWork()
		<-workDone
	}()
	value, err := afterClient.Await(ctx, typ, id)
	if err != nil || string(value) != "32" {
		t.Fatalf("result after restart=%s err=%v", value, err)
	}
	recoveryLatency := time.Since(enablingAt)
	if recoveryLatency >= 30*time.Second {
		t.Errorf("signal terminal recovery=%s from last enabling signal, want <30s", recoveryLatency)
	}
	readConn, readJS, err := connect(3)
	if err != nil {
		t.Fatal(err)
	}
	defer readConn.Close()
	value, err = client.NewObserved(readJS, recorder).Await(ctx, typ, id)
	if err != nil || string(value) != "32" {
		t.Fatalf("cross-node result=%s err=%v", value, err)
	}
	records, _, err := journal.New(readJS).Read(ctx, typ, id)
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
		t.Fatalf("consumed=%d journal entries=%d terminal=%s", consumed, len(records), records[len(records)-1].Kind)
	}
	operations := recorder.Snapshot()
	if result, err := history.CheckStarts(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("start history=%s: %v", result, err)
	}
	if result, err := history.CheckSignals(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("signal history=%s: %v", result, err)
	}
	if result, err := history.CheckResults(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("result history=%s: %v", result, err)
	}
	report, err := integrity.Check(ctx, afterJS)
	if err != nil || report.Invocations != 1 || report.Journals != 1 || report.Terminal != 1 {
		t.Fatalf("retained audit=%+v: %v", report, err)
	}
	t.Logf("five-container full restart retained and consumed %d ordered signals; journal entries=%d recovery_from_last_signal=%s", consumed, len(records), recoveryLatency)
}
