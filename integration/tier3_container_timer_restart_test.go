//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
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

func TestFiveContainerTimersSurviveFullRestart(t *testing.T) {
	if os.Getenv("WF_TIER3_CONTAINER") != "1" {
		t.Skip("set WF_TIER3_CONTAINER=1 for five-container NATS proof")
	}
	const typ, count = "tier3-restart-timer", 16
	const delay = 75 * time.Second
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
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
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
	workerConn, workerJS, err := connect(1)
	if err != nil {
		t.Fatal(err)
	}
	defer workerConn.Close()
	handler := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var index int
		if err := json.Unmarshal(raw, &index); err != nil {
			return nil, err
		}
		if err := wf.Sleep(c, "after-restart", delay); err != nil {
			return nil, err
		}
		return json.Marshal(index)
	}
	first, err := worker.New(ctx, workerJS, "tier3-timers-before-restart", map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	firstCtx, stopFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunAssigned(firstCtx, 0, 1) }()
	recorder := &history.Recorder{}
	defer func() {
		if path := os.Getenv("WF_TIER3_TIMER_HISTORY_OUT"); path != "" {
			file, err := os.Create(path)
			if err != nil {
				t.Errorf("create timer history: %v", err)
				return
			}
			if err := recorder.WriteJSONL(file); err != nil {
				t.Errorf("write timer history: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Errorf("close timer history: %v", err)
			}
		}
	}()
	startClient := client.NewObserved(beforeJS, recorder)
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("timer-%02d", index)
		if _, err := startClient.Start(ctx, typ, id, []byte(fmt.Sprint(index))); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	run, err := beforeJS.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	var pending uint64
	for until := time.Now().Add(time.Minute); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		info, infoErr := run.Info(attempt)
		stop()
		if infoErr == nil {
			pending = info.State.Msgs
			if first.Metrics().TimersScheduled == count && pending == count {
				break
			}
		}
		select {
		case err := <-firstDone:
			t.Fatalf("first worker exited before suspension: %v", err)
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	if first.Metrics().TimersScheduled != count || pending != count {
		t.Fatalf("timers before restart: scheduled=%d pending=%d want=%d", first.Metrics().TimersScheduled, pending, count)
	}
	stopFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first worker: %v", err)
	}
	fireAt := make([]time.Time, count)
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("timer-%02d", index)
		records, _, err := journal.New(beforeJS).Read(ctx, typ, id)
		if err != nil || len(records) != 3 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.Suspended {
			t.Fatalf("timer %s before restart: records=%+v err=%v", id, records, err)
		}
		var request struct {
			Kind   string    `json:"kind"`
			FireAt time.Time `json:"fire_at"`
		}
		if err := json.Unmarshal(records[1].Payload, &request); err != nil || request.Kind != "timer" || !time.Now().Before(request.FireAt) {
			t.Fatalf("timer %s fire_at=%s request=%s err=%v", id, request.FireAt, records[1].Payload, err)
		}
		fireAt[index] = request.FireAt
	}
	beforeConn.Close()
	workerConn.Close()
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
	if err := waitFiveReplicaReadiness(ctx, afterJS, identity.Partition(typ, "timer-00", provision.Partitions)); err != nil {
		t.Fatalf("five-replica timer readiness after restart: %v", err)
	}
	healedAt := time.Now()
	attempt, stopAttempt := context.WithTimeout(ctx, 5*time.Second)
	run, err = afterJS.Stream(attempt, "WF_RUN")
	stopAttempt()
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("timer-%02d", index)
		source := fmt.Sprintf("wf.schedule.%s.%s.0", typ, id)
		stored, err := run.GetLastMsgForSubject(ctx, source)
		if err != nil || stored.Header.Get(jetstream.ScheduleHeader) != "@at "+fireAt[index].UTC().Format(time.RFC3339Nano) {
			t.Fatalf("timer %s schedule after restart: message=%+v err=%v", id, stored, err)
		}
	}
	replacementConn, replacementJS, err := connect(2)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementConn.Close()
	replacement, err := worker.New(ctx, replacementJS, "tier3-timers-after-restart", map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	workCtx, stopWork := context.WithCancel(ctx)
	workDone := make(chan error, 1)
	go func() { workDone <- replacement.RunAssigned(workCtx, 0, 1) }()
	defer func() {
		stopWork()
		<-workDone
	}()
	jobs := make(chan int, count)
	readErrors := make(chan error, 1)
	var readers sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			c := client.NewObserved(afterJS, recorder)
			for index := range jobs {
				id := fmt.Sprintf("timer-%02d", index)
				value, err := c.Await(ctx, typ, id)
				if err != nil || string(value) != fmt.Sprint(index) {
					select {
					case readErrors <- fmt.Errorf("timer %s result=%s err=%v", id, value, err):
					default:
					}
					return
				}
			}
		}()
	}
	for index := 0; index < count; index++ {
		jobs <- index
	}
	close(jobs)
	readers.Wait()
	select {
	case err := <-readErrors:
		t.Fatal(err)
	default:
	}
	if result, err := history.CheckStarts(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("timer start history=%s: %v", result, err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("timer result history=%s: %v", result, err)
	}
	jrnStream, err := afterJS.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	var p99 time.Duration
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("timer-%02d", index)
		records, _, err := journal.New(afterJS).Read(ctx, typ, id)
		if err != nil || len(records) != 5 || records[4].Kind != journal.Completed {
			t.Fatalf("timer %s terminal journal=%+v err=%v", id, records, err)
		}
		raw, err := jrnStream.GetMsg(ctx, records[4].Sequence, jetstream.WithGetMsgSubject(identity.JournalSubject(typ, id)))
		if err != nil || raw.Time.Before(fireAt[index]) {
			t.Fatalf("timer %s completed early: message=%+v due=%s err=%v", id, raw, fireAt[index], err)
		}
		enabledAt := fireAt[index]
		if healedAt.After(enabledAt) {
			enabledAt = healedAt
		}
		if lateness := raw.Time.Sub(enabledAt); lateness > p99 {
			p99 = lateness
		}
	}
	var report integrity.Report
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		report, err = integrity.Check(attempt, afterJS)
		stop()
		if err == nil && report.Invocations == count && report.Journals == count && report.Entries == 5*count && report.Terminal == count {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || report.Invocations != count || report.Journals != count || report.Entries != 5*count || report.Terminal != count {
		t.Fatalf("timer restart retained state=%+v err=%v", report, err)
	}
	t.Logf("five-container timer restart count=%d post-heal p99=%s retained=%+v", count, p99, report)
	if p99 >= 30*time.Second {
		t.Fatalf("timer restart p99=%s exceeds 30s target", p99)
	}
}
