//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
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
	runFiveContainerTimerRestart(t, false)
}

func TestFiveContainerThirtyDayTimerFiresAfterTwoRestarts(t *testing.T) {
	if os.Getenv("WF_TIER3_LONG_TIMER") != "1" {
		t.Skip("set WF_TIER3_LONG_TIMER=1 for advanced-clock thirty-day timer proof")
	}
	runFiveContainerTimerRestart(t, true)
}

func runFiveContainerTimerRestart(t *testing.T, longTimer bool) {
	t.Helper()
	typ, count, delay := "tier3-restart-timer", 16, 75*time.Second
	restarts := 1
	advance := time.Duration(0)
	var cluster *testcluster.DockerCluster
	var err error
	if longTimer {
		typ, count, delay = "tier3-thirty-day-timer", 1, 30*24*time.Hour
		restarts, advance = 2, 31*24*time.Hour
		cluster, err = testcluster.StartAdvancingClockDockerCluster(t.TempDir(), 5, advance)
	} else {
		cluster, err = testcluster.StartDockerCluster(t.TempDir(), 5)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	if longTimer {
		if err := cluster.AdvanceStoppedClusterClock(); err == nil {
			t.Fatal("clock advance accepted while servers were running")
		}
	}
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
			if first.Metrics().TimersScheduled == uint64(count) && pending == uint64(count) {
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
	if first.Metrics().TimersScheduled != uint64(count) || pending != uint64(count) {
		t.Fatalf("timers before restart: scheduled=%d pending=%d want=%d", first.Metrics().TimersScheduled, pending, count)
	}
	stopFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first worker: %v", err)
	}
	fireAt := make([]time.Time, count)
	prefixes := make([][]journal.Record, count)
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
		prefixes[index] = records
		if longTimer && (request.FireAt.Sub(time.Now()) < delay-time.Minute || request.FireAt.Sub(time.Now()) > delay+time.Minute) {
			t.Fatalf("timer deadline is not thirty days: %s", request.FireAt)
		}
	}
	beforeConn.Close()
	workerConn.Close()
	for cycle := 0; cycle < restarts; cycle++ {
		for i := 0; i < 5; i++ {
			if err := cluster.KillNode(i); err != nil {
				t.Fatalf("cycle %d kill node %d: %v", cycle, i, err)
			}
		}
		if longTimer && cycle == 1 {
			if err := cluster.AdvanceStoppedClusterClock(); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 5; i++ {
			if err := cluster.RestartNode(i); err != nil {
				t.Fatalf("cycle %d restart node %d: %v", cycle, i, err)
			}
		}
		phaseConn, phaseJS, err := connect(0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := waitRouteCounts(ctx, cluster, 0, 16, 30*time.Second); err != nil {
			phaseConn.Close()
			t.Fatal(err)
		}
		if err := waitFiveReplicaReadiness(ctx, phaseJS, identity.Partition(typ, "timer-00", provision.Partitions)); err != nil {
			phaseConn.Close()
			t.Fatal(err)
		}
		if longTimer {
			expected := time.Duration(0)
			if cycle == 1 {
				expected = advance
			}
			for i := 0; i < 5; i++ {
				attempt, done := context.WithTimeout(ctx, 2*time.Second)
				now, err := cluster.ServerNow(attempt, i)
				done()
				delta := now.Sub(time.Now())
				if err != nil || delta < expected-2*time.Second || delta > expected+2*time.Second {
					phaseConn.Close()
					t.Fatalf("cycle %d node %d clock=%s expected=%s err=%v", cycle, i, delta, expected, err)
				}
				t.Logf("restart_cycle=%d node=%d measured_server_offset=%s", cycle+1, i, delta)
			}
			prefix, _, err := journal.New(phaseJS).Read(ctx, typ, "timer-00")
			if err != nil || !reflect.DeepEqual(prefix, prefixes[0]) {
				phaseConn.Close()
				t.Fatalf("cycle %d changed journal prefix: %+v err=%v", cycle, prefix, err)
			}
			if cycle == 0 {
				stream, err := phaseJS.Stream(ctx, "WF_RUN")
				if err != nil {
					phaseConn.Close()
					t.Fatal(err)
				}
				scheduled, err := stream.GetLastMsgForSubject(ctx, fmt.Sprintf("wf.schedule.%s.timer-00.0", typ))
				if err != nil || scheduled.Header.Get(jetstream.ScheduleHeader) != "@at "+fireAt[0].UTC().Format(time.RFC3339Nano) {
					phaseConn.Close()
					t.Fatalf("thirty-day schedule lost after first restart: %+v %v", scheduled, err)
				}
				if early, err := stream.GetLastMsgForSubject(ctx, identity.RunSubject(typ, "timer-00", provision.Partitions)); !errors.Is(err, jetstream.ErrMsgNotFound) {
					phaseConn.Close()
					t.Fatalf("thirty-day timer delivered before advanced restart: %+v err=%v", early, err)
				}
			}
		}
		phaseConn.Close()
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
	if longTimer {
		healedAt = healedAt.Add(advance)
	}
	attempt, stopAttempt := context.WithTimeout(ctx, 5*time.Second)
	run, err = afterJS.Stream(attempt, "WF_RUN")
	stopAttempt()
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count && !longTimer; index++ {
		id := fmt.Sprintf("timer-%02d", index)
		source := fmt.Sprintf("wf.schedule.%s.%s.0", typ, id)
		stored, err := run.GetLastMsgForSubject(ctx, source)
		if err != nil || stored.Header.Get(jetstream.ScheduleHeader) != "@at "+fireAt[index].UTC().Format(time.RFC3339Nano) {
			t.Fatalf("timer %s schedule after restart: message=%+v err=%v", id, stored, err)
		}
	}
	if longTimer {
		var wakeup *jetstream.RawStreamMsg
		for until := time.Now().Add(10 * time.Second); time.Now().Before(until); {
			wakeup, err = run.GetLastMsgForSubject(ctx, identity.RunSubject(typ, "timer-00", provision.Partitions))
			if err == nil && wakeup.Header.Get(identity.TimerStepHeader) == "0" && !wakeup.Time.Before(fireAt[0]) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil || wakeup == nil || wakeup.Header.Get(identity.TimerStepHeader) != "0" || wakeup.Time.Before(fireAt[0]) {
			t.Fatalf("advanced server did not fire retained thirty-day schedule: %+v %v", wakeup, err)
		}
		t.Logf("thirty-day timer fired after two full restarts: server_advance=%s due=%s wakeup_time=%s", advance, fireAt[0], wakeup.Time)
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
	if longTimer {
		for node := 0; node < 5; node++ {
			nc, js, err := connect(node)
			if err != nil {
				t.Fatal(err)
			}
			value, err := client.NewObserved(js, recorder).Await(ctx, typ, "timer-00")
			nc.Close()
			if err != nil || string(value) != "0" {
				t.Fatalf("node %d thirty-day immutable result=%s err=%v", node, value, err)
			}
		}
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
