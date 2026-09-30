//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
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
	const mixedSignalCount = 16
	const mixedTimerCount = 8
	if os.Getenv("WF_MIXED_CHAOS") != "1" {
		t.Skip("set WF_MIXED_CHAOS=1 for mixed real-cluster chaos")
	}
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(seed))
	root := t.TempDir()
	cluster, err := testcluster.StartPartitionableProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	defer func() {
		output := os.Getenv("FAULT_SCHEDULE_OUT")
		if !t.Failed() || output == "" {
			return
		}
		prefix := strings.TrimSuffix(output, filepath.Ext(output))
		for i := range cluster.Commands {
			data, readErr := os.ReadFile(cluster.LogPath(i))
			if readErr == nil {
				_ = os.WriteFile(fmt.Sprintf("%s-server-%d.log", prefix, i), data, 0644)
			}
		}
	}()
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
	if rng.Intn(2) == 1 {
		killed, other = other, killed
	}
	kinds := []string{"mixedshort", "mixedshort", "mixedshort", "mixedshort", "mixedtimer", "mixedtimer", "mixedtimer", "mixedsignal", "mixedsignal"}
	rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
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
		timers := make([]*wf.TimerHandle, mixedTimerCount)
		for n := range timers {
			timer, err := c.Timer(fmt.Sprintf("quorum-loss-%d", n), 10*time.Second)
			if err != nil {
				return nil, err
			}
			timers[n] = timer
		}
		for _, timer := range timers {
			if err := timer.Await(); err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`42`), nil
	}
	handlers["mixedsignal"] = func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if _, err := mark(input); err != nil {
			return nil, err
		}
		for n := 0; n < mixedSignalCount; n++ {
			value, err := wf.AwaitSignal(c, "go")
			if err != nil {
				return nil, err
			}
			if string(value) != strconv.Itoa(n) {
				return nil, fmt.Errorf("ordered signal %d carried %q", n, value)
			}
		}
		return json.RawMessage(`42`), nil
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
	timerDue := make([]time.Time, len(invocations))
	until = time.Now().Add(20 * time.Second)
	for time.Now().Before(until) {
		allSuspended = true
		for i, inv := range invocations {
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
			if inv.typ == "mixedtimer" {
				seenTimers := map[string]bool{}
				for _, record := range records {
					if record.Kind != journal.StepRequested {
						continue
					}
					var request struct {
						Kind   string    `json:"kind"`
						Name   string    `json:"name"`
						FireAt time.Time `json:"fire_at"`
					}
					if json.Unmarshal(record.Payload, &request) == nil && request.Kind == "timer_start" {
						if request.FireAt.IsZero() || seenTimers[request.Name] {
							t.Fatalf("timer %s/%s has missing deadline or duplicate name %q", inv.typ, inv.id, request.Name)
						}
						seenTimers[request.Name] = true
						if request.FireAt.After(timerDue[i]) {
							timerDue[i] = request.FireAt
						}
					}
				}
				if len(seenTimers) != mixedTimerCount || timerDue[i].IsZero() {
					allSuspended = false
				}
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
	pauseAt := int64(10 + rng.Intn(30))
	schedule := testcluster.FaultSchedule{Seed: seed, Events: []testcluster.FaultEvent{
		{Op: testcluster.SlowDisk, A: other, LatencyMillis: int64(25 + 5*rng.Intn(16))},
		{AtMillis: 0, Op: testcluster.PartitionNodes, A: killed, B: other},
		{AtMillis: pauseAt, Op: testcluster.PauseNode, A: leader},
		{AtMillis: pauseAt + int64(10+rng.Intn(30)), Op: testcluster.KillNode, A: killed},
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
	var healedAt time.Time
	until = time.Now().Add(20 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		stream, lookupErr := js[other].Stream(attempt, "WF_JRN")
		if lookupErr == nil {
			var info *jetstream.StreamInfo
			info, lookupErr = stream.Info(attempt)
			if lookupErr == nil && info.Cluster != nil && info.Cluster.Leader != "" {
				healedAt = time.Now()
			}
		}
		stop()
		if !healedAt.IsZero() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if healedAt.IsZero() {
		t.Fatal("journal quorum did not recover after route heal")
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
	var successor *worker.Worker
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		successor, err = worker.New(attempt, resumed, "mixed-after", handlers)
		stop()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("worker after four faults: %v (context: %v)", err, ctx.Err())
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
	releasedAt := time.Now()
	type completion struct {
		index int
		at    time.Time
		value json.RawMessage
		err   error
	}
	completed := make(chan completion, len(invocations))
	for i, inv := range invocations {
		go func(index int, inv invocation) {
			value, awaitErr := client.New(js[other]).Await(ctx, inv.typ, inv.id)
			completed <- completion{index: index, at: time.Now(), value: value, err: awaitErr}
		}(i, inv)
	}
	enabledAt := make([]time.Time, len(invocations))
	for i, inv := range invocations {
		enabledAt[i] = healedAt
		if inv.typ == "mixedshort" {
			enabledAt[i] = releasedAt
		}
		if inv.typ == "mixedtimer" && timerDue[i].After(healedAt) {
			enabledAt[i] = timerDue[i]
		}
	}
	for i, inv := range invocations {
		if inv.typ == "mixedsignal" {
			for n := 0; n < mixedSignalCount; n++ {
				until = time.Now().Add(20 * time.Second)
				for time.Now().Before(until) {
					attempt, stop := context.WithTimeout(ctx, 2*time.Second)
					_, err = c.Signal(attempt, inv.typ, inv.id, "go", []byte(strconv.Itoa(n)), fmt.Sprintf("mixed-signal-%d-%d", i, n))
					stop()
					if err == nil {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				if err != nil {
					t.Fatalf("signal %d/%d after heal: %v", i, n, err)
				}
			}
			enabledAt[i] = time.Now()
		}
	}
	latencies := make([]time.Duration, 0, len(invocations))
	for range invocations {
		result := <-completed
		inv := invocations[result.index]
		if result.err != nil || string(result.value) != "42" {
			t.Fatalf("mixed %s/%s result=%s err=%v", inv.typ, inv.id, result.value, result.err)
		}
		latency := result.at.Sub(enabledAt[result.index])
		if latency < 0 {
			latency = 0
		}
		t.Logf("mixed recovery type=%s id=%s latency=%s", inv.typ, inv.id, latency)
		latencies = append(latencies, latency)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p99 := latencies[(99*len(latencies)+99)/100-1]
	t.Logf("mixed provisional result observation: p99=%s max=%s (fan-out parent still measured from quorum)", p99, latencies[len(latencies)-1])
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
	var terminalStream jetstream.Stream
	restartedReadStarted := time.Now()
	readinessAttempts := 0
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		readinessAttempts++
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		terminalStream, err = third.Stream(attempt, "WF_JRN")
		if err == nil {
			var streamInfo *jetstream.StreamInfo
			streamInfo, err = terminalStream.Info(attempt)
			if err == nil && streamInfo.Cluster != nil && streamInfo.Cluster.Leader != "" {
				stop()
				break
			}
			if err == nil {
				err = fmt.Errorf("WF_JRN has no leader after restart")
			}
		}
		stop()
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || terminalStream == nil {
		t.Fatalf("restarted node WF_JRN readiness after %s and %d attempts: %v (test context: %v)", time.Since(restartedReadStarted), readinessAttempts, err, ctx.Err())
	}
	t.Logf("restarted node WF_JRN ready in %s after %d attempts", time.Since(restartedReadStarted), readinessAttempts)
	seen := map[string]bool{}
	for _, childID := range childIDs {
		if childID == "" || seen[childID] {
			t.Fatalf("duplicate or empty child ID: %q", childID)
		}
		seen[childID] = true
	}
	type terminalAudit struct {
		at      time.Time
		elapsed time.Duration
		err     error
	}
	auditOne := func(typ, id, want string) terminalAudit {
		started := time.Now()
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		result, err := client.New(third).Await(attempt, typ, id)
		stop()
		if err != nil || string(result) != want {
			return terminalAudit{elapsed: time.Since(started), err: fmt.Errorf("result=%s err=%v", result, err)}
		}
		until := time.Now().Add(15 * time.Second)
		var entry journal.Entry
		var readErr error
		for time.Now().Before(until) && ctx.Err() == nil {
			attempt, stop := context.WithTimeout(ctx, 3*time.Second)
			raw, err := terminalStream.GetLastMsgForSubject(attempt, identity.JournalSubject(typ, id))
			stop()
			readErr = err
			if readErr == nil {
				readErr = json.Unmarshal(raw.Data, &entry)
				if readErr == nil && entry.Kind == journal.Completed {
					return terminalAudit{at: raw.Time, elapsed: time.Since(started)}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return terminalAudit{elapsed: time.Since(started), err: fmt.Errorf("terminal kind=%s err=%v", entry.Kind, readErr)}
	}
	audits := make([]terminalAudit, len(invocations)+len(childIDs))
	var auditWorkers sync.WaitGroup
	for i, inv := range invocations {
		auditWorkers.Add(1)
		go func() {
			defer auditWorkers.Done()
			audits[i] = auditOne(inv.typ, inv.id, "42")
		}()
	}
	for i, childID := range childIDs {
		auditWorkers.Add(1)
		go func() {
			defer auditWorkers.Done()
			audits[len(invocations)+i] = auditOne("mixedchild", childID, "7")
		}()
	}
	auditWorkers.Wait()
	terminalAt := make([]time.Time, len(invocations))
	for i, inv := range invocations {
		audit := audits[i]
		if audit.err != nil {
			t.Fatalf("restarted node mixed %s/%s audit after %s: %v", inv.typ, inv.id, audit.elapsed, audit.err)
		}
		terminalAt[i] = audit.at
		t.Logf("restarted node terminal type=%s id=%s audit=%s", inv.typ, inv.id, audit.elapsed)
	}
	lastChildCompletedAt := healedAt
	for i, childID := range childIDs {
		audit := audits[len(invocations)+i]
		if audit.err != nil {
			t.Fatalf("restarted node child %s audit after %s: %v", childID, audit.elapsed, audit.err)
		}
		if audit.at.After(lastChildCompletedAt) {
			lastChildCompletedAt = audit.at
		}
		t.Logf("restarted node child id=%s audit=%s", childID, audit.elapsed)
	}
	enabledAt[len(invocations)-1] = lastChildCompletedAt
	var signalStream jetstream.Stream
	signalReadStarted := time.Now()
	signalReadAttempts := 0
	until = time.Now().Add(30 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		signalReadAttempts++
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		signalStream, err = js[other].Stream(attempt, "WF_SIG")
		if err == nil {
			var streamInfo *jetstream.StreamInfo
			streamInfo, err = signalStream.Info(attempt)
			if err == nil && streamInfo.Cluster != nil && streamInfo.Cluster.Leader != "" {
				stop()
				break
			}
			if err == nil {
				err = fmt.Errorf("WF_SIG has no leader after route heal")
			}
		}
		stop()
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || signalStream == nil {
		t.Fatalf("survivor node %d WF_SIG readiness after %s and %d attempts: %v (test context: %v)", other, time.Since(signalReadStarted), signalReadAttempts, err, ctx.Err())
	}
	t.Logf("survivor node %d WF_SIG ready in %s after %d attempts", other, time.Since(signalReadStarted), signalReadAttempts)
	for i, inv := range invocations {
		if inv.typ != "mixedsignal" {
			continue
		}
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		raw, readErr := signalStream.GetLastMsgForSubject(attempt, "wf.sig."+inv.typ+"."+inv.id+".go")
		stop()
		if readErr != nil {
			t.Fatalf("committed signal timestamp %s: %v", inv.id, readErr)
		}
		enabledAt[i] = healedAt
		if raw.Time.After(healedAt) {
			enabledAt[i] = raw.Time
		}
	}
	terminalLatencies := make([]time.Duration, 0, len(invocations))
	for i, inv := range invocations {
		latency := terminalAt[i].Sub(enabledAt[i])
		if latency < 0 {
			latency = 0
		}
		terminalLatencies = append(terminalLatencies, latency)
		t.Logf("mixed terminal type=%s id=%s latency=%s", inv.typ, inv.id, latency)
	}
	sort.Slice(terminalLatencies, func(i, j int) bool { return terminalLatencies[i] < terminalLatencies[j] })
	terminalP99 := terminalLatencies[(99*len(terminalLatencies)+99)/100-1]
	t.Logf("mixed terminal p99 from last enabling event=%s", terminalP99)
	t.Logf("mixed workers before=%+v after=%+v", first.Metrics(), successor.Metrics())
	if terminalP99 >= 30*time.Second {
		t.Errorf("mixed terminal p99=%s, want <30s", terminalP99)
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
				t.Fatalf("signal %s consumption %d: sequence=%d previous=%d payload=%q err=%v", inv.id, consumed, signal.Sequence, previous, signal.Payload, err)
			}
			previous = signal.Sequence
			consumed++
		}
		if consumed != mixedSignalCount {
			t.Fatalf("signal %s consumed entries=%d, want %d", inv.id, consumed, mixedSignalCount)
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
