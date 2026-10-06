package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"syscall"
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

// TestFiveHundredChildFanout interrupts the parent during child creation,
// then collects all child results after a restart.
func TestFiveHundredChildFanout(t *testing.T) {
	runFiveHundredChildFanout(t, false, false, false)
}

// The second fan-out run moves the journal leader after the parent has
// committed a seeded prefix of child requests.
func TestFiveHundredChildFanoutAcrossJournalLeaderRestart(t *testing.T) {
	runFiveHundredChildFanout(t, true, false, false)
}

func TestFiveHundredChildFanoutAfterParentSIGKILL(t *testing.T) {
	runFiveHundredChildFanout(t, false, true, false)
}

func TestFiveHundredChildFanoutAfterParentResultSIGKILL(t *testing.T) {
	runFiveHundredChildFanout(t, false, false, true)
}

// This opt-in matrix includes boundary positions that random interior cuts miss.
func TestFiveHundredChildFanoutParentBoundaryMatrix(t *testing.T) {
	if os.Getenv("WF_FANOUT_BOUNDARY_MATRIX") != "1" {
		t.Skip("set WF_FANOUT_BOUNDARY_MATRIX=1 for the six full 500-child parent cuts")
	}
	for _, phase := range []string{"create", "results"} {
		for _, position := range []string{"first", "interior", "last"} {
			t.Run(phase+"/"+position, func(t *testing.T) {
				// Empty means the production fixture's recorded seeded interior cut.
				cut := ""
				if position == "first" {
					cut = "0"
				}
				if position == "last" {
					cut = "499"
				}
				t.Setenv("WF_FANOUT_CHILD_CUT", "")
				t.Setenv("WF_FANOUT_RESULT_CUT", "")
				if phase == "create" {
					t.Setenv("WF_FANOUT_CHILD_CUT", cut)
				} else {
					t.Setenv("WF_FANOUT_RESULT_CUT", cut)
				}
				runFiveHundredChildFanout(t, false, phase == "create", phase == "results")
			})
		}
	}
}

func TestFiveHundredChildFanoutProcessChild(t *testing.T) {
	if os.Getenv("WF_FANOUT_PROCESS_CHILD") != "1" {
		t.Skip("500-child parent worker process helper")
	}
	url, marker := os.Getenv("WF_FANOUT_PROCESS_URL"), os.Getenv("WF_FANOUT_PROCESS_MARKER")
	cut, err := strconv.Atoi(os.Getenv("WF_FANOUT_PROCESS_CUT"))
	phase := os.Getenv("WF_FANOUT_PROCESS_PHASE")
	if url == "" || marker == "" || err != nil || cut < 0 || cut >= 500 || (phase != "results" && phase != "create") {
		t.Fatal("invalid 500-child process helper configuration")
	}
	traceOptions := fanoutProcessTrace(t, marker)
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	pause := func() error {
		if err := os.WriteFile(marker+".tmp", []byte(strconv.Itoa(cut)), 0600); err != nil {
			return err
		}
		if err := os.Rename(marker+".tmp", marker); err != nil {
			return err
		}
		select {}
	}
	handlers := fanoutHandlers(cut, pause)
	if phase == "results" {
		handlers = fanoutHandlersAtResults(cut, pause)
	}
	w, err := worker.New(context.Background(), js, "parent-process-before-kill", handlers, traceOptions...)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunPartition(context.Background(), identity.Partition("parent", "large-fanout", provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

func runFiveHundredChildFanout(t *testing.T, restartLeader, killParentProcess, killDuringResults bool) {
	t.Helper()
	seed, err := testcluster.SeedFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	const childCount = 500
	cut := 100 + rand.New(rand.NewSource(seed)).Intn(300)
	if raw := os.Getenv("WF_FANOUT_CHILD_CUT"); raw != "" {
		cut, err = strconv.Atoi(raw)
		if err != nil || cut < 0 || cut >= childCount {
			t.Fatalf("invalid WF_FANOUT_CHILD_CUT=%q", raw)
		}
	}
	resultCut := 100 + rand.New(rand.NewSource(seed^0x5c0115)).Intn(300)
	if raw := os.Getenv("WF_FANOUT_RESULT_CUT"); killDuringResults && raw != "" {
		resultCut, err = strconv.Atoi(raw)
		if err != nil || resultCut < 0 || resultCut >= childCount {
			t.Fatalf("invalid WF_FANOUT_RESULT_CUT=%q", raw)
		}
	}
	t.Logf("FAULT_SEED=%d parent_cut_after_child=%d", seed, cut)
	var all []jetstream.JetStream
	var cluster *testcluster.Cluster
	retainedRoot := ""
	if base := os.Getenv("WF_FANOUT_COMBINED_ROOT"); base != "" {
		if !filepath.IsAbs(base) {
			t.Fatal("absolute combined fanout root required")
		}
		retainedRoot = filepath.Join(base, t.Name())
		if err := os.MkdirAll(filepath.Dir(retainedRoot), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(retainedRoot, 0700); err != nil {
			t.Fatal(err)
		}
		cluster, err = testcluster.Start(filepath.Join(retainedRoot, "cluster"), 3)
		if err != nil {
			t.Fatal(err)
		}
		all, cluster = setupCluster(t, cluster)
	} else {
		all, cluster = setup(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	reached := make(chan struct{})
	release := make(chan struct{})
	var pauseOnce sync.Once
	afterChild := func() error {
		if !killParentProcess {
			pauseOnce.Do(func() {
				close(reached)
				<-release
			})
		}
		return nil
	}
	handlers := fanoutHandlers(cut, afterChild)
	if _, err := client.New(all[0]).Start(ctx, "parent", "large-fanout", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	parentPart := identity.Partition("parent", "large-fanout", provision.Partitions)
	var creationPrefix []journal.Record
	if killParentProcess {
		creationPrefix = killFanoutParentAtCut(t, ctx, all[0], cluster.Servers[1].ClientURL(), cut)
	} else {
		first, err := worker.New(ctx, all[1], "parent-before-cut", handlers)
		if err != nil {
			t.Fatal(err)
		}
		firstCtx, stopFirst := context.WithCancel(ctx)
		firstDone := make(chan error, 1)
		go func() { firstDone <- first.RunPartition(firstCtx, parentPart) }()
		select {
		case <-reached:
		case <-ctx.Done():
			t.Fatal("parent did not reach injected cut")
		}
		stopFirst()
		close(release)
		if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("first parent worker: %v", err)
		}
	}
	if restartLeader && !killDuringResults {
		stream, err := all[0].Stream(ctx, "WF_JRN")
		if err != nil {
			t.Fatal(err)
		}
		before, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		nodes := [3]jetstream.JetStream{all[0], all[1], all[2]}
		if retainedRoot != "" {
			restartFanoutJournalAtBoundary(t, ctx, all, cluster, retainedRoot, "create", creationPrefix)
		} else if err := restartJournalLeader(ctx, &nodes, cluster, before.State.Msgs); err != nil {
			t.Fatal(err)
		}
		if retainedRoot != "" {
			copy(nodes[:], all)
		}
		copy(all, nodes[:])
		t.Logf("restarted WF_JRN leader after %d child requests; retained journal messages=%d", cut+1, before.State.Msgs)
	}
	var dispatchMu sync.Mutex
	var parentDispatch []worker.DispatchEvent
	observeParent := func(event worker.DispatchEvent) {
		if event.Type != "parent" || event.ID != "large-fanout" {
			return
		}
		dispatchMu.Lock()
		defer dispatchMu.Unlock()
		if len(parentDispatch) == 256 {
			copy(parentDispatch, parentDispatch[1:])
			parentDispatch = parentDispatch[:255]
		}
		parentDispatch = append(parentDispatch, event)
	}
	second, err := worker.New(ctx, all[1], "parent-after-cut", handlers, worker.WithDispatchObserver(observeParent))
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.RunPartition(secondCtx, parentPart) }()
	j := journal.New(all[0])
	var childIDs []string
	logCreationFailure := func() {
		dispatchMu.Lock()
		events := append([]worker.DispatchEvent(nil), parentDispatch...)
		dispatchMu.Unlock()
		logFanoutCreationFailure(t, all, parentPart, len(childIDs), second.Metrics(), events)
	}
	for ctx.Err() == nil {
		records, _, err := j.Read(ctx, "parent", "large-fanout")
		if err != nil {
			logCreationFailure()
			t.Fatal(err)
		}
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
		if len(childIDs) == childCount && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(childIDs) != childCount {
		logCreationFailure()
		t.Fatalf("parent started %d children before timeout: %v", len(childIDs), ctx.Err())
	}
	stopSecond()
	if err := <-secondDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("second parent worker: %v", err)
	}
	seenIDs := map[string]bool{}
	parts := map[uint32]bool{}
	insideParent := 0
	for _, id := range childIDs {
		if id == "" || seenIDs[id] {
			t.Fatalf("duplicate or empty child id %q", id)
		}
		seenIDs[id] = true
		part := identity.Partition("child", id, provision.Partitions)
		if part == parentPart {
			insideParent++
		} else {
			parts[part] = true
		}
	}
	childHandlers := handlers
	var childOptions []worker.Option
	if retainedRoot != "" && killDuringResults {
		// Complete children sharing the parent's partition without collecting
		// parent results before the requested cut. A paused parent may hold one
		// slot while other slots run children and fence duplicate wakeups.
		parts[parentPart] = true
		childHandlers = map[string]worker.Handler{"child": handlers["child"], "parent": holdFanoutParent}
		childOptions = append(childOptions, worker.WithPartitionConcurrency(4))
	}
	childWorker, err := worker.New(ctx, all[2], "child-fanout-worker", childHandlers, childOptions...)
	if err != nil {
		t.Fatal(err)
	}
	childCtx, stopChildren := context.WithCancel(ctx)
	defer stopChildren()
	childDone := make([]chan error, 0, len(parts))
	for part := range parts {
		done := make(chan error, 1)
		childDone = append(childDone, done)
		go func(part uint32, done chan error) { done <- childWorker.RunPartition(childCtx, part) }(part, done)
	}
	if retainedRoot == "" || !killDuringResults {
		sig, err := all[0].Stream(ctx, "WF_SIG")
		if err != nil {
			t.Fatal(err)
		}
		for ctx.Err() == nil {
			info, err := sig.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if info.State.Msgs >= uint64(childCount-insideParent) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatalf("children outside parent partition did not finish: %v", ctx.Err())
		}
	}
	childrenJoined := false
	if retainedRoot != "" && killDuringResults {
		// Admit every child by identity; aggregate signal counts can include
		// shared-partition children and can shrink after signal consumption.
		outsideChecked := 0
		for index, id := range childIDs {
			value, err := client.New(all[0]).Await(ctx, "child", id)
			if err != nil || string(value) != strconv.Itoa(2*index) {
				t.Fatalf("prepared child %d result=%s err=%v", index, value, err)
			}
			if identity.Partition("child", id, provision.Partitions) != parentPart {
				outsideChecked++
			}
		}
		t.Logf("confirmed %d outside child results before worker stop", outsideChecked)
		t.Logf("confirmed %d child results including %d parent-partition children before worker stop", childCount, insideParent)
		stopChildren()
		for _, done := range childDone {
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		}
		childrenJoined = true
		prepareFanoutResultSignals(t, ctx, all[0], parentPart, childCount)
	}
	var killedPrefix []journal.Record
	if killDuringResults {
		t.Logf("FAULT_SEED=%d parent_cut_after_result=%d", seed, resultCut)
		killedPrefix = killFanoutParentAtBoundary(t, ctx, all[0], cluster.Servers[1].ClientURL(), resultCut, "results")
		if restartLeader {
			restartFanoutJournalAtBoundary(t, ctx, all, cluster, retainedRoot, "results", killedPrefix)
			j = journal.New(all[0])
		}
	}
	final, err := worker.New(ctx, all[1], "parent-after-children", handlers)
	if err != nil {
		t.Fatal(err)
	}
	finalCtx, stopFinal := context.WithCancel(ctx)
	defer stopFinal()
	finalDone := make(chan error, 1)
	go func() { finalDone <- final.RunPartition(finalCtx, parentPart) }()
	value, err := client.New(all[0]).Await(ctx, "parent", "large-fanout")
	if err != nil || string(value) != "249500" {
		t.Fatalf("parent result=%s err=%v", value, err)
	}
	stopFinal()
	if err := <-finalDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("final parent worker: %v", err)
	}
	stopChildren()
	if !childrenJoined {
		for _, done := range childDone {
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("child worker: %v", err)
			}
		}
	}
	if retainedRoot != "" && os.Getenv("WF_FANOUT_PHYSICAL_DRAIN") == "1" {
		drainFanoutTerminalWakeups(t, ctx, all, cluster, handlers, retainedRoot)
	}
	if killParentProcess || killDuringResults {
		finalRecords, _, err := j.Read(ctx, "parent", "large-fanout")
		if err != nil {
			t.Fatal(err)
		}
		for phase, prefix := range map[string][]journal.Record{"create": creationPrefix, "results": killedPrefix} {
			if len(prefix) == 0 {
				continue
			}
			if len(finalRecords) <= len(prefix) || !reflect.DeepEqual(prefix, finalRecords[:len(prefix)]) {
				t.Fatalf("parent changed durable %s prefix after SIGKILL", phase)
			}
			if finalRecords[len(finalRecords)-1].Epoch <= prefix[len(prefix)-1].Epoch {
				t.Fatalf("%s successor completed without a higher fencing epoch", phase)
			}
			t.Logf("FANOUT_PREFIX_PRESERVED phase=%s entries=%d old_epoch=%d final_epoch=%d", phase, len(prefix), prefix[len(prefix)-1].Epoch, finalRecords[len(finalRecords)-1].Epoch)
		}
	}
	report, err := integrity.Check(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	if report.Invocations != childCount+1 || report.Journals != childCount+1 || report.Terminal != childCount+1 {
		t.Fatalf("fan-out integrity: %+v", report)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != childCount+1 {
		t.Fatalf("invocation count=%d", info.State.Msgs)
	}
	for index, id := range childIDs {
		if killDuringResults {
			for _, peer := range []jetstream.JetStream{all[0], all[2]} {
				result, err := client.New(peer).Await(ctx, "child", id)
				if err != nil || string(result) != strconv.Itoa(2*index) {
					t.Fatalf("child %d result=%s err=%v", index, result, err)
				}
			}
		}
		message, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject("child", id))
		if err != nil || message.Header.Get(client.ParentTypeHeader) != "parent" || message.Header.Get(client.ParentIDHeader) != "large-fanout" {
			t.Fatalf("child %s invocation=%+v err=%v", id, message, err)
		}
	}
	if retainedRoot != "" {
		data, err := json.MarshalIndent(map[string]any{"child_count": childCount, "parent_result": string(value), "retained_integrity": report, "creation_prefix": creationPrefix, "result_prefix": killedPrefix, "seed": seed, "creation_cut": cut, "result_cut": resultCut}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(retainedRoot, "final-proof.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("completed %d children; %d shared the parent partition", childCount, insideParent)
}

// The polling read can outlive the fixture context during leader movement.
// Diagnose with a fresh, bounded context so the original error is not the only
// evidence of whether the parent or its observer stopped making progress.
func logFanoutCreationFailure(t *testing.T, nodes []jetstream.JetStream, partition uint32, childCount int, metrics worker.Metrics, events []worker.DispatchEvent) {
	t.Helper()
	t.Logf("fan-out creation failure: last observed children=%d replacement metrics=%+v", childCount, metrics)
	for _, event := range events {
		t.Logf("fan-out parent dispatch at=%s worker=%s stage=%s run_seq=%d delivery=%d err=%s", event.At.UTC().Format(time.RFC3339Nano), event.Worker, event.Stage, event.RunSequence, event.Delivery, event.Error)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for node, js := range nodes {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		stream, err := js.Stream(attempt, "WF_JRN")
		if err == nil {
			info, infoErr := stream.Info(attempt)
			if info != nil {
				t.Logf("fan-out node=%d journal state=%+v cluster=%+v err=%v", node, info.State, info.Cluster, infoErr)
			} else {
				t.Logf("fan-out node=%d journal info err=%v", node, infoErr)
			}
			last, lastErr := stream.GetLastMsgForSubject(attempt, identity.JournalSubject("parent", "large-fanout"))
			var entry journal.Entry
			if lastErr == nil {
				lastErr = json.Unmarshal(last.Data, &entry)
			}
			t.Logf("fan-out node=%d parent tail index=%d kind=%s epoch=%d worker=%s err=%v", node, entry.Index, entry.Kind, entry.Epoch, entry.WorkerID, lastErr)
		} else {
			t.Logf("fan-out node=%d journal lookup err=%v", node, err)
		}
		stop()
	}
	attempt, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	run, err := nodes[0].Stream(attempt, "WF_RUN")
	if err != nil {
		t.Logf("fan-out run lookup err=%v", err)
		return
	}
	info, err := run.Info(attempt)
	t.Logf("fan-out run info=%+v err=%v", info, err)
	consumer, err := run.Consumer(attempt, fmt.Sprintf("WF_P_%02d", partition))
	if err != nil {
		t.Logf("fan-out parent consumer lookup err=%v", err)
		return
	}
	consumerInfo, err := consumer.Info(attempt)
	t.Logf("fan-out parent consumer info=%+v err=%v", consumerInfo, err)
}

func fanoutHandlers(cut int, afterChild func() error) map[string]worker.Handler {
	return fanoutHandlersAtCuts(cut, afterChild, -1, nil)
}

func fanoutHandlersAtResults(cut int, afterResult func() error) map[string]worker.Handler {
	return fanoutHandlersAtCuts(-1, nil, cut, afterResult)
}

func fanoutHandlersAtCuts(cut int, afterChild func() error, resultCut int, afterResult func() error) map[string]worker.Handler {
	const childCount = 500
	return map[string]worker.Handler{
		"parent": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			promises := make([]wf.Promise, childCount)
			for i := range promises {
				input, _ := json.Marshal(i)
				promise, err := wf.CallAsync(c, "child", input)
				if err != nil {
					return nil, err
				}
				promises[i] = promise
				if i == cut {
					if err := afterChild(); err != nil {
						return nil, err
					}
				}
			}
			sum := 0
			for index, promise := range promises {
				value, err := wf.AwaitPromise(c, promise)
				if err != nil {
					return nil, err
				}
				var n int
				if err := json.Unmarshal(value, &n); err != nil {
					return nil, err
				}
				if index == resultCut {
					if err := afterResult(); err != nil {
						return nil, err
					}
				}
				sum += n
			}
			return json.Marshal(sum)
		},
		"child": func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			var n int
			if err := json.Unmarshal(input, &n); err != nil {
				return nil, err
			}
			value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { return n * 2, nil })
			if err != nil {
				return nil, err
			}
			return json.Marshal(value)
		},
	}
}

func killFanoutParentAtCut(t *testing.T, ctx context.Context, js jetstream.JetStream, url string, cut int) []journal.Record {
	return killFanoutParentAtBoundary(t, ctx, js, url, cut, "create")
}

func killFanoutParentAtBoundary(t *testing.T, ctx context.Context, js jetstream.JetStream, url string, cut int, phase string) []journal.Record {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if base := os.Getenv("WF_FANOUT_COMBINED_ROOT"); base != "" {
		root = filepath.Join(base, t.Name())
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	marker := filepath.Join(root, "parent-cut")
	logFile, err := os.Create(filepath.Join(root, "parent.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.Command(executable, "-test.run=^TestFiveHundredChildFanoutProcessChild$")
	cmd.Env = append(os.Environ(), "WF_FANOUT_PROCESS_CHILD=1", "WF_FANOUT_PROCESS_URL="+url, "WF_FANOUT_PROCESS_MARKER="+marker, "WF_FANOUT_PROCESS_CUT="+strconv.Itoa(cut), "WF_FANOUT_PROCESS_PHASE="+phase)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	until := time.Now().Add(30 * time.Second)
	if os.Getenv("WF_FANOUT_DIAGNOSTIC_TRACE") == "1" && os.Getenv("WF_FANOUT_COMBINED_ROOT") != "" {
		captureFanoutParentSDKAt(t, cmd, root, "actual-parent-sdk-start.json", nil)
	}
	for {
		data, err := os.ReadFile(marker)
		if err == nil {
			if string(data) != strconv.Itoa(cut) {
				t.Fatalf("parent cut marker=%q, want %d", data, cut)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) || ctx.Err() != nil || time.Now().After(until) {
			logs, _ := os.ReadFile(logFile.Name())
			t.Fatalf("parent process did not reach child cut: marker=%v ctx=%v logs=%s", err, ctx.Err(), logs)
		}
		time.Sleep(5 * time.Millisecond)
	}
	var records []journal.Record
	until = time.Now().Add(10 * time.Second)
	for time.Now().Before(until) && ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		records, _, err = journal.New(js).Read(attempt, "parent", "large-fanout")
		stop()
		if err == nil && fanoutCutMatches(records, cut, phase) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || !fanoutCutMatches(records, cut, phase) {
		t.Fatalf("parent journal before SIGKILL: phase=%s cut=%d entries=%d err=%v", phase, cut, len(records), err)
	}
	if os.Getenv("WF_FANOUT_COMBINED_ROOT") != "" {
		captureFanoutParentSDK(t, cmd, root, records)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := cmd.Wait()
	waited = true
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("parent process was not SIGKILLed: state=%v err=%v", cmd.ProcessState, waitErr)
	}
	t.Logf("SIGKILLed parent worker at %s cut %d; retained entries=%d", phase, cut, len(records))
	return records
}

func fanoutCutMatches(records []journal.Record, cut int, phase string) bool {
	if len(records) == 0 || records[len(records)-1].Kind != journal.StepCompleted {
		return false
	}
	if phase != "results" {
		return len(records) == 1+2*(cut+1)
	}
	var names []string
	results := 0
	pendingKind, pendingName := "", ""
	for _, record := range records {
		switch record.Kind {
		case journal.StepRequested:
			if pendingKind != "" {
				return false
			}
			var request struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			}
			if json.Unmarshal(record.Payload, &request) != nil {
				return false
			}
			pendingKind, pendingName = request.Kind, request.Name
		case journal.StepCompleted:
			switch pendingKind {
			case "call_async":
				names = append(names, pendingName)
			case "signal":
				if results >= len(names) || pendingName != names[results] {
					return false
				}
				results++
			default:
				return false
			}
			pendingKind, pendingName = "", ""
		case journal.Started, journal.Suspended, journal.SignalConsumed:
		default:
			return false
		}
	}
	return len(names) == 500 && results == cut+1 && pendingKind == ""
}
