package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/internal/graphreplayfixture"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

// This is an internal admission fixture. It uses actual SDK checkpoints and
// independent deliveries; the public canonical continuation gate stays closed.
// Replay runs in the production CLI process, with a matching production plugin
// ABI, after all native servers have stopped.
func TestNativeGraphContinuationOfflineReplay(t *testing.T) {
	cli, plugin := buildGraphOfflineReplayTools(t)
	for _, replicas := range []int{1, 3} {
		for _, archive := range []bool{false, true} {
			t.Run(fmt.Sprintf("R%d-domain/archive=%t", replicas, archive), func(t *testing.T) {
				testNativeGraphContinuationOfflineReplay(t, cli, plugin, replicas, archive, nil)
			})
		}
	}
}

func TestNativeGraphContinuationChildOfflineReplay(t *testing.T) {
	cli, plugin := buildGraphOfflineReplayTools(t)
	for _, replicas := range []int{1, 3} {
		for _, archive := range []bool{false, true} {
			for _, cached := range []bool{false, true} {
				for _, failed := range []bool{false, true} {
					t.Run(fmt.Sprintf("R%d-domain/archive=%t/cached=%t/failed=%t", replicas, archive, cached, failed), func(t *testing.T) {
						mode := graphreplayfixture.ChildInput{Value: 7, Cached: cached, Failed: failed}
						testNativeGraphContinuationOfflineReplay(t, cli, plugin, replicas, archive, &mode)
					})
				}
			}
		}
	}
}

func buildGraphOfflineReplayTools(t *testing.T) (string, string) {
	t.Helper()
	buildCtx, stopBuild := context.WithTimeout(context.Background(), 3*time.Minute)
	defer stopBuild()
	root := graphOfflineTempDir(t)
	cli, plugin := filepath.Join(root, "wf"), filepath.Join(root, "handler.so")
	for _, args := range [][]string{
		{"build", "-race", "-o", cli, "./cmd/wf"},
		{"build", "-race", "-buildmode=plugin", "-o", plugin, "./worker/testdata/graphreplayplugin"},
	} {
		command := exec.CommandContext(buildCtx, "go", args...)
		command.Dir = ".."
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build %v: %v: %s", args, err, output)
		}
	}
	return cli, plugin
}

func testNativeGraphContinuationOfflineReplay(t *testing.T, cli, plugin string, replicas int, archive bool, childMode *graphreplayfixture.ChildInput) {
	const domain = "OFFLINEREPLAY"
	cluster, err := testcluster.StartWithDomain(graphOfflineTempDir(t), replicas, domain)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if replicas > 1 {
		for {
			ready := false
			for _, server := range cluster.Servers {
				ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
			}
			if ready {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	js, err := jetstream.NewWithDomain(cluster.Clients[0], domain)
	if err != nil {
		t.Fatal(err)
	}
	if err = provision.Ensure(ctx, js, replicas); err != nil {
		t.Fatal(err)
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: "OFFLINE_AUTH", AuthorityPrefix: "wf.graph.offline", ObjectBucket: "OFFLINE_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: archive}
	configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs {
		if _, err = js.CreateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sdk, err := client.NewWithGraphJournal(js, graph)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte(`7`)
	initial, middle, finish := graphreplayfixture.Initial, graphreplayfixture.Middle, graphreplayfixture.Finish
	stages := []string{"middle_v1", "finish_v1"}
	symbol, missingSymbol := "Workflow", "MissingContinuation"
	if childMode != nil {
		input, err = json.Marshal(childMode)
		if err != nil {
			t.Fatal(err)
		}
		initial, middle, finish = graphreplayfixture.ChildInitial, graphreplayfixture.ChildMiddle, graphreplayfixture.ChildFinish
		stages = []string{"child_middle_v1", "child_finish_v1"}
		symbol, missingSymbol = "ChildWorkflow", "MissingChildContinuation"
	}
	h, err := sdk.Start(ctx, "continued", "offline", input)
	if err != nil {
		t.Fatal(err)
	}
	graphreplayfixture.InitialCalls.Store(0)
	graphreplayfixture.MiddleCalls.Store(0)
	graphreplayfixture.FinishCalls.Store(0)
	graphreplayfixture.Effects.Store(0)
	graphreplayfixture.ChildCalls.Store(0)
	var childPromise wf.Promise
	leases, err := lease.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	deliverWorkflow := func(typ, id string) {
		t.Helper()
		runner, err := New(ctx, js, "offline-worker", map[string]Handler{h.Type: initial, graphreplayfixture.ChildType: graphreplayfixture.Child}, WithGraphJournal(graph))
		if err != nil {
			t.Fatal(err)
		}
		defer runner.Close()
		runner.continuations = map[string]map[string]ContinuationHandler{h.Type: {stages[0]: middle, stages[1]: finish}}
		owner, err := leases.Acquire(ctx, typ, id, "offline-worker")
		if err != nil {
			t.Fatal(err)
		}
		stageCtx, stop := context.WithCancel(ctx)
		heartbeat := make(chan error, 1)
		go func() {
			ticker := time.NewTicker(runner.heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stageCtx.Done():
					heartbeat <- nil
					return
				case <-ticker.C:
					renewCtx, stopRenew := context.WithTimeout(stageCtx, 3*time.Second)
					_, err := owner.RenewIfIdle(renewCtx, runner.heartbeatInterval)
					stopRenew()
					if err != nil {
						stop()
						heartbeat <- err
						return
					}
				}
			}
		}()
		var noOp bool
		err = runner.execute(stageCtx, typ, id, owner, time.Time{}, timerWakeup{}, &noOp, runner.deliveryOperations(typ, id, 0, 0))
		stop()
		heartbeatErr, releaseErr := <-heartbeat, owner.Release(ctx)
		if err != nil || releaseErr != nil || heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
			t.Fatal("delivery", err, heartbeatErr, releaseErr)
		}
	}
	deliver := func() { t.Helper(); deliverWorkflow(h.Type, h.ID) }
	authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
	if err != nil {
		t.Fatal(err)
	}
	port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
	if err != nil {
		t.Fatal(err)
	}
	retireChild := func() {
		t.Helper()
		if childMode == nil || graphreplayfixture.ChildCalls.Load() != 1 {
			t.Fatal("child retirement before completion")
		}
		childStatus, err := graph.InspectRetirement(ctx, childPromise.ChildType, childPromise.ChildID)
		if err != nil {
			t.Fatal(err)
		}
		childView, err := graph.Open(ctx, childPromise.ChildType, childPromise.ChildID, childStatus.Invocation)
		if err != nil {
			t.Fatal(err)
		}
		var receipts []journal.GraphPayloadLink
		if err = childView.ReadRange(ctx, 0, childView.Count(), func(record journal.GraphRecord) error {
			receipts = append(receipts, record.EntryBlob)
			receipts = append(receipts, record.Blobs...)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err = childView.Close(ctx); err != nil {
			t.Fatal(err)
		}
		// Duplicate child delivery must already be inert before its generation retires.
		deliverWorkflow(childPromise.ChildType, childPromise.ChildID)
		if graphreplayfixture.ChildCalls.Load() != 1 {
			t.Fatal("duplicate child reran")
		}
		if err = graph.Retire(ctx, childPromise.ChildType, childPromise.ChildID, childStatus.Invocation, childStatus.Tail); err != nil {
			t.Fatal(err)
		}
		if _, err = (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, time.Now().Add(2*time.Minute)); err != nil {
			t.Fatal(err)
		}
		for _, receipt := range receipts {
			if _, err = port.Get(ctx, receipt, journal.DefaultGraphPayloadLimit); !errors.Is(err, jetstream.ErrObjectNotFound) {
				t.Fatal("retired child receipt still exists", receipt, err)
			}
		}
		invocations, err := js.Stream(ctx, "WF_INV")
		if err != nil {
			t.Fatal(err)
		}
		if err = invocations.DeleteMsg(ctx, childStatus.Invocation); err != nil {
			t.Fatal(err)
		}
		if _, err = invocations.GetMsg(ctx, childStatus.Invocation); !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatal("child compatibility source absence unconfirmed", err)
		}
		resultBytes := graphreplayfixture.ChildResultBytes
		if childMode.Failed {
			resultBytes = 0
		}
		t.Logf("OFFLINE_CHILD_RECLAIMED source_receipts=%d compatibility_source_removed=true result_bytes=%d failed=%t cached=%t", len(receipts), resultBytes, childMode.Failed, childMode.Cached)
	}
	for index, stage := range stages {
		if childMode != nil && childMode.Cached && index == 1 {
			deliverWorkflow(childPromise.ChildType, childPromise.ChildID)
		}
		deliver()
		status, err := graph.InspectStart(ctx, h.Type, h.ID)
		if err != nil || status.Checkpoint == nil || status.Checkpoint.Stage != stage || !status.ContinuationReady() {
			t.Fatal("SDK checkpoint", stage, status, err)
		}
		if childMode != nil {
			view, err := graph.Open(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			found, err := view.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil || found == nil {
				t.Fatal("child checkpoint absent", err)
			}
			var frame checkpoint.Frame
			var locals graphreplayfixture.ChildLocals
			expectedOutcomes := 0
			if childMode.Cached && index == 1 {
				expectedOutcomes = 1
			}
			if json.Unmarshal(found.Frame, &frame) != nil || json.Unmarshal(frame.Data, &locals) != nil || frame.Stage != stage || locals.Count != index+1 || locals.Promise.ChildType != graphreplayfixture.ChildType || len(frame.PromiseOutcomes) != expectedOutcomes {
				t.Fatal("child checkpoint frame", frame, locals)
			}
			if index == 0 {
				childPromise = locals.Promise
			} else if childPromise != locals.Promise {
				t.Fatal("promise changed across checkpoints")
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("OFFLINE_CHILD_CHECKPOINT stage=%s count=%d cached_outcomes=%d child_calls=%d", stage, locals.Count, expectedOutcomes, graphreplayfixture.ChildCalls.Load())
		}
		if archive {
			// The owned range already reads relocated archive copies. Census
			// the old physical objects instead, and match them against the
			// logical entry hashes and currently owned receipts after sweep.
			before, err := port.Objects(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, time.Now().Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
			view, err := graph.Open(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			hashes, owned := map[string]bool{}, map[string]bool{}
			if err = view.ReadRange(ctx, 0, view.Count(), func(record journal.GraphRecord) error {
				hashes[record.EntryBlob.Hash] = true
				owned[record.EntryBlob.Reference.Object] = true
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			removed := 0
			for _, object := range before {
				if hashes[object.Key] && !owned[object.Reference.Object] {
					_, err := port.Get(ctx, journal.GraphPayloadLink{Hash: object.Key, Reference: object.Reference}, journal.MaxGraphEntryBytes)
					if !errors.Is(err, jetstream.ErrObjectNotFound) {
						t.Fatal("original entry receipt absence unconfirmed", object, err)
					}
					removed++
				}
			}
			if removed == 0 {
				t.Fatal("original checkpoint prefix was not collected")
			}
			t.Logf("OFFLINE_ARCHIVE stage=%s original_entry_receipts_removed=%d", stage, removed)
		}
	}
	if childMode != nil && childMode.Cached {
		retireChild()
	}
	deliver() // The final stage must actually suspend, never complete early.
	type bundle struct {
		Type      string            `json:"type"`
		ID        string            `json:"id"`
		InvSeq    uint64            `json:"inv_seq"`
		Input     []byte            `json:"input"`
		InputHash string            `json:"input_hash"`
		Journal   []journal.Record  `json:"journal"`
		Objects   map[string][]byte `json:"objects"`
	}
	version := 5
	if archive {
		version = 6
	}
	export := func(name string) (bundle, string) {
		t.Helper()
		command := exec.CommandContext(ctx, cli, "-url", cluster.Clients[0].ConnectedUrl(), "-domain", domain, "-replicas", strconv.Itoa(replicas), "-graph-authority-stream", cfg.AuthorityStream, "-graph-authority-prefix", cfg.AuthorityPrefix, "-graph-object-bucket", cfg.ObjectBucket, "-graph-cursor-version", strconv.Itoa(version), "export-replay", h.Type, h.ID)
		output, err := command.Output()
		if err != nil {
			t.Fatalf("export: %v", err)
		}
		var got bundle
		if err = json.Unmarshal(output, &got); err != nil {
			t.Fatal(err)
		}
		invocations, err := js.Stream(ctx, "WF_INV")
		if err != nil {
			t.Fatal(err)
		}
		invocation, err := invocations.GetMsg(ctx, h.InvSeq)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := ReadGraphReplaySnapshot(ctx, graph, h.Type, h.ID, invocation)
		if err != nil || got.Type != h.Type || got.ID != h.ID || got.InvSeq != h.InvSeq || !bytes.Equal(got.Input, input) || !sameOfflineJournal(got.Journal, snapshot.Records) || !reflect.DeepEqual(got.Objects, snapshot.Objects) {
			t.Fatal("export snapshot mismatch", err)
		}
		path := filepath.Join(graphOfflineTempDir(t), name+".json")
		if err = os.WriteFile(path, output, 0600); err != nil {
			t.Fatal(err)
		}
		return got, path
	}
	var pendingPath string
	var pendingJournal []journal.Record
	if childMode != nil && !childMode.Cached {
		pending, path := export("child-pending")
		waiting := "signal:" + childPromise.SignalName
		if len(pending.Journal) == 0 || pending.Journal[len(pending.Journal)-1].Kind != journal.Suspended || !bytes.Contains(pending.Journal[len(pending.Journal)-1].Payload, []byte(waiting)) || graphreplayfixture.ChildCalls.Load() != 0 {
			t.Fatal("parent did not really suspend before child execution")
		}
		pendingPath, pendingJournal = path, pending.Journal
		deliverWorkflow(childPromise.ChildType, childPromise.ChildID)
		deliver() // Consume and transfer the outcome before collecting its source.
		retireChild()
	}
	suspended, suspendedPath := export("suspended")
	if len(pendingJournal) > 0 && (len(suspended.Journal) < len(pendingJournal) || !sameOfflineJournal(pendingJournal, suspended.Journal[:len(pendingJournal)])) {
		t.Fatal("pending child history changed after outcome transfer/retirement")
	}
	if len(suspended.Journal) == 0 || suspended.Journal[len(suspended.Journal)-1].Kind != journal.Suspended || !bytes.Contains(suspended.Journal[len(suspended.Journal)-1].Payload, []byte("signal:gate")) {
		t.Fatal("no actual signal wait")
	}
	if _, err = sdk.Signal(ctx, h.Type, h.ID, "gate", []byte(`true`), "gate"); err != nil {
		t.Fatal(err)
	}
	deliver()
	result, err := sdk.Await(ctx, h.Type, h.ID)
	if err != nil || string(result) != "60" {
		t.Fatal("result", string(result), err)
	}
	completed, completedPath := export("completed")
	deliver() // completed duplicate must not reenter any SDK handler.
	expectedFinishCalls := int64(2)
	if childMode != nil && !childMode.Cached {
		expectedFinishCalls = 3
	}
	if graphreplayfixture.InitialCalls.Load() != 1 || graphreplayfixture.MiddleCalls.Load() != 1 || graphreplayfixture.FinishCalls.Load() != expectedFinishCalls || graphreplayfixture.Effects.Load() != 1 {
		t.Fatal("handler/effect counts", graphreplayfixture.InitialCalls.Load(), graphreplayfixture.MiddleCalls.Load(), graphreplayfixture.FinishCalls.Load(), graphreplayfixture.Effects.Load())
	}
	if !reflect.DeepEqual(suspended.Journal, completed.Journal[:len(suspended.Journal)]) {
		t.Fatal("suspended prefix changed")
	}
	keys, err := port.RootKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		root, err := port.ReadRoot(ctx, key)
		if err != nil || len(root.Readers) != 0 {
			t.Fatal("reader leak", root, err)
		}
	}
	cluster.Close() // Every replay below is offline, with a deliberately dead URL.
	marker := filepath.Join(graphOfflineTempDir(t), "effect")
	replay := func(path, symbol string) ([]byte, error) {
		command := exec.CommandContext(ctx, cli, "-url", "nats://127.0.0.1:1", "-handler-plugin", plugin, "-handler-symbol", symbol, "-replay-bundle", path, "replay")
		command.Env = append(os.Environ(), "WF_REPLAY_EFFECT_MARKER="+marker)
		return command.CombinedOutput()
	}
	replays := []struct{ path, status, wait, result string }{{suspendedPath, "suspended", "signal:gate", ""}, {completedPath, "completed", "", "60"}}
	if pendingPath != "" {
		replays = append(replays, struct{ path, status, wait, result string }{pendingPath, "suspended", "signal:" + childPromise.SignalName, ""})
	}
	for _, test := range replays {
		output, err := replay(test.path, symbol)
		var report struct {
			Status         string          `json:"status"`
			WaitingOn      string          `json:"waiting_on"`
			Result         json.RawMessage `json:"result"`
			JournalEntries int             `json:"journal_entries"`
		}
		if err != nil || json.Unmarshal(output, &report) != nil || report.Status != test.status || report.WaitingOn != test.wait || string(report.Result) != test.result {
			t.Fatalf("offline %s: %s: %v", test.status, output, err)
		}
		t.Logf("OFFLINE_REPLAY status=%s records=%d result=%s waiting_on=%s", report.Status, report.JournalEntries, report.Result, report.WaitingOn)
	}
	if output, err := replay(completedPath, missingSymbol); err == nil || !bytes.Contains(output, []byte(wf.ErrUnknownContinuation.Error())) {
		t.Fatalf("missing stage accepted: %s %v", output, err)
	}
	negativeControls := 2
	if childMode != nil && !childMode.Failed {
		var childRef string
		for _, record := range completed.Journal {
			if record.Kind != journal.SignalConsumed {
				continue
			}
			var event signalRecord
			if json.Unmarshal(record.Payload, &event) == nil && event.Child != nil {
				childRef = event.Child.Ref
			}
		}
		if childRef == "" || len(completed.Objects[childRef]) < graphreplayfixture.ChildResultBytes {
			t.Fatal("external child result absent from exported owned objects")
		}
		// Keep every checkpoint and signal object intact, removing only the
		// successful child's parent-owned external result.
		missingChild := completed
		missingChild.Objects = make(map[string][]byte, len(completed.Objects))
		for name, value := range completed.Objects {
			if name != childRef {
				missingChild.Objects[name] = value
			}
		}
		encoded, err := json.Marshal(missingChild)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(graphOfflineTempDir(t), "missing-child-result.json")
		if err = os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		if output, err := replay(path, symbol); err == nil || !bytes.Contains(output, []byte(wf.ErrReplayObjectMissing.Error())) || !bytes.Contains(output, []byte(childRef)) {
			t.Fatalf("missing child result accepted or misclassified: %s %v", output, err)
		}
		negativeControls++
	}
	// Missing owned frame bytes must be rejected rather than fetched live.
	for key := range completed.Objects {
		delete(completed.Objects, key)
	}
	corrupt, err := json.Marshal(completed)
	if err != nil {
		t.Fatal(err)
	}
	corruptPath := filepath.Join(graphOfflineTempDir(t), "missing-objects.json")
	if err = os.WriteFile(corruptPath, corrupt, 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := replay(corruptPath, symbol); err == nil || !bytes.Contains(output, []byte(wf.ErrReplayObjectMissing.Error())) {
		t.Fatalf("missing objects accepted: %s", output)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("offline replay ran an effect", err)
	}
	if childMode != nil {
		if graphreplayfixture.ChildCalls.Load() != 1 {
			t.Fatal("live child count", graphreplayfixture.ChildCalls.Load())
		}
		t.Logf("OFFLINE_CHILD_COMPLETE replicas=%d version=%d cached=%t failed=%t initial=1 middle=1 finish=%d effects=1 child=1 readers=0 negative_controls=%d", replicas, version, childMode.Cached, childMode.Failed, expectedFinishCalls, negativeControls)
		return
	}
	t.Logf("OFFLINE_COMPLETE replicas=%d version=%d initial=1 middle=1 finish=2 effects=1 readers=0 negative_controls=2", replicas, version)
}

// A qualification runner can retain native stores, CLI/plugin binaries and
// exported bundles outside /tmp; ordinary development tests clean themselves.
func graphOfflineTempDir(t *testing.T) string {
	t.Helper()
	if root := os.Getenv("WF_GRAPH_OFFLINE_ROOT"); root != "" {
		directory, err := os.MkdirTemp(root, "offline-")
		if err != nil {
			t.Fatal(err)
		}
		return directory
	}
	return t.TempDir()
}

// The CLI intentionally indents RawMessage payloads. Compare the complete
// serialized record arrays after JSON compaction; owned object bytes remain
// subject to exact byte comparisons above.
func sameOfflineJournal(a, b []journal.Record) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}
