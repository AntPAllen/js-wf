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
	for _, replicas := range []int{1, 3} {
		for _, archive := range []bool{false, true} {
			t.Run(fmt.Sprintf("R%d-domain/archive=%t", replicas, archive), func(t *testing.T) {
				testNativeGraphContinuationOfflineReplay(t, cli, plugin, replicas, archive)
			})
		}
	}
}

func testNativeGraphContinuationOfflineReplay(t *testing.T, cli, plugin string, replicas int, archive bool) {
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
	h, err := sdk.Start(ctx, "continued", "offline", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	graphreplayfixture.InitialCalls.Store(0)
	graphreplayfixture.MiddleCalls.Store(0)
	graphreplayfixture.FinishCalls.Store(0)
	graphreplayfixture.Effects.Store(0)
	leases, err := lease.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	deliver := func() {
		t.Helper()
		runner, err := New(ctx, js, "offline-worker", map[string]Handler{h.Type: graphreplayfixture.Initial}, WithGraphJournal(graph))
		if err != nil {
			t.Fatal(err)
		}
		defer runner.Close()
		runner.continuations = map[string]map[string]ContinuationHandler{h.Type: {"middle_v1": graphreplayfixture.Middle, "finish_v1": graphreplayfixture.Finish}}
		owner, err := leases.Acquire(ctx, h.Type, h.ID, "offline-worker")
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
		err = runner.execute(stageCtx, h.Type, h.ID, owner, time.Time{}, timerWakeup{}, &noOp, runner.deliveryOperations(h.Type, h.ID, 0, 0))
		stop()
		heartbeatErr, releaseErr := <-heartbeat, owner.Release(ctx)
		if err != nil || releaseErr != nil || heartbeatErr != nil && !errors.Is(heartbeatErr, context.Canceled) {
			t.Fatal("delivery", err, heartbeatErr, releaseErr)
		}
	}
	authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
	if err != nil {
		t.Fatal(err)
	}
	port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"middle_v1", "finish_v1"} {
		deliver()
		status, err := graph.InspectStart(ctx, h.Type, h.ID)
		if err != nil || status.Checkpoint == nil || status.Checkpoint.Stage != stage || !status.ContinuationReady() {
			t.Fatal("SDK checkpoint", stage, status, err)
		}
		if archive {
			before, err := graph.Open(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			checkpoint, err := before.ReadCheckpoint(ctx, h.Type, h.ID)
			if err != nil || checkpoint == nil {
				t.Fatal("checkpoint absent", err)
			}
			var originals []journal.GraphPayloadLink
			if err = before.ReadRange(ctx, 0, checkpoint.Request.Index, func(record journal.GraphRecord) error { originals = append(originals, record.EntryBlob); return nil }); err != nil {
				t.Fatal(err)
			}
			if err = before.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = (graphpublication.Protocol{Port: port}).SweepWithReaders(ctx, time.Now().Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
			removed := 0
			for _, receipt := range originals {
				_, err := port.Get(ctx, receipt, journal.MaxGraphEntryBytes)
				if errors.Is(err, jetstream.ErrObjectNotFound) {
					removed++
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if removed == 0 {
				t.Fatal("original checkpoint prefix was not collected")
			}
			t.Logf("OFFLINE_ARCHIVE stage=%s original_entry_receipts_removed=%d", stage, removed)
		}
	}
	deliver() // final stage must actually wait for the canonical signal.
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
		if err != nil || got.Type != h.Type || got.ID != h.ID || got.InvSeq != h.InvSeq || !bytes.Equal(got.Input, []byte(`7`)) || !reflect.DeepEqual(got.Journal, snapshot.Records) || !reflect.DeepEqual(got.Objects, snapshot.Objects) {
			t.Fatal("export snapshot mismatch", err)
		}
		path := filepath.Join(graphOfflineTempDir(t), name+".json")
		if err = os.WriteFile(path, output, 0600); err != nil {
			t.Fatal(err)
		}
		return got, path
	}
	suspended, suspendedPath := export("suspended")
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
	if graphreplayfixture.InitialCalls.Load() != 1 || graphreplayfixture.MiddleCalls.Load() != 1 || graphreplayfixture.FinishCalls.Load() != 2 || graphreplayfixture.Effects.Load() != 1 {
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
	for _, test := range []struct{ path, status, wait, result string }{{suspendedPath, "suspended", "signal:gate", ""}, {completedPath, "completed", "", "60"}} {
		output, err := replay(test.path, "Workflow")
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
	if output, err := replay(completedPath, "MissingContinuation"); err == nil || !bytes.Contains(output, []byte(wf.ErrUnknownContinuation.Error())) {
		t.Fatalf("missing stage accepted: %s %v", output, err)
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
	if output, err := replay(corruptPath, "Workflow"); err == nil || !bytes.Contains(output, []byte(wf.ErrReplayObjectMissing.Error())) {
		t.Fatalf("missing objects accepted: %s", output)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("offline replay ran an effect", err)
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
