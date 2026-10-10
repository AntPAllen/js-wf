package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"
)

func TestGraphOperatorRejectsPartialOrLegacyOnlySelection(t *testing.T) {
	for _, args := range [][]string{
		{"-graph-cursor-version", "6", "export-journal", "test", "id"},
		{"-graph-view-namespace", "graph-test", "list"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-postgres-dsn", "postgres://invalid", "list"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-graph-view-namespace", "graph-test", "list"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-postgres-dsn", "postgres://invalid", "-graph-view-namespace", "graph-test", "-graph-view-bucket", "VIEW", "project"},

		{"-graph-authority-stream", "AUTH", "result", "test", "id"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "journal-capacity"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "scan-tombstones"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "list"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-graph-view-bucket", "WF_VIEW", "list"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-graph-view-bucket", "WF_STATE", "project"},
	} {
		err := run(args, &bytes.Buffer{})
		if err == nil || !(strings.Contains(err.Error(), "requires") || strings.Contains(err.Error(), "migration")) {
			t.Fatalf("args=%v err=%v", args, err)
		}
	}
}

func TestNativeCanonicalGraphOperatorCommands(t *testing.T) {
	testNativeCanonicalGraphOperatorCommands(t, false)
}
func TestNativeCanonicalGraphPostgresOperatorCommands(t *testing.T) {
	if os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN")
	}
	testNativeCanonicalGraphOperatorCommands(t, true)
}
func testNativeCanonicalGraphOperatorCommands(t *testing.T, postgres bool) {
	marker := filepath.Join(operatorTempDir(t), "replay-effect")
	t.Setenv("WF_REPLAY_EFFECT_MARKER", marker)
	for _, domain := range []string{"", "WFGRAPHOPS"} {
		count := 1
		name := "R1"
		if domain != "" {
			count = 3
			name = "R3Domain"
		}
		t.Run(name, func(t *testing.T) {
			var cluster *testcluster.Cluster
			var err error
			if domain == "" {
				cluster, err = testcluster.Start(operatorTempDir(t), count)
			} else {
				cluster, err = testcluster.StartWithDomain(operatorTempDir(t), count, domain)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			var js jetstream.JetStream
			if domain == "" {
				js, err = jetstream.New(cluster.Clients[0])
			} else {
				js, err = jetstream.NewWithDomain(cluster.Clients[0], domain)
			}
			if err != nil {
				t.Fatal(err)
			}
			if count > 1 {
				for ctx.Err() == nil {
					ready := false
					for _, server := range cluster.Servers {
						ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == count
					}
					if ready {
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			if err = provision.Ensure(ctx, js, count); err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "OP_GRAPH_AUTH", AuthorityPrefix: "wf.graph.operator", ObjectBucket: "OP_GRAPH_OBJECTS", ExpectedReplicas: count, CanonicalStarts: true, CanonicalSignals: true}
			configs, err := journal.NativeGraphStreamConfigs(cfg, count)
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
			var observed, wrong, requests atomic.Int64
			opts := []jetstream.JetStreamOpt{}
			opts = append(opts, jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
				requests.Add(1)
				if domain != "" {
					if strings.HasPrefix(subject, "$JS."+domain+".API.") {
						observed.Add(1)
					} else {
						wrong.Add(1)
					}
				}
			}}))
			base := []string{"-url", cluster.Servers[0].ClientURL(), "-timeout", "45s", "-replicas", strconv.Itoa(count), "-graph-authority-stream", cfg.AuthorityStream, "-graph-authority-prefix", cfg.AuthorityPrefix, "-graph-object-bucket", cfg.ObjectBucket}
			if domain != "" {
				base = append(base, "-domain", domain)
			}
			call := func(args ...string) ([]byte, error) {
				phase := "other"
			operation:
				for _, arg := range args {
					switch arg {
					case "start", "signal", "result", "export-journal", "export-replay", "replay", "describe", "audit", "list", "lag", "cancel", "purge", "history", "scans", "scan-start", "scan-signal", "scan-terminal", "scan-timer", "scan-suspended":
						phase = arg
						break operation
					}
				}
				started, before := time.Now(), requests.Load()
				deadline, _ := ctx.Deadline()
				t.Logf("GRAPH_OPERATOR_CALL_BEGIN operation=%s domain=%s fixture_remaining=%s", phase, domain, time.Until(deadline))
				var out bytes.Buffer
				err := runWithJetStreamOptions(append(append([]string(nil), base...), args...), &out, opts...)
				t.Logf("GRAPH_OPERATOR_CALL_END operation=%s domain=%s wall=%s api_requests=%d fixture_remaining=%s error=%v", phase, domain, time.Since(started), requests.Load()-before, time.Until(deadline), err)
				return out.Bytes(), err
			}
			const viewBucket = "OP_GRAPH_VIEW"
			if _, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: viewBucket, Replicas: count}); err != nil {
				t.Fatal(err)
			}
			viewArgs := []string{"-graph-view-bucket", viewBucket}
			projectionOpts := []visibility.Option{visibility.WithGraphJournal(graph, viewBucket), visibility.WithGraphRefreshInterval(100 * time.Millisecond)}
			if postgres {
				dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
				db, err := sql.Open("pgx", dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				namespace := t.Name()
				viewArgs = []string{"-postgres-dsn", dsn, "-graph-view-namespace", namespace}
				projectionOpts = []visibility.Option{visibility.WithGraphJournal(graph, namespace), visibility.WithPostgres(&visibility.PostgresStore{DB: db, Namespace: namespace})}
			}
			projection, err := visibility.New(ctx, js, projectionOpts...)
			if err != nil {
				t.Fatal(err)
			}
			projectArgs := append(append(append([]string(nil), base...), viewArgs...), "-interval", "100ms", "project")
			encodedArgs, _ := json.Marshal(projectArgs)
			projectRoot := operatorTempDir(t)
			projectLog, err := os.Create(filepath.Join(projectRoot, "project.log"))
			if err != nil {
				t.Fatal(err)
			}
			defer projectLog.Close()
			projectProcess := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOperatorDaemonProcessHelper$")
			projectProcess.Env = append(os.Environ(), "WF_OPERATOR_DAEMON_ARGS="+string(encodedArgs), "WF_OPERATOR_DAEMON_READY="+filepath.Join(projectRoot, "ready"), "WF_OPERATOR_DAEMON_STAGE=running")
			projectProcess.Stdout, projectProcess.Stderr = projectLog, projectLog
			if err = projectProcess.Start(); err != nil {
				t.Fatal(err)
			}
			projectDone := make(chan error, 1)
			go func() { projectDone <- projectProcess.Wait() }()
			defer func() {
				_ = projectProcess.Process.Signal(syscall.SIGTERM)
				select {
				case e := <-projectDone:
					if e != nil {
						t.Error("graph project daemon", e)
					}
					trace, e := os.ReadFile(filepath.Join(projectRoot, "project.log"))
					if e != nil || strings.Contains(string(trace), ".WF_JRN") {
						t.Error("graph project used legacy journal", e)
					}
					if domain != "" && (!strings.Contains(string(trace), "request=$JS."+domain+".API.") || strings.Contains(string(trace), "request=$JS.API.")) {
						t.Error("graph project domain API mismatch")
					}
					if postgres {
						if err := projection.Rebuild(ctx); err != nil {
							t.Error("graph PostgreSQL rebuild after projector shutdown", err)
						}
						if _, err := projection.Get(ctx, "graph-operator", "success"); !errors.Is(err, visibility.ErrNotFound) {
							t.Error("purged PostgreSQL row retained after rebuild", err)
						}
					}
					t.Log("graph project subprocess joined; legacy journal requests zero and domain API verified")
				case <-time.After(5 * time.Second):
					_ = projectProcess.Process.Kill()
					<-projectDone
					t.Error("graph project daemon did not stop")
				}
			}()
			var effects atomic.Int64
			const typ = "graph-operator"
			handlers := map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				value, e := wf.AwaitSignal(c, "go")
				if e != nil {
					return nil, e
				}
				_, e = wf.Run(c, "once", 0, func(context.Context) (int, error) { effects.Add(1); return 7, nil })
				return json.RawMessage(value), e
			}}
			w, err := worker.New(ctx, js, "graph-operator-worker", handlers, worker.WithGraphJournal(graph))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			partitionSet := map[uint32]bool{}
			for _, id := range []string{"success", "cancelled"} {
				partitionSet[identity.Partition(typ, id, provision.Partitions)] = true
			}
			partitions := []uint32{}
			for partition := range partitionSet {
				partitions = append(partitions, partition)
			}
			done := make(chan error, 1)
			go func() { done <- w.RunPartitions(runCtx, partitions) }()
			joined := false
			defer func() {
				if !joined {
					stop()
					if err := <-done; err != nil {
						t.Error(err)
					}
				}
			}()
			payload, _ := json.Marshal(strings.Repeat("owned", 1<<20))
			file := filepath.Join(operatorTempDir(t), "payload.json")
			if err = os.WriteFile(file, payload, 0600); err != nil {
				t.Fatal(err)
			}
			data, err := call("-input-file", file, "start", typ, "success")
			if err != nil {
				t.Fatal(err)
			}
			var handle client.Handle
			if err = json.Unmarshal(data, &handle); err != nil || handle.InvSeq == 0 {
				t.Fatal(handle, err)
			}
			if _, err = call("-input-file", file, "signal", typ, "success", "go", "key"); err != nil {
				t.Fatal(err)
			}
			data, err = call("result", typ, "success")
			if err != nil {
				t.Fatal(err)
			}
			var got string
			if err = json.Unmarshal(data, &got); err != nil || got != strings.Repeat("owned", 1<<20) {
				t.Fatal("large canonical result differs", err)
			}
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = state.Put(ctx, identity.Key(typ, "success"), []byte(`{"status":"completed","result":false}`)); err != nil {
				t.Fatal(err)
			}
			again, err := call("result", typ, "success")
			if err != nil || !bytes.Equal(data, again) {
				t.Fatal("projection overrode canonical result", err)
			}
			journalBytes, err := call("export-journal", typ, "success")
			if err != nil {
				t.Fatal(err)
			}
			var records []journal.Record
			if err = json.Unmarshal(journalBytes, &records); err != nil || len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
				t.Fatal("missing canonical journal", err)
			}
			// Replay export must survive source deletion and cannot use legacy blobs.
			signals, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				t.Fatal(err)
			}
			if err = signals.Purge(ctx); err != nil {
				t.Fatal(err)
			}
			if err = js.DeleteObjectStore(ctx, "WF_BLOB"); err != nil {
				t.Fatal(err)
			}
			bundleBytes, err := call("export-replay", typ, "success")
			if err != nil {
				t.Fatal(err)
			}
			var bundle replayBundle
			if err = json.Unmarshal(bundleBytes, &bundle); err != nil || !bytes.Equal(bundle.Input, payload) || len(bundle.Objects) == 0 {
				t.Fatal("canonical replay bundle lost owned data", err)
			}
			pluginPath := buildReplayPlugin(t)
			if _, err = call("-handler-plugin", pluginPath, "-handler-symbol", "GraphSignalWorkflow", "replay", typ, "success"); err != nil {
				t.Fatal(err)
			}
			bundleFile := filepath.Join(operatorTempDir(t), "replay.json")
			if err = os.WriteFile(bundleFile, bundleBytes, 0600); err != nil {
				t.Fatal(err)
			}
			var offline bytes.Buffer
			if err = run([]string{"-url", "nats://127.0.0.1:1", "-handler-plugin", pluginPath, "-handler-symbol", "GraphSignalWorkflow", "-replay-bundle", bundleFile, "replay"}, &offline); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("offline replay executed an effect", err)
			}
			var replayed replayReport
			if err = json.Unmarshal(offline.Bytes(), &replayed); err != nil || replayed.Status != "completed" || !bytes.Equal(replayed.Result, payload) {
				t.Fatal("offline canonical replay differs", err)
			}
			for {
				row, e := projection.Get(ctx, typ, "success")
				if e == nil && row.Status == "completed" && row.InvSeq == handle.InvSeq {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("graph projection failed to catch up", e, ctx.Err())
				}
				select {
				case e := <-projectDone:
					projectDone <- e
					t.Fatal("graph projection stopped", e)
				default:
				}
				time.Sleep(20 * time.Millisecond)
			}
			legacyView, err := js.KeyValue(ctx, "WF_VIEW")
			if err != nil {
				t.Fatal(err)
			}
			foreign, _ := json.Marshal(visibility.Row{SchemaVersion: 1, Type: "foreign", ID: "legacy", Status: "completed", InvSeq: 999})
			if _, err = legacyView.Put(ctx, "row.foreign.legacy", foreign); err != nil {
				t.Fatal(err)
			}
			listArgs := append(append([]string(nil), viewArgs...), "-limit", "1")
			if !postgres {
				listArgs = append(listArgs, "-rebuild")
			}
			listed, err := call(append(listArgs, "list", "completed")...)
			if err != nil {
				t.Fatal(err)
			}
			var page visibility.Page
			if err = json.Unmarshal(listed, &page); err != nil || len(page.Rows) != 1 || page.Rows[0].Type != typ || page.Rows[0].ID != "success" || page.Rows[0].Status != "completed" {
				t.Fatal("graph view mixed sources", page, err)
			}
			if _, err = call(append(append([]string(nil), viewArgs...), "lag")...); err != nil {
				t.Fatal(err)
			}
			if _, err = call("describe", typ, "success"); err != nil {
				t.Fatal(err)
			}
			for _, command := range []string{"scan-start", "scan-signal", "scan-terminal", "scan-timer", "scan-suspended"} {
				if _, err = call(command); err != nil {
					t.Fatal(command, err)
				}
			}
			if err = state.Delete(ctx, identity.Key(typ, "success")); err != nil {
				t.Fatal(err)
			}
			if _, err = call("scan-terminal"); err != nil {
				t.Fatal(err)
			}
			if _, err = state.Get(ctx, identity.Key(typ, "success")); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatal("dry-run changed absent terminal projection", err)
			}
			if _, err = call("-apply", "scan-terminal"); err != nil {
				t.Fatal(err)
			}
			for {
				entry, getErr := state.Get(ctx, identity.Key(typ, "success"))
				if getErr == nil {
					var expected, actual bytes.Buffer
					if err = json.Compact(&expected, records[len(records)-1].Payload); err != nil {
						t.Fatal(err)
					}
					if err = json.Compact(&actual, entry.Value()); err != nil || !bytes.Equal(actual.Bytes(), expected.Bytes()) {
						t.Fatal("manual terminal repair differs from canonical outcome", err)
					}
					break
				}
				if ctx.Err() != nil || !errors.Is(getErr, jetstream.ErrKeyNotFound) {
					t.Fatal("manual terminal repair failed", getErr, ctx.Err())
				}
				time.Sleep(20 * time.Millisecond)
			}
			if after, err := call("export-journal", typ, "success"); err != nil || !bytes.Equal(after, journalBytes) {
				t.Fatal("manual repair changed canonical history", err)
			}
			if _, err = call("start", typ, "cancelled", "null"); err != nil {
				t.Fatal(err)
			}
			if _, err = call("cancel", typ, "cancelled"); err != nil {
				t.Fatal(err)
			}
			if _, err = call("result", typ, "cancelled"); !errors.Is(err, client.ErrCancelled) {
				t.Fatal("canonical cancellation missing", err)
			}
			stop()
			err = <-done
			joined = true
			if err != nil {
				t.Fatal(err)
			}
			if effects.Load() != 1 {
				t.Fatalf("effect count=%d", effects.Load())
			}
			if _, err = call("-grace", "1h", "purge", typ, "success"); err != nil {
				t.Fatal(err)
			}
			if _, err = call("result", typ, "success"); !errors.Is(err, client.ErrPurged) {
				t.Fatal("purged result was exposed", err)
			}
			legacy, err := js.Stream(ctx, "WF_JRN")
			if err != nil {
				t.Fatal(err)
			}
			info, err := legacy.Info(ctx)
			if err != nil || info.State.Msgs != 0 {
				t.Fatal("graph CLI used legacy history", err)
			}
			if domain != "" && (observed.Load() == 0 || wrong.Load() != 0) {
				t.Fatal("domain routing", observed.Load(), wrong.Load())
			}
			t.Log(fmt.Sprintf("canonical CLI large Start/Signal/result, forged mirror ignored, canonical online/offline replay after source/blob deletion, canonical CLI project/list/lag and isolated page while worker active, history/scans, dry-run/apply terminal restoration, cancellation, purge and effect one; domain=%s requests=%d wrong=%d", domain, observed.Load(), wrong.Load()))
		})
	}
}
