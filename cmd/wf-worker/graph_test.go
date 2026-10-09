package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
)

func TestWorkerGraphAdmissionRejectsIncompatibleCLI(t *testing.T) {
	base := []string{"-id", "graph", "-handler-plugin", "unused"}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"partial", []string{"-graph-authority-stream", "AUTH"}, "requires graph-authority"},
		{"namespace", []string{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "bad.*", "-graph-object-bucket", "OBJECTS", "-timer-backend", "native"}, "invalid native graph namespace"},
		{"fallback", []string{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-timer-backend", "fallback"}, "fallback timer migration"},
		{"retention", []string{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-timer-backend", "native", "-retention-type", "purge"}, "graph-aware retention"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := run(context.Background(), append(append([]string(nil), base...), tc.args...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want %s", err, tc.want)
			}
		})
	}
}

func TestWorkerRunnerCanonicalGraphRepair(t *testing.T) {
	plugin := testWorkerPlugin(t)
	for _, domain := range []string{"", "WFGRAPH"} {
		name := "R1"
		if domain != "" {
			name = "R3Domain"
		}
		t.Run(name, func(t *testing.T) {
			cluster, err := workerDomainCluster(t, domain)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if domain != "" {
				admitWorkerDomain(t, ctx, cluster, domain)
			}
			js, err := workerDomainJS(cluster, domain)
			if err != nil {
				t.Fatal(err)
			}
			replicas := len(cluster.Servers)
			if err = provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "CLI_GRAPH_AUTH", AuthorityPrefix: "wf.graph.cli", ObjectBucket: "CLI_GRAPH_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, Encoding: journal.ProtobufV1}
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
			c, err := client.NewWithGraphJournal(js, graph)
			if err != nil {
				t.Fatal(err)
			}
			// The runner must discover this owned input before any invocation exists.
			if _, err = graph.ReserveStart(ctx, journal.GraphStartRequest{Type: "worker-smoke", ID: "reserved"}, []byte(`42`)); err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "worker-graph-signal", "reserved", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = graph.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "go", Key: "reserved"}, []byte(`43`), false); err != nil {
				t.Fatal(err)
			}
			if _, err = c.Start(ctx, "worker-timer", "reserved", []byte(`null`)); err != nil {
				t.Fatal(err)
			}
			runs, err := js.Stream(ctx, "WF_RUN")
			if err != nil {
				t.Fatal(err)
			}
			if err = runs.Purge(ctx); err != nil {
				t.Fatal(err)
			}
			events := filepath.Join(t.TempDir(), "events.jsonl")
			args := append(workerDomainArgs(domain, replicas), []string{"-url", cluster.Servers[0].ClientURL(), "-id", "graph-cli", "-handler-plugin", plugin, "-metrics-addr", "127.0.0.1:0", "-graph-authority-stream", cfg.AuthorityStream, "-graph-authority-prefix", cfg.AuthorityPrefix, "-graph-object-bucket", cfg.ObjectBucket, "-journal-encoding", "protobuf-v1", "-timer-backend", "native", "-reconcile-interval", "100ms", "-events-file", events}...)
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			joined := false
			go func() { done <- runDomainWorker(t, runCtx, domain, args) }()
			defer func() {
				if joined {
					return
				}
				stop()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			for _, r := range []struct{ typ, result string }{{"worker-smoke", "42"}, {h.Type, "43"}, {"worker-timer", "42"}} {
				type resultReply struct {
					value []byte
					err   error
				}
				reply := make(chan resultReply, 1)
				go func() { value, err := c.Await(runCtx, r.typ, "reserved"); reply <- resultReply{value, err} }()
				var result []byte
				select {
				case err := <-done:
					joined = true
					t.Fatalf("graph runner exited before result: %v", err)
				case value := <-reply:
					result, err = value.value, value.err
				}
				if err != nil || string(result) != r.result {
					t.Fatalf("%s result=%s err=%v", r.typ, result, err)
				}
			}
			records, _, err := graph.ReadExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(records)
			state, err := js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			key := identity.Key(h.Type, h.ID)
			var original []byte
			for ctx.Err() == nil {
				entry, e := state.Get(ctx, key)
				if e == nil {
					original = entry.Value()
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if len(original) == 0 {
				t.Fatal("terminal projection missing", ctx.Err())
			}
			// Delete twice inside the normal dispatch dedup window. Only the graph
			// terminal loop can rediscover and enqueue either missing projection.
			for deletion := 0; deletion < 2; deletion++ {
				if err = state.Delete(ctx, key); err != nil {
					t.Fatal(err)
				}
				restored := false
				for ctx.Err() == nil {
					entry, e := state.Get(ctx, key)
					if e == nil {
						if !bytes.Equal(entry.Value(), original) {
							t.Fatal("projection bytes changed")
						}
						restored = true
						break
					}
					time.Sleep(20 * time.Millisecond)
				}
				if !restored {
					t.Fatal("terminal projection not restored", ctx.Err())
				}
			}
			afterRecords, _, err := graph.ReadExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(afterRecords)
			if !bytes.Equal(before, after) {
				t.Fatal("terminal repair changed canonical journal")
			}
			legacy, err := js.Stream(ctx, "WF_JRN")
			if err != nil {
				t.Fatal(err)
			}
			info, err := legacy.Info(ctx)
			if err != nil || info.State.Msgs != 0 {
				t.Fatal("graph CLI wrote legacy journal", err)
			}
			stop()
			shutdownErr := <-done
			joined = true
			if shutdownErr != nil {
				t.Fatal(shutdownErr)
			}
			// Graceful shutdown drains and syncs the append-only event file.
			// Require acknowledged canonical repairs.
			data, err := os.ReadFile(events)
			if err != nil {
				t.Fatal(err)
			}
			observed := map[string]bool{}
			for _, line := range bytes.Split(data, []byte("\n")) {
				var row struct {
					Kind  string                         `json:"kind"`
					Event struct{ Kind, Outcome string } `json:"event"`
				}
				if json.Unmarshal(line, &row) == nil && row.Kind == "repair" && row.Event.Outcome == "acknowledged" {
					observed[row.Event.Kind] = true
				}
			}
			for _, kind := range []string{"graph-start", "graph-signal", "graph-terminal"} {
				if !observed[kind] {
					t.Fatalf("missing %s repair evidence: %v", kind, observed)
				}
			}
			t.Log("canonical CLI reserved Start/Signal recovery, native timer completion, two terminal restorations, unchanged journal, no legacy journal writes; replicas=" + strconv.Itoa(replicas) + " domain=" + domain)
		})
	}
}
