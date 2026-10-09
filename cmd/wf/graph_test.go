package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

func TestGraphOperatorRejectsPartialOrLegacyOnlySelection(t *testing.T) {
	for _, args := range [][]string{
		{"-graph-authority-stream", "AUTH", "result", "test", "id"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "list"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "export-replay", "test", "id"},
	} {
		err := run(args, &bytes.Buffer{})
		if err == nil || !(strings.Contains(err.Error(), "requires graph-authority") || strings.Contains(err.Error(), "migration")) {
			t.Fatalf("args=%v err=%v", args, err)
		}
	}
}

func TestNativeCanonicalGraphOperatorCommands(t *testing.T) {
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
			var observed, wrong atomic.Int64
			opts := []jetstream.JetStreamOpt{}
			if domain != "" {
				opts = append(opts, jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
					if strings.HasPrefix(subject, "$JS."+domain+".API.") {
						observed.Add(1)
					} else {
						wrong.Add(1)
					}
				}}))
			}
			base := []string{"-url", cluster.Servers[0].ClientURL(), "-timeout", "45s", "-replicas", strconv.Itoa(count), "-graph-authority-stream", cfg.AuthorityStream, "-graph-authority-prefix", cfg.AuthorityPrefix, "-graph-object-bucket", cfg.ObjectBucket}
			if domain != "" {
				base = append(base, "-domain", domain)
			}
			call := func(args ...string) ([]byte, error) {
				var out bytes.Buffer
				err := runWithJetStreamOptions(append(append([]string(nil), base...), args...), &out, opts...)
				return out.Bytes(), err
			}
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
			if _, err = call("describe", typ, "success"); err != nil {
				t.Fatal(err)
			}
			for _, command := range []string{"scan-start", "scan-signal", "scan-terminal", "scan-timer", "scan-suspended"} {
				if _, err = call(command); err != nil {
					t.Fatal(command, err)
				}
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
			t.Log(fmt.Sprintf("canonical CLI large Start/Signal/result, forged mirror ignored, history/scans, cancellation, purge and effect one; domain=%s requests=%d wrong=%d", domain, observed.Load(), wrong.Load()))
		})
	}
}
