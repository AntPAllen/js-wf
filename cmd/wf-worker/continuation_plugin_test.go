package main

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/testcluster"
	"testing"
	"time"
)

func TestWorkerContinuationPlugin(t *testing.T) {
	path := testWorkerPlugin(t)
	if _, _, err := loadHandlers(path, "InvalidWorkflows"); err == nil {
		t.Fatal("invalid stage admitted")
	}
	for _, symbol := range []string{"Workflows", "WorkflowFactory"} {
		t.Run(symbol, func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				defer close(done)
				done <- run(ctx, []string{"-url", cluster.Servers[0].ClientURL(), "-id", "continued-runner", "-replicas", "1", "-handler-plugin", path, "-handler-symbol", symbol, "-metrics-addr", "127.0.0.1:0", "-reconcile-interval", "100ms"})
			}()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}()
			for ctx.Err() == nil {
				if _, err := js.Stream(ctx, "WF_INV"); err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("runner exited: %v", err)
				default:
				}
				time.Sleep(20 * time.Millisecond)
			}
			c := client.New(js)
			if _, err := c.Start(ctx, "continued", "runner", []byte(`7`)); err != nil {
				t.Fatal(err)
			}
			reached := false
			for ctx.Err() == nil {
				records, _, err := journal.New(js).Read(ctx, "continued", "runner")
				if err == nil && len(records) > 0 && records[len(records)-1].Kind == journal.Failed {
					t.Fatalf("runner failed before continuation gate: %s", records[len(records)-1].Payload)
				}
				if err == nil && len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
					var wait struct {
						WaitingOn string `json:"waiting_on"`
					}
					_ = json.Unmarshal(records[len(records)-1].Payload, &wait)
					if wait.WaitingOn == "signal:gate" {
						reached = true
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !reached {
				t.Fatal("continuation never reached signal gate")
			}
			if _, err := c.Signal(ctx, "continued", "runner", "gate", []byte(`true`), "gate"); err != nil {
				t.Fatal(err)
			}
			result, err := c.Await(ctx, "continued", "runner")
			if err != nil || string(result) != "60" {
				t.Fatalf("result=%s err=%v", result, err)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			auditCtx, stopAudit := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopAudit()
			if _, err := integrity.Check(auditCtx, js); err != nil {
				t.Fatal(err)
			}
			t.Logf("%s runner completed two checkpoints, restored state/input/locals, result=60", symbol)
		})
	}
}
