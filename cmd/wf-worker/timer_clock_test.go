package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/journal"
	"js-wf/runtimeclock"
	"js-wf/testcluster"
)

func TestWorkerRunnerRejectsClockWithoutRepairsOrConfig(t *testing.T) {
	for _, args := range [][]string{{"-timer-clock-config", "unused", "-reconcile=false"}, {"-provision-timer-clock"}} {
		err := run(context.Background(), append([]string{"-id", "clock", "-handler-plugin", "unused"}, args...))
		if err == nil || !strings.Contains(err.Error(), "requires") {
			t.Fatalf("unsafe clock startup=%v", err)
		}
	}
}

func TestWorkerRunnerUsesIndependentClockForNativeAndFallbackTimers(t *testing.T) {
	plugin := testWorkerPlugin(t)
	for _, backend := range []string{"native", "fallback"} {
		t.Run(backend, func(t *testing.T) {
			tags := map[int][]string{0: {"clock-a"}, 1: {"clock-b"}, 2: {"clock-c"}}
			cluster, err := testcluster.StartWithServerTags(t.TempDir(), 3, tags)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close)
			cfg := runtimeclock.DefaultConfig()
			for i, s := range cluster.Servers {
				cfg.Probes = append(cfg.Probes, runtimeclock.Probe{Name: fmt.Sprintf("WF_CLOCK_%d", i), Server: s.Name(), Identity: s.ID(), Tag: tags[i][0]})
			}
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "clock.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			done := make(chan error, 1)
			runnerStopped := false
			go func() {
				done <- run(ctx, []string{"-url", cluster.Servers[2].ClientURL(), "-id", "clock-worker", "-replicas", "3", "-handler-plugin", plugin, "-metrics-addr", "127.0.0.1:0", "-timer-backend", backend, "-timer-clock-config", path, "-provision-timer-clock", "-reconcile-interval", "20ms"})
			}()
			t.Cleanup(func() {
				cancel()
				if runnerStopped {
					return
				}
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("clock runner shutdown: %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Error("clock runner did not stop")
				}
			})
			js, err := jetstream.New(cluster.Clients[2])
			if err != nil {
				t.Fatal(err)
			}
			// Provisioning precedes worker start; wait for the full clock topology
			// before submitting so missing setup is not confused with a timer bug.
			for {
				attempt, stopAttempt := context.WithTimeout(ctx, 500*time.Millisecond)
				_, err := js.Stream(attempt, cfg.Probes[len(cfg.Probes)-1].Name)
				stopAttempt()
				if err == nil {
					break
				}
				select {
				case err := <-done:
					runnerStopped = true
					t.Fatalf("clock runner exited during provisioning: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
			c := client.New(js)
			for {
				_, err = c.Start(ctx, "worker-timer", "common", []byte(`null`))
				if err == nil {
					break
				}
				if !errors.Is(err, jetstream.ErrStreamNotFound) && !retryableStartupError(err) {
					t.Fatal(err)
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(20 * time.Millisecond):
				}
			}
			value, err := c.Await(ctx, "worker-timer", "common")
			if err != nil || string(value) != "42" {
				t.Fatalf("common clock result=%s err=%v", value, err)
			}
			records, _, err := journal.New(js).Read(ctx, "worker-timer", "common")
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, r := range records {
				if r.Kind != journal.StepRequested {
					continue
				}
				var req struct {
					Kind   string `json:"kind"`
					Domain string `json:"clock_domain"`
				}
				if json.Unmarshal(r.Payload, &req) != nil {
					t.Fatal("invalid timer request")
				}
				if req.Kind == "timer" {
					if req.Domain != runtimeclock.DeadlineDomain {
						t.Fatalf("actual CLI request domain=%q", req.Domain)
					}
					found = true
				}
			}
			if !found {
				t.Fatal("no actual tagged timer request")
			}
			if backend == "fallback" {
				timers, err := js.Stream(ctx, "WF_TIMER")
				if err != nil {
					t.Fatal(err)
				}
				for {
					info, err := timers.Info(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if info.State.Msgs == 0 {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("fallback record was not acknowledged/deleted")
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			t.Logf("actual %s runner completes tagged timer with common clock and configured elected repairs", backend)
		})
	}
}
