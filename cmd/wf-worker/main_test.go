package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

func TestWorkerRunnerCompletesWorkflowAndServesMetrics(t *testing.T) {
	pluginPath := filepath.Join(t.TempDir(), "handler.so")
	buildArgs := []string{"build"}
	if workerPluginRace {
		buildArgs = append(buildArgs, "-race")
	}
	buildArgs = append(buildArgs, "-buildmode=plugin", "-o", pluginPath, "./testdata/handlerplugin")
	build := exec.Command("go", buildArgs...)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build handler plugin: %v: %s", err, output)
	}
	for _, mode := range []string{"static", "kv"} {
		t.Run(mode, func(t *testing.T) { runWorkerSmoke(t, pluginPath, mode) })
	}
}

func runWorkerSmoke(t *testing.T, pluginPath, mode string) {
	t.Helper()
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
	if mode == "kv" {
		if err := provision.Ensure(ctx, js, 1); err != nil {
			t.Fatal(err)
		}
		assignments, err := assignment.New(ctx, js)
		if err != nil {
			t.Fatal(err)
		}
		if err := assignments.InitializeStatic(ctx, []string{"runner-smoke"}); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-url", cluster.Servers[0].ClientURL(), "-id", "runner-smoke",
			"-replicas", "1", "-handler-plugin", pluginPath, "-mode", mode,
			"-metrics-addr", metricsAddr, "-reconcile-interval", "100ms",
		})
	}()
	httpClient := &http.Client{Timeout: time.Second}
	metricsURL := "http://" + metricsAddr + "/metrics"
	for ctx.Err() == nil {
		response, err := httpClient.Get(metricsURL)
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case runErr := <-done:
			t.Fatalf("worker runner exited before metrics became ready: %v", runErr)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("metrics server did not become ready: %v", ctx.Err())
	}
	c := client.New(js)
	if _, err := c.Start(ctx, "worker-smoke", "job", []byte(`42`)); err != nil {
		t.Fatal(err)
	}
	result, err := c.Await(ctx, "worker-smoke", "job")
	if err != nil || string(result) != "42" {
		t.Fatalf("runner result=%s err=%v", result, err)
	}
	response, err := httpClient.Get(metricsURL)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(data), "js_wf_worker_lease_acquisitions_total 1") || !strings.Contains(string(data), "js_wf_worker_enqueue_to_lease_seconds_count 1") {
		t.Fatalf("metrics status=%d body=%s err=%v", response.StatusCode, data, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker runner did not shut down")
	}
}
