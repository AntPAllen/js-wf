package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

var workerTestPluginOnce sync.Once
var workerTestPluginPath string
var workerTestPluginDir string
var workerTestPluginErr error

func TestMain(m *testing.M) {
	code := m.Run()
	_ = os.RemoveAll(workerTestPluginDir)
	os.Exit(code)
}

func TestWorkerRunnerCompletesWorkflowAndServesMetrics(t *testing.T) {
	pluginPath := testWorkerPlugin(t)
	for _, mode := range []string{"static", "kv"} {
		t.Run(mode, func(t *testing.T) { runWorkerSmoke(t, pluginPath, mode) })
	}
}

func testWorkerPlugin(t *testing.T) string {
	t.Helper()
	workerTestPluginOnce.Do(func() {
		workerTestPluginDir, workerTestPluginErr = os.MkdirTemp("", "wf-worker-plugin-")
		if workerTestPluginErr != nil {
			return
		}
		workerTestPluginPath = filepath.Join(workerTestPluginDir, "handler.so")
		buildArgs := []string{"build"}
		if workerPluginRace {
			buildArgs = append(buildArgs, "-race")
		}
		buildArgs = append(buildArgs, "-buildmode=plugin", "-o", workerTestPluginPath, "./testdata/handlerplugin")
		build := exec.Command("go", buildArgs...)
		if output, err := build.CombinedOutput(); err != nil {
			workerTestPluginErr = fmt.Errorf("build handler plugin: %w: %s", err, output)
		}
	})
	if workerTestPluginErr != nil {
		t.Fatal(workerTestPluginErr)
	}
	return workerTestPluginPath
}

func TestWorkerRunnerStartsAfterServerRestart(t *testing.T) {
	pluginPath := testWorkerPlugin(t)
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	serverURL := cluster.Servers[0].ClientURL()
	cluster.KillNode(0)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-url", serverURL, "-id", "restart-smoke", "-replicas", "1", "-handler-plugin", pluginPath, "-metrics-addr", metricsAddr, "-reconcile=false"})
	}()
	select {
	case runErr := <-done:
		t.Fatalf("runner exited before NATS restart: %v", runErr)
	case <-time.After(300 * time.Millisecond):
	}
	if err := cluster.RestartNode(0); err != nil {
		t.Fatal(err)
	}
	metricsURL := "http://" + metricsAddr + "/metrics"
	clientHTTP := &http.Client{Timeout: time.Second}
	for ctx.Err() == nil {
		response, reqErr := clientHTTP.Get(metricsURL)
		if reqErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case runErr := <-done:
			t.Fatalf("runner exited during NATS recovery: %v", runErr)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("runner did not start after NATS restart: %v", ctx.Err())
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(js)
	if _, err := c.Start(ctx, "worker-smoke", "after-restart", []byte(`42`)); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, "worker-smoke", "after-restart"); err != nil || string(value) != "42" {
		t.Fatalf("workflow after restart: result=%s err=%v", value, err)
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("runner shutdown: %v", runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestWorkerRunnerRunsFallbackTimerLoop(t *testing.T) {
	pluginPath := testWorkerPlugin(t)
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
	if err := provision.EnsureFallback(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr := listener.Addr().String()
	_ = listener.Close()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-url", cluster.Servers[0].ClientURL(), "-id", "fallback-smoke", "-replicas", "1", "-handler-plugin", pluginPath, "-metrics-addr", metricsAddr, "-reconcile-interval", "100ms"})
	}()
	metricsURL := "http://" + metricsAddr + "/metrics"
	clientHTTP := &http.Client{Timeout: time.Second}
	for ctx.Err() == nil {
		response, reqErr := clientHTTP.Get(metricsURL)
		if reqErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case runErr := <-done:
			t.Fatalf("fallback runner exited during startup: %v", runErr)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("fallback runner did not start: %v", ctx.Err())
	}
	c := client.New(js)
	if _, err := c.Start(ctx, "worker-timer", "fallback", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, "worker-timer", "fallback"); err != nil || string(value) != "42" {
		t.Fatalf("fallback timer result=%s err=%v", value, err)
	}
	timers, err := js.Stream(ctx, "WF_TIMER")
	if err != nil {
		t.Fatal(err)
	}
	info, err := timers.Info(ctx)
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("fallback timer stream: info=%+v err=%v", info, err)
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("fallback runner shutdown: %v", runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fallback runner did not stop")
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
			"-retention-type", "retention", "-retention-grace", "1h",
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
	first, err := c.Start(ctx, "worker-smoke", "job", []byte(`42`))
	if err != nil {
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
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	tombstone, err := json.Marshal(retention.Tombstone{
		Tombstone: true,
		InvSeq:    1,
		PurgedAt:  time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	key := identity.Key("worker-smoke", "expired")
	if _, err := state.Put(ctx, key, tombstone); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		_, err := state.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		if err != nil {
			t.Fatalf("check tombstone: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("tombstone loop did not reclaim expired state: %v", ctx.Err())
	}
	request, err := json.Marshal(retention.Request{Type: "worker-smoke", ID: "job"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Start(ctx, "retention", "purge-job", request); err != nil {
		t.Fatal(err)
	}
	if value, err := c.Await(ctx, "retention", "purge-job"); err != nil || string(value) != "true" {
		t.Fatalf("retention workflow result=%s err=%v", value, err)
	}
	if _, err := c.Await(ctx, "worker-smoke", "job"); !errors.Is(err, client.ErrPurged) {
		t.Fatalf("retired target: %v", err)
	}
	second, err := c.Start(ctx, "worker-smoke", "job", []byte(`43`))
	if err != nil || second.InvSeq <= first.InvSeq {
		t.Fatalf("reused invocation=%+v original=%+v err=%v", second, first, err)
	}
	if value, err := c.Await(ctx, "worker-smoke", "job"); err != nil || string(value) != "43" {
		t.Fatalf("reused workflow result=%s err=%v", value, err)
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
