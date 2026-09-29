package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

// TestWorkerRunnerSustainedMetricsScrape is opt-in because it keeps one live
// process and a real JetStream server busy for at least fifteen minutes in CI.
func TestWorkerRunnerSustainedMetricsScrape(t *testing.T) {
	if os.Getenv("WF_METRICS_SOAK") != "1" {
		t.Skip("set WF_METRICS_SOAK=1 to run the live metrics scrape soak")
	}
	duration := 15 * time.Minute
	if raw := os.Getenv("WF_METRICS_SOAK_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration <= 0 {
			t.Fatalf("invalid WF_METRICS_SOAK_DURATION=%q", raw)
		}
	}
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), duration+time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-url", cluster.Servers[0].ClientURL(), "-id", "metrics-soak", "-replicas", "1", "-handler-plugin", pluginPath, "-metrics-addr", metricsAddr})
	}()
	metricsURL := "http://" + metricsAddr + "/metrics"
	httpClient := &http.Client{Timeout: 2 * time.Second}
	for ctx.Err() == nil {
		response, requestErr := httpClient.Get(metricsURL)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case runErr := <-done:
			t.Fatalf("metrics runner exited at startup: %v", runErr)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("metrics runner did not start: %v", ctx.Err())
	}
	startedAt := time.Now()
	deadline := startedAt.Add(duration)
	c := client.New(js)
	var completed int
	var scrapes int
	var priorAcquisitions float64
	for time.Now().Before(deadline) && ctx.Err() == nil {
		id := fmt.Sprintf("soak-%08d", completed)
		attempt, stop := context.WithTimeout(ctx, 10*time.Second)
		_, startErr := c.Start(attempt, "worker-smoke", id, []byte(`42`))
		var value []byte
		var awaitErr error
		if startErr == nil {
			value, awaitErr = c.Await(attempt, "worker-smoke", id)
		}
		stop()
		if startErr != nil || awaitErr != nil || string(value) != "42" {
			t.Fatalf("workflow %s: start=%v await=%v result=%s", id, startErr, awaitErr, value)
		}
		completed++
		response, err := httpClient.Get(metricsURL)
		if err != nil {
			t.Fatalf("metrics scrape %d: %v", scrapes, err)
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("metrics scrape %d: status=%d read=%v body=%s", scrapes, response.StatusCode, readErr, body)
		}
		acquisitions, err := parsePrometheusSample(body, "js_wf_worker_lease_acquisitions_total")
		if err != nil || acquisitions < priorAcquisitions || acquisitions < float64(completed) {
			t.Fatalf("metrics scrape %d: acquisitions=%g previous=%g completed=%d err=%v", scrapes, acquisitions, priorAcquisitions, completed, err)
		}
		if _, err := parsePrometheusSample(body, "js_wf_journal_bytes"); err != nil {
			t.Fatalf("metrics scrape %d: %v", scrapes, err)
		}
		priorAcquisitions = acquisitions
		scrapes++
		select {
		case <-ctx.Done():
		case <-time.After(250 * time.Millisecond):
		}
	}
	if ctx.Err() != nil || completed < int(duration.Seconds()/5) {
		t.Fatalf("metrics soak stopped early: completed=%d scrapes=%d elapsed=%s err=%v", completed, scrapes, time.Since(startedAt), ctx.Err())
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("metrics runner shutdown: %v", runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("metrics runner did not stop")
	}
	t.Logf("metrics scrape soak: duration=%s completed=%d scrapes=%d final_lease_acquisitions=%.0f", time.Since(startedAt), completed, scrapes, priorAcquisitions)
}

func parsePrometheusSample(body []byte, name string) (float64, error) {
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[0] == name {
			return strconv.ParseFloat(parts[1], 64)
		}
	}
	return 0, fmt.Errorf("metric %s missing", name)
}
