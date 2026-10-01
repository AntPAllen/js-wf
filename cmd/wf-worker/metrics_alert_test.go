package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

const monitoringPrometheusImage = "prom/prometheus:v3.15.0"
const monitoringAlertmanagerImage = "prom/alertmanager:v0.34.1"

type capacityWebhook struct {
	Version  string `json:"version"`
	Receiver string `json:"receiver"`
	Status   string `json:"status"`
	Alerts   []struct {
		Status      string            `json:"status"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
		StartsAt    time.Time         `json:"startsAt"`
		EndsAt      time.Time         `json:"endsAt"`
	} `json:"alerts"`
}

// Real rule evaluation and notification delivery; no alert is synthesized or
// posted to Alertmanager by the test. Keep the shipping rule's two-minute hold.
func TestJournalCapacityAlertDeliveredAndResolved(t *testing.T) {
	if os.Getenv("WF_MONITORING_ALERT") != "1" {
		t.Skip("set WF_MONITORING_ALERT=1 for real Prometheus/Alertmanager delivery")
	}
	if runtime.GOOS != "linux" {
		t.Skip("fixture uses Docker host networking on Linux")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	root := os.Getenv("WF_MONITORING_ARTIFACT_DIR")
	if root == "" {
		root = t.TempDir()
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	var receiptMu sync.Mutex
	var receipts []json.RawMessage
	delivered := make(chan capacityWebhook, 16)
	receiver := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			out.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		data, err := io.ReadAll(io.LimitReader(req.Body, 1024*1024))
		var payload capacityWebhook
		if err != nil || json.Unmarshal(data, &payload) != nil {
			out.WriteHeader(http.StatusBadRequest)
			return
		}
		receiptMu.Lock()
		receipts = append(receipts, append(json.RawMessage(nil), data...))
		receiptMu.Unlock()
		select {
		case delivered <- payload:
		default:
		}
		out.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	t.Cleanup(func() {
		receiptMu.Lock()
		defer receiptMu.Unlock()
		data, err := json.MarshalIndent(receipts, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		if err := os.WriteFile(filepath.Join(root, "webhooks.json"), data, 0644); err != nil {
			t.Error(err)
		}
	})
	cluster, err := testcluster.Start(filepath.Join(root, "nats"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	for node, srv := range cluster.Servers {
		srv.SetLogger(logger.NewFileLogger(filepath.Join(root, fmt.Sprintf("nats-%d.log", node)), true, false, false, false), false, false)
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	setupCtx, finishSetup := context.WithTimeout(ctx, 30*time.Second)
	for {
		attempt, stopAttempt := context.WithTimeout(setupCtx, 3*time.Second)
		started := time.Now()
		_, err = provision.EnsureAutoWithJournalLimit(attempt, js, 3, 16384)
		stopAttempt()
		t.Logf("provision elapsed=%s err=%v", time.Since(started), err)
		if err == nil {
			break
		}
		if setupCtx.Err() != nil || !retryableStartupError(err) {
			finishSetup()
			t.Fatalf("bounded provisioning: %v", err)
		}
		select {
		case <-setupCtx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	finishSetup()
	// Mount the production capacity/worker metrics handler. Capacity is queried
	// from the real replicated journal; no synthetic metric source is substituted.
	metrics := httptest.NewServer(metricsHandler(js, nil))
	defer metrics.Close()
	freeAddress := func() string {
		t.Helper()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		return address
	}
	promAddress, amAddress := freeAddress(), freeAddress()
	rule, err := os.ReadFile("../../docs/monitoring/prometheus-rules.yml")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("prometheus-rules.yml", string(rule))
	write("prometheus.yml", fmt.Sprintf(`global:
  scrape_interval: 250ms
  evaluation_interval: 250ms
rule_files: [/fixture/prometheus-rules.yml]
alerting:
  alertmanagers:
    - static_configs:
        - targets: [%q]
scrape_configs:
  - job_name: wf-worker
    static_configs:
      - targets: [%q]
        labels: {cluster: monitoring-fixture}
`, amAddress, strings.TrimPrefix(metrics.URL, "http://")))
	write("alertmanager.yml", fmt.Sprintf(`route:
  receiver: local-fixture
  group_by: [alertname, cluster]
  group_wait: 0s
  group_interval: 1s
  repeat_interval: 1h
receivers:
  - name: local-fixture
    webhook_configs:
      - url: %q
        send_resolved: true
`, receiver.URL))
	docker := func(args ...string) string {
		t.Helper()
		command := exec.CommandContext(ctx, "docker", args...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	start := func(label, image string, args ...string) {
		t.Helper()
		name := fmt.Sprintf("wf-monitor-%s-%d-%d", label, os.Getpid(), time.Now().UnixNano())
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			output, _ := exec.CommandContext(cleanup, "docker", "logs", name).CombinedOutput()
			if err := os.WriteFile(filepath.Join(root, label+".log"), output, 0644); err != nil {
				t.Error(err)
			}
			if t.Failed() {
				t.Logf("%s logs:\n%s", label, output)
			}
			output, err := exec.CommandContext(cleanup, "docker", "rm", "-f", "-v", name).CombinedOutput()
			if err != nil {
				t.Errorf("cleanup %s: %v %s", name, err, output)
			}
		})
		base := []string{"run", "-d", "--name", name, "--network", "host", "--memory", "256m", "--mount", "type=bind,src=" + root + ",dst=/fixture,readonly", image}
		id := docker(append(base, args...)...)
		t.Logf("%s image=%s container=%s", label, image, id)
		info := docker("inspect", "--format", "{{.Image}}", name)
		write(label+"-image.txt", image+"\n"+info+"\n")
	}
	start("alertmanager", monitoringAlertmanagerImage, "--config.file=/fixture/alertmanager.yml", "--web.listen-address="+amAddress, "--cluster.listen-address=")
	start("prometheus", monitoringPrometheusImage, "--config.file=/fixture/prometheus.yml", "--web.listen-address="+promAddress, "--storage.tsdb.retention.time=1h")
	httpClient := &http.Client{Timeout: time.Second}
	get := func(path string) ([]byte, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+promAddress+path, nil)
		if err != nil {
			return nil, err
		}
		response, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("status=%d: %s", response.StatusCode, data)
		}
		return data, err
	}
	wait := func(label string, until time.Time, check func() bool) {
		t.Helper()
		for ctx.Err() == nil && time.Now().Before(until) {
			if check() {
				return
			}
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatalf("waiting for %s: %v", label, ctx.Err())
	}
	wait("below-threshold scrape", time.Now().Add(15*time.Second), func() bool {
		raw, err := get("/api/v1/query?query=js_wf_journal_capacity_ratio")
		if err != nil {
			return false
		}
		var result struct {
			Data struct {
				Result []struct{ Value []json.RawMessage }
			}
		}
		if json.Unmarshal(raw, &result) != nil || len(result.Data.Result) != 1 || len(result.Data.Result[0].Value) != 2 {
			return false
		}
		var value string
		if json.Unmarshal(result.Data.Result[0].Value[1], &value) != nil {
			return false
		}
		ratio, err := strconv.ParseFloat(value, 64)
		return err == nil && ratio < 0.7
	})
	// Confirm actual scrape/evaluation below threshold causes no notification.
	quietUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(quietUntil) {
		select {
		case payload := <-delivered:
			t.Fatalf("alert below threshold: %+v", payload)
		case <-time.After(100 * time.Millisecond):
		}
	}
	filledAt := time.Now()
	if _, err := js.Publish(ctx, "wf.jrn.capacity.alert", bytes.Repeat([]byte{'x'}, 12288)); err != nil {
		t.Fatal(err)
	}
	capacity, err := provision.CheckJournalCapacity(ctx, js)
	if err != nil || !capacity.Alert {
		t.Fatalf("capacity=%+v err=%v", capacity, err)
	}
	t.Logf("fill used=%d limit=%d ratio=%f", capacity.UsedBytes, capacity.LimitBytes, capacity.Utilization)
	wait("pending shipping alert", time.Now().Add(10*time.Second), func() bool {
		data, err := get("/api/v1/alerts")
		if err != nil {
			return false
		}
		var value struct {
			Data struct {
				Alerts []struct {
					State  string
					Labels map[string]string
				}
			}
		}
		if json.Unmarshal(data, &value) != nil {
			return false
		}
		for _, alert := range value.Data.Alerts {
			if alert.State == "pending" && alert.Labels["alertname"] == "WorkflowJournalCapacityHigh" {
				write("pending-alert.json", string(data))
				return true
			}
		}
		return false
	})
	awaitDelivery := func(status string, limit time.Duration) capacityWebhook {
		t.Helper()
		timer := time.NewTimer(limit)
		defer timer.Stop()
		for {
			select {
			case payload := <-delivered:
				if payload.Status != status {
					continue
				}
				if payload.Version != "4" || payload.Receiver != "local-fixture" || len(payload.Alerts) != 1 {
					t.Fatalf("bad webhook: %+v", payload)
				}
				alert := payload.Alerts[0]
				if alert.Status != status || alert.Labels["alertname"] != "WorkflowJournalCapacityHigh" || alert.Labels["cluster"] != "monitoring-fixture" || alert.Labels["severity"] != "warning" || alert.Annotations["summary"] != "Workflow journal is at least 70% full" || alert.StartsAt.IsZero() {
					t.Fatalf("bad alert: %+v", alert)
				}
				return payload
			case <-timer.C:
				t.Fatalf("no %s webhook within %s", status, limit)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	firing := awaitDelivery("firing", 150*time.Second)
	firingAt := time.Now()
	if firingAt.Sub(filledAt) < 2*time.Minute {
		t.Fatalf("shipping two-minute hold bypassed: %s", firingAt.Sub(filledAt))
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject("wf.jrn.capacity.alert")); err != nil {
		t.Fatal(err)
	}
	resolved := awaitDelivery("resolved", 20*time.Second)
	if !resolved.Alerts[0].StartsAt.Equal(firing.Alerts[0].StartsAt) || resolved.Alerts[0].EndsAt.Before(firing.Alerts[0].StartsAt) {
		t.Fatalf("resolution changed alert identity: %+v", resolved)
	}
	capacity, err = provision.CheckJournalCapacity(ctx, js)
	if err != nil || capacity.Alert {
		t.Fatalf("resolved capacity=%+v err=%v", capacity, err)
	}
	t.Logf("firing_after_fill=%s resolved_after_firing=%s capacity_ratio=%f", firingAt.Sub(filledAt), time.Since(firingAt), capacity.Utilization)
}
