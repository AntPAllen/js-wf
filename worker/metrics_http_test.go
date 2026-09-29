package worker

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestMetricsHandlerAggregatesLatencyDistributions(t *testing.T) {
	first, second := &Worker{}, &Worker{}
	first.metrics.leaseAcquisitions.Add(2)
	second.metrics.leaseAcquisitions.Add(3)
	acquired := time.Unix(100, 0)
	for _, latency := range []time.Duration{100 * time.Millisecond, 30*time.Second + time.Nanosecond} {
		first.metrics.recordLeaseLatency(&jetstream.MsgMetadata{Timestamp: acquired.Add(-latency)}, acquired)
	}
	for _, latency := range []time.Duration{500 * time.Millisecond, 2 * time.Second, 31 * time.Second} {
		second.metrics.recordLeaseLatency(&jetstream.MsgMetadata{Timestamp: acquired.Add(-latency)}, acquired)
	}
	for _, latency := range []time.Duration{0, 30*time.Second + time.Millisecond} {
		first.metrics.recordTimerFired(acquired, acquired.Add(latency))
	}
	for _, latency := range []time.Duration{250 * time.Millisecond, 5 * time.Second} {
		second.metrics.recordTimerFired(acquired, acquired.Add(latency))
	}
	handler := MetricsHandler(first, second)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("scrape status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	body := response.Body.String()
	for _, line := range []string{
		"js_wf_worker_lease_acquisitions_total 5",
		"js_wf_worker_enqueue_to_lease_seconds_bucket{le=\"0.1\"} 1",
		"js_wf_worker_enqueue_to_lease_seconds_bucket{le=\"0.5\"} 2",
		"js_wf_worker_enqueue_to_lease_seconds_bucket{le=\"2\"} 3",
		"js_wf_worker_enqueue_to_lease_seconds_bucket{le=\"30\"} 3",
		"js_wf_worker_enqueue_to_lease_seconds_bucket{le=\"+Inf\"} 5",
		"js_wf_worker_enqueue_to_lease_seconds_count 5",
		"js_wf_worker_enqueue_to_lease_max_seconds 31.000000000",
		"js_wf_worker_timer_lateness_seconds_bucket{le=\"0.5\"} 2",
		"js_wf_worker_timer_lateness_seconds_bucket{le=\"+Inf\"} 4",
		"js_wf_worker_timer_lateness_seconds_count 4",
	} {
		if !strings.Contains(body, line+"\n") {
			t.Fatalf("scrape lacks %q:\n%s", line, body)
		}
	}
	request := httptest.NewRequest(http.MethodPost, "/metrics", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%d, want 405", response.Code)
	}
}
