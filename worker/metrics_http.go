package worker

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

// AggregateMetrics combines current snapshots from workers in one process.
// Counters reset when these Worker instances are replaced.
func AggregateMetrics(workers ...*Worker) Metrics {
	var total Metrics
	for _, worker := range workers {
		if worker == nil {
			continue
		}
		part := worker.Metrics()
		total.LeaseAcquisitions += part.LeaseAcquisitions
		total.LeaseContentions += part.LeaseContentions
		total.LeaseAcquireFailures += part.LeaseAcquireFailures
		total.FencingEvents += part.FencingEvents
		total.HandoffEnqueues += part.HandoffEnqueues
		total.Redeliveries += part.Redeliveries
		total.EnqueueToLeaseSamples += part.EnqueueToLeaseSamples
		total.EnqueueToLeaseTotal += part.EnqueueToLeaseTotal
		if part.EnqueueToLeaseMaximum > total.EnqueueToLeaseMaximum {
			total.EnqueueToLeaseMaximum = part.EnqueueToLeaseMaximum
		}
		total.TimersScheduled += part.TimersScheduled
		total.TimersFired += part.TimersFired
		total.TimerLateTotal += part.TimerLateTotal
		if part.TimerLateMaximum > total.TimerLateMaximum {
			total.TimerLateMaximum = part.TimerLateMaximum
		}
		total.CancelledTimerNoOps += part.CancelledTimerNoOps
		for i := range total.TimerLateBuckets {
			total.TimerLateBuckets[i] += part.TimerLateBuckets[i]
			total.EnqueueToLeaseBuckets[i] += part.EnqueueToLeaseBuckets[i]
		}
	}
	return total
}

// MetricsHandler exports an aggregate of the supplied workers in Prometheus
// text format. Mount it on the application's HTTP server, for example at
// /metrics. It takes a fresh snapshot on every request.
func MetricsHandler(workers ...*Worker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		view := AggregateMetrics(workers...)
		var output bytes.Buffer
		counter := func(name string, value uint64) {
			fmt.Fprintf(&output, "# TYPE %s counter\n%s %d\n", name, name, value)
		}
		gaugeSeconds := func(name string, value time.Duration) {
			fmt.Fprintf(&output, "# TYPE %s gauge\n%s %.9f\n", name, name, value.Seconds())
		}
		counter("js_wf_worker_lease_acquisitions_total", view.LeaseAcquisitions)
		counter("js_wf_worker_lease_contentions_total", view.LeaseContentions)
		counter("js_wf_worker_lease_acquire_failures_total", view.LeaseAcquireFailures)
		counter("js_wf_worker_fencing_events_total", view.FencingEvents)
		counter("js_wf_worker_handoff_enqueues_total", view.HandoffEnqueues)
		counter("js_wf_worker_redeliveries_total", view.Redeliveries)
		counter("js_wf_worker_timers_scheduled_total", view.TimersScheduled)
		counter("js_wf_worker_timers_fired_total", view.TimersFired)
		counter("js_wf_worker_cancelled_timer_noops_total", view.CancelledTimerNoOps)
		writeLatencyHistogram(&output, "js_wf_worker_enqueue_to_lease_seconds", view.EnqueueToLeaseBuckets, view.EnqueueToLeaseTotal)
		gaugeSeconds("js_wf_worker_enqueue_to_lease_max_seconds", view.EnqueueToLeaseMaximum)
		writeLatencyHistogram(&output, "js_wf_worker_timer_lateness_seconds", view.TimerLateBuckets, view.TimerLateTotal)
		gaugeSeconds("js_wf_worker_timer_lateness_max_seconds", view.TimerLateMaximum)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write(output.Bytes())
	})
}

func writeLatencyHistogram(output *bytes.Buffer, name string, buckets [6]uint64, sum time.Duration) {
	fmt.Fprintf(output, "# TYPE %s histogram\n", name)
	boundaries := [...]string{"0.1", "0.5", "2", "10", "30"}
	var count uint64
	for i, boundary := range boundaries {
		count += buckets[i]
		fmt.Fprintf(output, "%s_bucket{le=%q} %d\n", name, boundary, count)
	}
	count += buckets[len(buckets)-1]
	fmt.Fprintf(output, "%s_bucket{le=\"+Inf\"} %d\n", name, count)
	fmt.Fprintf(output, "%s_sum %.9f\n%s_count %d\n", name, sum.Seconds(), name, count)
}
