package sim

import (
	"os"
	"sync/atomic"
)

// Optional aggregate for measuring extended seeded runs. Exact traces remain
// the source of truth for individual schedules and failure replay.
var coverageEnabled = os.Getenv("SIM_COVERAGE_SUMMARY") == "1"

var coverageSummary struct {
	schedules atomic.Int64
	choices   atomic.Int64
	events    atomic.Int64
	virtual   [4]atomic.Int64 // zero, below one second, below one minute, one minute or more
	maxMillis atomic.Int64
}

func recordCoverage(s *Scheduler) {
	if !coverageEnabled || s.replay != nil || s.trace.Workload == "" {
		return
	}
	coverageSummary.schedules.Add(1)
	coverageSummary.choices.Add(int64(len(s.trace.Decisions)))
	coverageSummary.events.Add(int64(len(s.trace.Transport)))
	bucket := 0
	switch {
	case s.now >= 60_000:
		bucket = 3
	case s.now >= 1_000:
		bucket = 2
	case s.now > 0:
		bucket = 1
	}
	coverageSummary.virtual[bucket].Add(1)
	for {
		old := coverageSummary.maxMillis.Load()
		if s.now <= old || coverageSummary.maxMillis.CompareAndSwap(old, s.now) {
			break
		}
	}
}
