package sim

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	start := time.Now()
	code := m.Run()
	if coverageEnabled {
		elapsed := time.Since(start)
		choices := coverageSummary.choices.Load()
		seeds := os.Getenv("SIM_SEEDS")
		if seeds == "" {
			seeds = "1000"
		}
		fmt.Printf("TIER1_COVERAGE model_version=%d seeds_per_workload=%s generated_schedules=%d scheduler_choices=%d choices_per_second=%.1f transport_events=%d virtual_ms_max=%d virtual_buckets_zero=%d under_1s=%d under_1m=%d at_least_1m=%d elapsed=%s\n",
			TraceVersion, seeds, coverageSummary.schedules.Load(), choices,
			float64(choices)/elapsed.Seconds(), coverageSummary.events.Load(), coverageSummary.maxMillis.Load(),
			coverageSummary.virtual[0].Load(), coverageSummary.virtual[1].Load(), coverageSummary.virtual[2].Load(), coverageSummary.virtual[3].Load(), elapsed)
	}
	os.Exit(code)
}
