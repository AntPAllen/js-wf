package sim

import (
	"os"
	"strconv"
	"testing"
)

// seededScheduleLimit keeps the per-workload commit gate at 1,000 while
// allowing an explicit larger seed range for CI timing and release runs.
func seededScheduleLimit(t *testing.T) int64 {
	t.Helper()
	raw := os.Getenv("SIM_SEEDS")
	if raw == "" {
		return 1000
	}
	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || limit < 1000 || limit > 1_000_000 {
		t.Fatalf("invalid SIM_SEEDS=%q: want 1000..1000000", raw)
	}
	return limit
}

// seededSchedules records completed loop bodies rather than inferring coverage
// from the configured limit. A break, early return or Fatal leaves a short count.
func seededSchedules(t *testing.T) func(func(int64) bool) {
	t.Helper()
	limit := seededScheduleLimit(t)
	var completed int64
	t.Cleanup(func() {
		if coverageEnabled {
			t.Logf("TIER1_SEEDS test=%s first=1 last=%d completed=%d requested=%d", t.Name(), completed, completed, limit)
		}
		if completed != limit && !t.Failed() {
			t.Errorf("seed range incomplete: completed %d of %d", completed, limit)
		}
	})
	return trackedSeeds(limit, &completed)
}

func trackedSeeds(limit int64, completed *int64) func(func(int64) bool) {
	return func(yield func(int64) bool) {
		for seed := int64(1); seed <= limit; seed++ {
			if !yield(seed) {
				return
			}
			*completed++
		}
	}
}

func TestSeedIterationAccountsCompletedBodies(t *testing.T) {
	var complete int64
	for seed := range trackedSeeds(3, &complete) {
		if seed == 2 {
			continue
		}
	}
	if complete != 3 {
		t.Fatalf("completed=%d", complete)
	}
	var interrupted int64
	for seed := range trackedSeeds(3, &interrupted) {
		if seed == 2 {
			break
		}
	}
	if interrupted != 1 {
		t.Fatalf("interrupted=%d", interrupted)
	}
}
