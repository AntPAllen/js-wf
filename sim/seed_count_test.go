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
