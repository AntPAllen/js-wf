package integration_test

import (
	"testing"
	"time"
)

func TestSleepRecoveryLatenessStartsAfterFinalHealOrDue(t *testing.T) {
	base := time.Unix(0, 0)
	for _, tc := range []struct {
		name      string
		completed time.Duration
		due       time.Duration
		healed    time.Duration
		want      time.Duration
	}{
		{name: "due before heal", completed: 35 * time.Second, due: time.Second, healed: 30 * time.Second, want: 5 * time.Second},
		{name: "due after heal", completed: 35 * time.Second, due: 32 * time.Second, healed: 30 * time.Second, want: 3 * time.Second},
		{name: "completed during fault", completed: 25 * time.Second, due: time.Second, healed: 30 * time.Second, want: 0},
		{name: "completed at heal", completed: 30 * time.Second, due: time.Second, healed: 30 * time.Second, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sleepRecoveryLateness(base.Add(tc.completed), base.Add(tc.due), base.Add(tc.healed))
			if got != tc.want {
				t.Fatalf("recovery lateness=%s, want %s", got, tc.want)
			}
		})
	}
}
