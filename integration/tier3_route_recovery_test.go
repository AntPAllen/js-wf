//go:build linux

package integration_test

import (
	"testing"
	"time"
)

func TestTier3RouteRecoveryKeepsHealthyStallsVisible(t *testing.T) {
	origin := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return origin.Add(time.Duration(seconds) * time.Second) }
	faults := []matrixLeaderFault{{Killed: at(10), Healed: at(50)}, {Killed: at(70), Healed: at(80)}}
	for _, test := range []struct {
		name                    string
		enabled, observed, want int
	}{
		{"before outage", 0, 9, 9},
		{"completed before heal", 15, 20, 0},
		{"after heal healthy stall", 51, 69, 18},
		{"spans outage", 0, 65, 15},
		{"spans repeated outages", 0, 90, 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			sample := matrixLatencySample{Enabled: at(test.enabled), Observed: at(test.observed)}
			if got := tier3RouteRecoveryDelay(sample, faults); got != time.Duration(test.want)*time.Second {
				t.Fatalf("recovery=%s want=%ds", got, test.want)
			}
		})
	}
}
