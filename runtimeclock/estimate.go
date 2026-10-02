// Package runtimeclock estimates a common time interval from independent server
// clocks. Adapters must authenticate server identity and use monotonic elapsed
// times from a single local anchor, rather than compare worker wall clocks.
package runtimeclock

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var ErrNoAgreement = errors.New("insufficient independent clock agreement")

// Observation brackets the instant at which a server sampled its wall clock.
// Started and Finished are monotonic durations from the same caller anchor.
// Multiple streams led by the same server are not independent observations.
type Observation struct {
	Server            string
	Time              time.Time
	Started, Finished time.Duration
}

// Interval bounds UTC at the caller's anchor. It is not a timestamp at return;
// advance both bounds by monotonic time elapsed since that anchor before use.
type Interval struct {
	Lower, Upper time.Time
	Sources      []string
}

type interval struct {
	lower, upper time.Time
	server       string
}

// Estimate tolerates maxSkewed arbitrary server clocks. Every accepted point
// must be supported by maxSkewed+1 distinct observations, so at least one of its
// supporters has clock error at most healthyError. Expanding that point by the
// largest observation width bounds the true anchor time even when a skewed
// supporter narrows the intersection to an edge of the healthy interval.
//
// No agreement, duplicate identities, malformed brackets and excessive RPC
// latency fail closed. The caller must obtain authentic independent identities;
// this function cannot detect aliases for one physical server.
func Estimate(observations []Observation, maxSkewed int, maxRoundTrip, healthyError time.Duration) (Interval, error) {
	var result Interval
	const maxDuration time.Duration = 1<<63 - 1
	if maxSkewed < 0 || maxSkewed > 2 || maxRoundTrip <= 0 || healthyError < 0 || healthyError > (maxDuration-maxRoundTrip)/2 || len(observations) > 5 {
		return result, fmt.Errorf("invalid clock estimate configuration")
	}
	seen := map[string]bool{}
	var intervals []interval
	var width time.Duration
	for _, observation := range observations {
		if observation.Server == "" || seen[observation.Server] || observation.Time.IsZero() || observation.Started < 0 || observation.Finished < observation.Started || observation.Finished-observation.Started > maxRoundTrip {
			return result, fmt.Errorf("invalid or non-independent clock observation: %q", observation.Server)
		}
		seen[observation.Server] = true
		lower := observation.Time.UTC().Add(-observation.Finished).Add(-healthyError)
		upper := observation.Time.UTC().Add(-observation.Started).Add(healthyError)
		if span := upper.Sub(lower); span > width {
			width = span
		}
		intervals = append(intervals, interval{lower, upper, observation.Server})
	}
	needed := maxSkewed + 1
	if len(intervals) < needed {
		return result, ErrNoAgreement
	}
	contributors := map[string]bool{}
	found := false
	// At most five sources give at most ten supported pairs/triples. Enumerate
	// their intersections rather than let input ordering choose a clock leader.
	var visit func(int, []interval)
	visit = func(next int, chosen []interval) {
		if len(chosen) == needed {
			lower, upper := chosen[0].lower, chosen[0].upper
			for _, entry := range chosen[1:] {
				if entry.lower.After(lower) {
					lower = entry.lower
				}
				if entry.upper.Before(upper) {
					upper = entry.upper
				}
			}
			if lower.After(upper) {
				return
			}
			if !found || lower.Before(result.Lower) {
				result.Lower = lower
			}
			if !found || upper.After(result.Upper) {
				result.Upper = upper
			}
			found = true
			for _, entry := range chosen {
				contributors[entry.server] = true
			}
			return
		}
		for i := next; i < len(intervals); i++ {
			visit(i+1, append(chosen, intervals[i]))
		}
	}
	visit(0, nil)
	if !found {
		return Interval{}, ErrNoAgreement
	}
	result.Lower = result.Lower.Add(-width)
	result.Upper = result.Upper.Add(width)
	for server := range contributors {
		result.Sources = append(result.Sources, server)
	}
	sort.Strings(result.Sources)
	return result, nil
}
