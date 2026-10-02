package runtimeclock

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestEstimateBoundsAnchorUnderSkewLossAndReordering(t *testing.T) {
	anchor := time.Unix(1_700_000_000, 0).UTC()
	// Every subset of five servers with one skewed clock and two unavailable
	// sources. Healthy error endpoints and all observation orders are checked.
	for bad := 0; bad < 5; bad++ {
		for _, skew := range []time.Duration{-time.Minute, time.Minute} {
			for missing := 0; missing < 32; missing++ {
				var observations []Observation
				for node := 0; node < 5; node++ {
					if missing&(1<<node) != 0 {
						continue
					}
					start := time.Duration(node) * time.Millisecond
					clockError := time.Duration(node%3-1) * time.Millisecond
					if node == bad {
						clockError = skew
					}
					observations = append(observations, Observation{fmt.Sprint(node), anchor.Add(start + 2*time.Millisecond + clockError), start, start + 4*time.Millisecond})
				}
				var expected Interval
				var expectedError bool
				var permutations func(int)
				permutations = func(index int) {
					if index < len(observations) {
						for i := index; i < len(observations); i++ {
							observations[index], observations[i] = observations[i], observations[index]
							permutations(index + 1)
							observations[index], observations[i] = observations[i], observations[index]
						}
						return
					}
					result, err := Estimate(observations, 1, time.Second, time.Millisecond)
					if err != nil && !errors.Is(err, ErrNoAgreement) {
						t.Fatal(err)
					}
					if err == nil && (result.Lower.After(anchor) || result.Upper.Before(anchor) || result.Upper.Sub(result.Lower) > 20*time.Millisecond) {
						t.Fatalf("unsafe or skew-dominated estimate: bad=%d missing=%d result=%+v", bad, missing, result)
					}
					if expected.Sources == nil && !expectedError {
						expected = result
						expectedError = err != nil
					} else if (err != nil) != expectedError || !reflect.DeepEqual(result, expected) {
						t.Fatal("observation order changes decision")
					}
				}
				permutations(0)
				availableHealthy := 0
				for node := 0; node < 5; node++ {
					if node != bad && missing&(1<<node) == 0 {
						availableHealthy++
					}
				}
				if availableHealthy >= 2 && expectedError {
					t.Fatalf("healthy agreement rejected: bad=%d missing=%d", bad, missing)
				}
				if availableHealthy < 2 && !expectedError {
					t.Fatalf("unsupported estimate accepted: bad=%d missing=%d", bad, missing)
				}
			}
		}
	}
}

func TestEstimateDoesNotTrustNarrowedIntersectionAlone(t *testing.T) {
	anchor := time.Unix(1_700_000_000, 0).UTC()
	// A skewed response intersects only the edge of a healthy RPC bracket;
	// the actual anchor lies outside that unexpanded intersection.
	result, err := Estimate([]Observation{
		{Server: "healthy", Time: anchor.Add(10 * time.Millisecond), Finished: 10 * time.Millisecond},
		{Server: "skewed", Time: anchor.Add(9 * time.Millisecond)},
	}, 1, time.Second, 0)
	if err != nil || result.Lower.After(anchor) || result.Upper.Before(anchor) {
		t.Fatalf("unsafe narrow agreement: %+v %v", result, err)
	}
}

func TestEstimateRejectsDuplicateMalformedAndUnboundedSources(t *testing.T) {
	anchor := time.Unix(1_700_000_000, 0).UTC()
	base := []Observation{{Server: "a", Time: anchor, Finished: time.Millisecond}, {Server: "b", Time: anchor, Finished: time.Millisecond}}
	for _, mode := range []string{"duplicate", "empty_id", "zero_time", "negative_start", "reversed", "slow", "disagreement"} {
		t.Run(mode, func(t *testing.T) {
			inputs := append([]Observation(nil), base...)
			switch mode {
			case "duplicate":
				inputs[1].Server = "a"
			case "empty_id":
				inputs[1].Server = ""
			case "zero_time":
				inputs[1].Time = time.Time{}
			case "negative_start":
				inputs[1].Started = -time.Millisecond
			case "reversed":
				inputs[1].Started = 2 * time.Millisecond
			case "slow":
				inputs[1].Finished = 2 * time.Second
			case "disagreement":
				inputs[1].Time = anchor.Add(time.Minute)
			}
			if _, err := Estimate(inputs, 1, time.Second, 0); err == nil {
				t.Fatal("unsafe observations accepted")
			}
		})
	}
}

func TestEstimateBoundsAnchorWithTwoSkewedServers(t *testing.T) {
	anchor := time.Unix(1_700_000_000, 0).UTC()
	for first := 0; first < 5; first++ {
		for second := first + 1; second < 5; second++ {
			for _, a := range []time.Duration{-time.Minute, time.Minute, 9 * time.Millisecond} {
				for _, b := range []time.Duration{-time.Minute, time.Minute, 9 * time.Millisecond} {
					var observations []Observation
					for node := 0; node < 5; node++ {
						when := anchor.Add(5 * time.Millisecond)
						if node == first {
							when = anchor.Add(a)
						}
						if node == second {
							when = anchor.Add(b)
						}
						observations = append(observations, Observation{Server: fmt.Sprint(node), Time: when, Finished: 10 * time.Millisecond})
					}
					result, err := Estimate(observations, 2, time.Second, 0)
					if err != nil || result.Lower.After(anchor) || result.Upper.Before(anchor) {
						t.Fatalf("two skewed clocks escaped interval: %d %d %+v %v", first, second, result, err)
					}
				}
			}
		}
	}
}

func TestEstimateRejectsOverflowingErrorWindow(t *testing.T) {
	if _, err := Estimate(nil, 1, time.Second, time.Duration(1<<63-1)); err == nil {
		t.Fatal("overflowing uncertainty window accepted")
	}
}
