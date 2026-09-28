package sim

import (
	"fmt"
	"reflect"
)

// ReproduceFailure reruns a workload with an exact or guided trace. It must
// return a generated trace and an error that identifies the checker failure.
type ReproduceFailure func(*Trace) (Trace, error)

// MinimizeFailureTrace removes forced scheduler choices while preserving the
// same caller-selected failure. It tests progressively smaller chunks, then
// verifies the best generated trace with exact replay. A rejected candidate
// may be a different failure or a changed enabled set. maxRuns bounds work.
func MinimizeFailureTrace(original Trace, reproduce ReproduceFailure, sameFailure func(error) bool, maxRuns int) (Trace, int, error) {
	if err := original.validate(); err != nil {
		return Trace{}, 0, err
	}
	if reproduce == nil || sameFailure == nil || maxRuns < 2 {
		return Trace{}, 0, fmt.Errorf("invalid trace minimization configuration")
	}
	if original.GuidanceMask != nil {
		return Trace{}, 0, fmt.Errorf("original trace must use exact replay")
	}
	best, err := reproduce(&original)
	runs := 1
	if !sameFailure(err) || !reflect.DeepEqual(original, best) {
		return Trace{}, runs, fmt.Errorf("original trace did not reproduce the requested failure and transcript: %v", err)
	}
	keep := make([]bool, len(original.Decisions))
	for i := range keep {
		keep[i] = true
	}
	firstChunk := len(keep) / 2
	if len(keep) == 1 {
		firstChunk = 1
	}
	for chunk := firstChunk; chunk >= 1 && runs < maxRuns-1; chunk /= 2 {
		for start := 0; start < len(keep) && runs < maxRuns-1; start += chunk {
			candidate := append([]bool(nil), keep...)
			changed := false
			for i := start; i < start+chunk && i < len(candidate); i++ {
				if candidate[i] {
					candidate[i] = false
					changed = true
				}
			}
			if !changed {
				continue
			}
			guided := original
			guided.GuidanceMask = candidate
			generated, runErr := reproduce(&guided)
			runs++
			if sameFailure(runErr) {
				keep = candidate
				best = generated
			}
		}
	}
	// The minimized output must stand alone: its exact transport transcript and
	// enabled sets have to reproduce the failure without a guidance mask.
	replayed, err := reproduce(&best)
	runs++
	if !sameFailure(err) || !reflect.DeepEqual(best, replayed) {
		return Trace{}, runs, fmt.Errorf("minimized trace did not replay the requested failure and transcript: %v", err)
	}
	return best, runs, nil
}
