package client

import (
	"errors"
	"fmt"
	"testing"

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
)

func TestCanonicalSignalConflictIsNotGenerationReplacement(t *testing.T) {
	for _, conflict := range []error{graphpublication.ErrConflict, blobpublication.ErrConflict} {
		cause := fmt.Errorf("%w: %w", journal.ErrStale, conflict)
		got := canonicalSignalError(cause)
		if !errors.Is(got, ErrSignalUnknown) || errors.Is(got, ErrStaleGeneration) || !errors.Is(got, conflict) {
			t.Fatalf("CAS conflict classified as replaced generation: %v", got)
		}
	}
	if got := canonicalSignalError(journal.ErrStale); !errors.Is(got, ErrStaleGeneration) || errors.Is(got, ErrSignalUnknown) {
		t.Fatalf("actual stale generation classification: %v", got)
	}
}
