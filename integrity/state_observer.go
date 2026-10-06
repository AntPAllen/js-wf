package integrity

import (
	"context"
	"time"
)

// StateSnapshotObservation describes one initial-state watch milestone. Counts
// are local to this attempt; stopping a watch does not imply initial completion.
type StateSnapshotObservation struct {
	Event           string    `json:"event"`
	Time            time.Time `json:"time"`
	ElapsedNS       int64     `json:"elapsed_ns"`
	Deadline        time.Time `json:"deadline"`
	BudgetNS        int64     `json:"budget_ns"`
	Received        int       `json:"received"`
	Included        int       `json:"included"`
	LastRevision    uint64    `json:"last_revision"`
	InitialComplete bool      `json:"initial_complete"`
	Error           string    `json:"error,omitempty"`
}

type stateSnapshotObserverKey struct{}

// WithStateSnapshotObserver enables synchronous diagnostic milestones without
// relaying watch updates, changing options, extending deadlines or retaining
// outcome values. The observer must return promptly and must not call transport
// methods. Progress is sampled every 10,000 entries; final counts are exact.
func WithStateSnapshotObserver(ctx context.Context, observer func(StateSnapshotObservation)) context.Context {
	return context.WithValue(ctx, stateSnapshotObserverKey{}, observer)
}
