//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/worker"
)

// This selection proof is preparation for an admitted timer cut. It does not
// itself prove SIGKILL occurred before the duration boundary or a later heal.
type matrixClockTimerAdmission struct {
	ID           string                         `json:"id"`
	Request      matrixControllerJournalReceipt `json:"request"`
	Suspended    matrixControllerJournalReceipt `json:"suspended"`
	Origin       worker.OperationEvent          `json:"origin"`
	Observed     time.Time                      `json:"observed"`
	EarliestDue  time.Time                      `json:"earliest_due"`
	SourceOffset time.Duration                  `json:"source_offset_ns"`
}

type matrixClockTimerCut struct {
	Admission *matrixClockTimerAdmission `json:"admission"`
	Tail      *jetstream.RawStreamMsg    `json:"refreshed_tail"`
	Refreshed time.Time                  `json:"refreshed"`
	Removed   time.Time                  `json:"removed"`
}

func refreshMatrixClockTimerCandidate(ctx context.Context, stream jetstream.Stream, candidate *matrixClockTimerAdmission) (*matrixClockTimerCut, error) {
	if candidate == nil {
		return nil, nil
	}
	tail, err := stream.GetLastMsgForSubject(ctx, candidate.Suspended.Subject)
	if err != nil {
		return nil, err
	}
	var entry journal.Entry
	if err := json.Unmarshal(tail.Data, &entry); err != nil {
		return nil, err
	}
	if tail.Sequence != candidate.Suspended.Sequence || tail.Subject != candidate.Suspended.Subject || !reflect.DeepEqual(entry, candidate.Suspended.Entry) {
		return nil, nil
	}
	refreshed := time.Now().UTC()
	if !refreshed.Add(750 * time.Millisecond).Before(candidate.EarliestDue) {
		return nil, nil
	}
	return &matrixClockTimerCut{Admission: candidate, Tail: tail, Refreshed: refreshed}, nil
}

// Select an observed durable positive Sleep request with the latest observed
// suspended tail, a matching shifted origin and enough time left for a cut.
// Receipts can lag. The caller must refresh the actual retained tail and verify
// kill/removal before EarliestDue; this candidate alone cannot admit a fault.
func selectMatrixPendingClockTimer(receipts []matrixControllerJournalReceipt, operations []worker.OperationEvent, now time.Time, offset, lead time.Duration) (*matrixClockTimerAdmission, error) {
	if now.IsZero() || offset == 0 || lead < 0 {
		return nil, fmt.Errorf("invalid clock timer admission arguments")
	}
	groups := map[string][]matrixControllerJournalReceipt{}
	for _, receipt := range receipts {
		if strings.HasPrefix(receipt.Subject, "wf.jrn.matrixtimer.") {
			groups[receipt.Subject] = append(groups[receipt.Subject], receipt)
		}
	}
	subjects := make([]string, 0, len(groups))
	for subject := range groups {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	for _, subject := range subjects {
		entries := groups[subject]
		sort.Slice(entries, func(i, j int) bool { return entries[i].Entry.Index < entries[j].Entry.Index })
		tail := entries[len(entries)-1]
		if tail.Entry.Kind != journal.Suspended || tail.ObservedAt.IsZero() || tail.ObservedAt.After(now) {
			continue
		}
		var wait struct {
			WaitingOn string `json:"waiting_on"`
		}
		if json.Unmarshal(tail.Entry.Payload, &wait) != nil {
			continue
		}
		var requested *matrixControllerJournalReceipt
		for i := len(entries) - 2; i >= 0; i-- {
			if entries[i].Entry.Kind == journal.StepCompleted {
				break
			}
			if entries[i].Entry.Kind == journal.StepRequested {
				candidate := entries[i]
				requested = &candidate
				break
			}
		}
		if requested == nil || requested.ObservedAt.IsZero() || requested.ObservedAt.After(now) {
			continue
		}
		var request struct {
			Kind     string    `json:"kind"`
			Name     string    `json:"name"`
			Duration int64     `json:"duration_nanos"`
			FireAt   time.Time `json:"fire_at"`
		}
		if json.Unmarshal(requested.Entry.Payload, &request) != nil || request.Kind != "timer" || request.Duration <= 0 || request.FireAt.IsZero() || wait.WaitingOn != "timer:"+request.Name {
			continue
		}
		id := strings.TrimPrefix(subject, "wf.jrn.matrixtimer.")
		var origins []worker.OperationEvent
		for _, op := range operations {
			if op.Type == "matrixtimer" && op.ID == id && op.Worker == requested.Entry.WorkerID && op.JournalIndex == requested.Entry.Index && op.JournalKind == journal.StepRequested && op.Operation == "timer_clock" && op.Error == "" && op.ServerTime != nil && op.ServerTime.Add(time.Duration(request.Duration)).Equal(request.FireAt) {
				origins = append(origins, op)
			}
		}
		if len(origins) > 1 {
			return nil, fmt.Errorf("ambiguous timer origin for %s index%d", id, requested.Entry.Index)
		}
		if len(origins) != 1 {
			continue
		}
		origin := origins[0]
		before := origin.At.Add(-origin.Duration)
		if origin.At.IsZero() || origin.Duration < 0 || origin.At.After(requested.ObservedAt) {
			continue
		}
		// Same two-second observation tolerance as the actual server clock profile.
		if origin.ServerTime.Before(before.Add(offset).Add(-2*time.Second)) || origin.ServerTime.After(origin.At.Add(offset).Add(2*time.Second)) {
			continue
		}
		due := before.Add(time.Duration(request.Duration))
		if !now.Add(lead).Before(due) {
			continue
		}
		return &matrixClockTimerAdmission{id, *requested, tail, origin, now, due, offset}, nil
	}
	return nil, nil
}
