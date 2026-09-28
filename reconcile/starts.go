package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"js-wf/client"

	"github.com/nats-io/nats.go/jetstream"
)

type StartScan struct {
	js     jetstream.JetStream
	client *client.Client
}

func NewStartScan(js jetstream.JetStream) *StartScan {
	return &StartScan{js: js, client: client.New(js)}
}

type ScanResult struct {
	NextSequence uint64      `json:"next_sequence"`
	Inspected    int         `json:"inspected"` // retained messages; holes also consume budget
	Reenqueued   int         `json:"reenqueued"`
	Removed      int         `json:"removed,omitempty"`
	Candidates   []Candidate `json:"candidates,omitempty"`
}

type Candidate struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Reason     string `json:"reason"`
	JournalSeq uint64 `json:"journal_seq"`
}

// Scan repairs the gap between an acked WF_INV publish and its WF_RUN publish.
// next is a stream sequence cursor; zero starts at the beginning. The caller
// persists the cursor and runs the scan periodically. An extra wakeup is safe
// only once the worker treats an existing terminal journal as a no-op.
func (s *StartScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	inv, err := s.js.Stream(ctx, "WF_INV")
	if err != nil {
		return ScanResult{}, err
	}
	jrn, err := s.js.Stream(ctx, "WF_JRN")
	if err != nil {
		return ScanResult{}, err
	}
	result := ScanResult{NextSequence: next}
	for scanned := 0; scanned < budget; scanned++ {
		m, err := inv.GetMsg(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			info, infoErr := inv.Info(ctx)
			if infoErr != nil {
				return result, infoErr
			}
			if next > info.State.LastSeq {
				result.NextSequence = 1
				return result, nil
			}
			next++
			result.NextSequence = next
			continue
		}
		if err != nil {
			return result, err
		}
		next = m.Sequence + 1
		result.NextSequence = next
		result.Inspected++
		parts := strings.Split(m.Subject, ".")
		if len(parts) != 4 {
			return result, fmt.Errorf("invalid invocation subject %q", m.Subject)
		}
		typ, id := parts[2], parts[3]
		_, err = jrn.GetLastMsgForSubject(ctx, "wf.jrn."+typ+"."+id)
		if err == nil {
			continue
		}
		if !errors.Is(err, jetstream.ErrMsgNotFound) {
			return result, err
		}
		result.Reenqueued++
		if dryRun {
			continue
		}
		if err := s.client.Enqueue(ctx, typ, id, "start:"+typ+"."+id+":"+strconv.FormatUint(m.Sequence, 10)); err != nil {
			return result, err
		}
	}
	return result, nil
}

// RunStartLoop scans continuously with a KV lease so one process is active at
// a time. A replacement starts from the beginning; repeated enqueue attempts
// carry the same message ID and workers treat extra wakeups idempotently.
func RunStartLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "start", interval, budget, NewStartScan(js).Scan)
}
