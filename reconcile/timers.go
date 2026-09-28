package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"js-wf/client"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type TimerScan struct {
	js     jetstream.JetStream
	client *client.Client
	jrn    *journal.Store
	Now    func() time.Time
}

func NewTimerScan(js jetstream.JetStream) *TimerScan {
	return &TimerScan{js: js, client: client.New(js), jrn: journal.New(js), Now: time.Now}
}

// Scan enqueues a fresh run for each invocation whose journal still has an
// uncompleted sleep, timer creation, or timer await past its fire time. It
// repairs missing schedules: a due timer can use this wakeup's server time.
func (s *TimerScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
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
		records, _, err := s.jrn.Read(ctx, typ, id)
		if err != nil {
			return result, err
		}
		if len(records) == 0 {
			continue
		}
		if records[len(records)-1].Kind == journal.Completed || records[len(records)-1].Kind == journal.Failed {
			continue
		}
		var pending *journal.Record
		for i := range records {
			r := &records[i]
			switch r.Kind {
			case journal.StepRequested:
				pending = r
			case journal.StepCompleted:
				pending = nil
			}
		}
		if pending == nil {
			continue
		}
		var req struct {
			Kind   string    `json:"kind"`
			FireAt time.Time `json:"fire_at"`
		}
		if err := json.Unmarshal(pending.Payload, &req); err != nil {
			return result, err
		}
		if req.Kind != "timer" && req.Kind != "timer_start" && req.Kind != "timer_await" || req.FireAt.IsZero() || s.Now().Before(req.FireAt) {
			continue
		}
		result.Reenqueued++
		if !dryRun {
			if err := s.client.Enqueue(ctx, typ, id, fmt.Sprintf("timer-reconcile:%s:%s:%d", typ, id, pending.Sequence)); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func RunTimerLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "timer", interval, budget, NewTimerScan(js).Scan)
}
