package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// SuspendedScan inspects retained invocations whose latest journal entry is
// Suspended. It repairs an overdue timer or an available awaited signal.
type SuspendedScan struct {
	js     jetstream.JetStream
	client *client.Client
	jrn    *journal.Store
	Now    func() time.Time
	Grace  time.Duration
}

func NewSuspendedScan(js jetstream.JetStream) *SuspendedScan {
	return &SuspendedScan{js: js, client: client.New(js), jrn: journal.New(js), Now: time.Now, Grace: time.Second}
}

// Scan advances a stream-sequence cursor. The budget counts holes as well as
// retained invocations, so a heavily purged stream cannot monopolize a scan.
// In dry-run mode Candidates reports the messages that would be published.
func (s *SuspendedScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
	if budget < 1 || s.Grace < 0 {
		return ScanResult{}, fmt.Errorf("invalid suspended scan budget or grace")
	}
	if next == 0 {
		next = 1
	}
	inv, err := s.js.Stream(ctx, "WF_INV")
	if err != nil {
		return ScanResult{}, err
	}
	sig, err := s.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return ScanResult{}, err
	}
	result := ScanResult{NextSequence: next}
	for scanned := 0; scanned < budget; scanned++ {
		input, err := inv.GetMsg(ctx, next)
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
		next = input.Sequence + 1
		result.NextSequence = next
		result.Inspected++
		parts := strings.Split(input.Subject, ".")
		if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
			return result, fmt.Errorf("invalid invocation subject %q", input.Subject)
		}
		typ, id := parts[2], parts[3]
		records, _, err := s.jrn.Read(ctx, typ, id)
		if err != nil {
			return result, err
		}
		if len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
			continue
		}
		last := records[len(records)-1]
		var suspended struct {
			WaitingOn string `json:"waiting_on"`
		}
		if err := json.Unmarshal(last.Payload, &suspended); err != nil {
			return result, err
		}
		var ready bool
		var reason string
		switch {
		case strings.HasPrefix(suspended.WaitingOn, "timer:"):
			name := strings.TrimPrefix(suspended.WaitingOn, "timer:")
			if identity.ValidateToken(name) != nil {
				return result, fmt.Errorf("invalid timer wait %q", suspended.WaitingOn)
			}
			ready, err = s.timerDue(records, name)
			reason = "timer"
		case strings.HasPrefix(suspended.WaitingOn, "signal:"):
			name := strings.TrimPrefix(suspended.WaitingOn, "signal:")
			if identity.ValidateToken(name) != nil {
				return result, fmt.Errorf("invalid signal wait %q", suspended.WaitingOn)
			}
			ready, err = signalAvailable(ctx, sig, typ, id, name, input.Sequence, records)
			reason = "signal"
		case strings.HasPrefix(suspended.WaitingOn, "select:"):
			parts := strings.Split(suspended.WaitingOn, ":")
			if len(parts) != 3 || identity.ValidateToken(parts[1]) != nil || identity.ValidateToken(parts[2]) != nil {
				return result, fmt.Errorf("invalid select wait %q", suspended.WaitingOn)
			}
			ready, err = signalAvailable(ctx, sig, typ, id, parts[2], input.Sequence, records)
			if err == nil && !ready {
				ready, err = s.timerDue(records, parts[1])
			}
			reason = "select"
		default:
			return result, fmt.Errorf("unknown suspended wait %q", suspended.WaitingOn)
		}
		if err != nil {
			return result, err
		}
		if !ready {
			continue
		}
		candidate := Candidate{Type: typ, ID: id, Reason: reason, JournalSeq: last.Sequence}
		result.Candidates = append(result.Candidates, candidate)
		result.Reenqueued++
		if !dryRun {
			messageID := fmt.Sprintf("reconcile:%s:%s:%d", typ, id, last.Sequence)
			if err := s.client.Enqueue(ctx, typ, id, messageID); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func (s *SuspendedScan) timerDue(records []journal.Record, name string) (bool, error) {
	var pending *journal.Record
	for i := range records {
		switch records[i].Kind {
		case journal.StepRequested:
			pending = &records[i]
		case journal.StepCompleted:
			pending = nil
		}
	}
	if pending == nil {
		return false, nil
	}
	var req struct {
		Kind      string    `json:"kind"`
		Name      string    `json:"name"`
		TimerName string    `json:"timer_name"`
		FireAt    time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(pending.Payload, &req); err != nil {
		return false, err
	}
	if req.Kind == "timer_signal_select" {
		return req.TimerName == name && !req.FireAt.IsZero() && !s.Now().Before(req.FireAt.Add(s.Grace)), nil
	}
	if (req.Kind != "timer" && req.Kind != "timer_await") || req.Name != name || req.FireAt.IsZero() {
		return false, nil
	}
	return !s.Now().Before(req.FireAt.Add(s.Grace)), nil
}

func signalAvailable(ctx context.Context, sig jetstream.Stream, typ, id, name string, invSeq uint64, records []journal.Record) (bool, error) {
	used := map[uint64]bool{}
	var consumed []uint64
	var lastConsumed uint64
	for _, record := range records {
		switch record.Kind {
		case journal.StepCompleted:
			var done struct {
				SignalSeq uint64 `json:"signal_seq"`
			}
			if err := json.Unmarshal(record.Payload, &done); err != nil {
				return false, err
			}
			if done.SignalSeq != 0 {
				used[done.SignalSeq] = true
			}
		case journal.SignalConsumed:
			var event struct {
				Sequence uint64 `json:"sig_seq"`
				Name     string `json:"name"`
			}
			if err := json.Unmarshal(record.Payload, &event); err != nil || event.Sequence == 0 {
				return false, fmt.Errorf("invalid consumed signal in journal")
			}
			if event.Sequence > lastConsumed {
				lastConsumed = event.Sequence
			}
			if event.Name == name {
				consumed = append(consumed, event.Sequence)
			}
		}
	}
	for _, seq := range consumed {
		if !used[seq] {
			return true, nil
		}
	}
	subject := "wf.sig." + typ + "." + id + "." + name
	for seq := lastConsumed + 1; seq != 0; {
		message, err := sig.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		seq = message.Sequence + 1
		if generation := message.Header.Get("Wf-Inv-Seq"); generation != "" && generation != strconv.FormatUint(invSeq, 10) {
			continue
		}
		if !used[message.Sequence] {
			return true, nil
		}
	}
	return false, nil
}

// RunSuspendedLoop uses the same KV leader lease and persisted cursor as the
// other reconcilers, under its own suspended scan key.
func RunSuspendedLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "suspended", interval, budget, NewSuspendedScan(js).Scan)
}
