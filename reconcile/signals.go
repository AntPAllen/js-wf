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

type SignalScan struct {
	js     jetstream.JetStream
	client *client.Client
	jrn    *journal.Store
}

func NewSignalScan(js jetstream.JetStream) *SignalScan {
	return &SignalScan{js: js, client: client.New(js), jrn: journal.New(js)}
}

type signalJournalState struct {
	terminal bool
	consumed map[uint64]bool
}

// Scan repairs signal publishes whose matching wakeup was interrupted. The
// cursor is a WF_SIG stream sequence. It is safe to scan the same range again;
// the worker's lease and terminal check make duplicate wakeups harmless.
func (s *SignalScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	sig, err := s.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return ScanResult{}, err
	}
	inv, err := s.js.Stream(ctx, "WF_INV")
	if err != nil {
		return ScanResult{}, err
	}
	cache := map[string]signalJournalState{}
	result := ScanResult{NextSequence: next}
	for scanned := 0; scanned < budget; scanned++ {
		m, err := sig.GetMsg(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			info, infoErr := sig.Info(ctx)
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
		if len(parts) != 5 || parts[0] != "wf" || parts[1] != "sig" {
			return result, fmt.Errorf("invalid signal subject %q", m.Subject)
		}
		typ, id := parts[2], parts[3]
		if err := identity.Validate(typ, id); err != nil {
			return result, err
		}
		key := identity.Key(typ, id)
		state, ok := cache[key]
		if !ok {
			records, _, err := s.jrn.Read(ctx, typ, id)
			if err != nil {
				return result, err
			}
			state = signalJournalState{consumed: map[uint64]bool{}}
			for _, r := range records {
				if r.Kind == journal.Completed || r.Kind == journal.Failed {
					state.terminal = true
				}
				if r.Kind == journal.SignalConsumed {
					var event struct {
						Sequence uint64 `json:"sig_seq"`
					}
					if err := json.Unmarshal(r.Payload, &event); err != nil {
						return result, err
					}
					state.consumed[event.Sequence] = true
				}
			}
			cache[key] = state
		}
		if state.terminal || state.consumed[m.Sequence] {
			continue
		}
		invocation, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		} else if err != nil {
			return result, err
		}
		if generation := m.Header.Get("Wf-Inv-Seq"); generation != "" && generation != strconv.FormatUint(invocation.Sequence, 10) {
			continue
		}
		result.Reenqueued++
		if !dryRun {
			if err := s.client.Enqueue(ctx, typ, id, fmt.Sprintf("signal-wakeup:%d", m.Sequence)); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

// RunSignalLoop keeps one signal reconciler active with a KV lease.
func RunSignalLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "signal", interval, budget, NewSignalScan(js).Scan)
}
