// Package integrity checks committed JetStream state without test bookkeeping.
package integrity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type Report struct {
	Invocations int
	Journals    int
	Entries     int
	Terminal    int
}

func scan(ctx context.Context, stream jetstream.Stream, visit func(*jetstream.RawStreamMsg) error) error {
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && seq != 0; seq++ {
		m, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := visit(m); err != nil {
			return err
		}
	}
	return nil
}

// Check verifies start-once, journal ordering, step outcome uniqueness and
// terminal state agreement over the retained streams. Run after quiescence.
func Check(ctx context.Context, js jetstream.JetStream) (Report, error) {
	var report Report
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return report, err
	}
	jrn, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return report, err
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return report, err
	}
	seen := map[string]struct{}{}
	if err := scan(ctx, inv, func(m *jetstream.RawStreamMsg) error {
		if _, ok := seen[m.Subject]; ok {
			return fmt.Errorf("duplicate invocation %s", m.Subject)
		}
		seen[m.Subject] = struct{}{}
		report.Invocations++
		return nil
	}); err != nil {
		return report, err
	}
	groups := map[string]struct{}{}
	if err := scan(ctx, jrn, func(m *jetstream.RawStreamMsg) error {
		var e journal.Entry
		if err := json.Unmarshal(m.Data, &e); err != nil {
			return err
		}
		groups[m.Subject] = struct{}{}
		return nil
	}); err != nil {
		return report, err
	}
	keys, err := state.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return report, err
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, "snap.") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(key, "snap."), ".")
		if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
			return report, fmt.Errorf("invalid snapshot key %s", key)
		}
		groups[identity.JournalSubject(parts[0], parts[1])] = struct{}{}
	}
	for subject := range groups {
		report.Journals++
		key := strings.TrimPrefix(subject, "wf.jrn.")
		parts := strings.Split(key, ".")
		if len(parts) != 2 || !strings.HasPrefix(subject, "wf.jrn.") {
			return report, fmt.Errorf("invalid journal subject %s", subject)
		}
		if _, ok := seen["wf.inv."+key]; !ok {
			return report, fmt.Errorf("journal %s has no invocation", subject)
		}
		records, _, err := journal.New(js).Read(ctx, parts[0], parts[1])
		if err != nil {
			return report, fmt.Errorf("%s: %w", subject, err)
		}
		report.Entries += len(records)
		var terminal []byte
		hasTerminal := false
		outstanding := false
		var lastSignalSeq uint64
		var lastAttempt int
		for i, record := range records {
			e := record.Entry
			if e.Index != uint64(i) {
				return report, fmt.Errorf("%s: index %d at position %d", subject, e.Index, i)
			}
			if i == 0 && e.Kind != journal.Started {
				return report, fmt.Errorf("%s: missing Started", subject)
			}
			if i > 0 && (e.Epoch < records[i-1].Epoch || e.Kind == journal.Started) {
				return report, fmt.Errorf("%s: epoch or Started violation", subject)
			}
			if hasTerminal {
				return report, fmt.Errorf("%s: entry after terminal", subject)
			}
			switch e.Kind {
			case journal.StepRequested:
				if outstanding {
					return report, fmt.Errorf("%s: overlapping step requests", subject)
				}
				outstanding = true
			case journal.StepCompleted:
				if !outstanding {
					return report, fmt.Errorf("%s: completion without request", subject)
				}
				outstanding = false
			case journal.Suspended:
				if !outstanding {
					return report, fmt.Errorf("%s: suspension without request", subject)
				}
			case journal.SignalConsumed:
				var sig struct {
					Sequence uint64 `json:"sig_seq"`
				}
				if err := json.Unmarshal(e.Payload, &sig); err != nil || sig.Sequence <= lastSignalSeq {
					return report, fmt.Errorf("%s: signal sequence out of order", subject)
				}
				lastSignalSeq = sig.Sequence
			case journal.Attempt:
				attempt, err := journal.DecodeAttempt(e.Payload)
				if err != nil || attempt.Count != lastAttempt+1 {
					return report, fmt.Errorf("%s: handler attempt out of order", subject)
				}
				lastAttempt = attempt.Count
			}
			if e.Kind == journal.Completed || e.Kind == journal.Failed {
				terminal = e.Payload
				hasTerminal = true
				report.Terminal++
			}
		}
		if hasTerminal {
			value, err := state.Get(ctx, key)
			if err != nil {
				return report, fmt.Errorf("%s: terminal state missing: %w", subject, err)
			}
			if !bytes.Equal(value.Value(), terminal) {
				return report, fmt.Errorf("%s: terminal state differs", subject)
			}
		}
	}
	return report, nil
}
