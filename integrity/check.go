// Package integrity checks committed JetStream state without test bookkeeping.
package integrity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	subjects := make([]string, 0, len(groups))
	for subject := range groups {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	for _, subject := range subjects {
		key, err := journalKey(subject, seen)
		if err != nil {
			return report, err
		}
		parts := strings.Split(key, ".")
		report.Journals++
		records, _, err := journal.New(js).Read(ctx, parts[0], parts[1])
		if err != nil {
			return report, fmt.Errorf("%s: %w", subject, err)
		}
		entries, terminal, err := checkJournalRecords(subject, records, func() ([]byte, error) {
			value, err := state.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			return value.Value(), nil
		})
		if err != nil {
			return report, err
		}
		report.Entries += entries
		if terminal {
			report.Terminal++
		}
	}
	return report, nil
}
