// Package integrity checks committed JetStream state without test bookkeeping.
package integrity

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"sort"
	"strings"
	"sync"
	"time"

	"js-wf/identity"
	"js-wf/internal/natsutil"
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
	return scanThrough(ctx, stream, nil, visit)
}

func scanThrough(ctx context.Context, stream jetstream.Stream, cutoff *uint64, visit func(*jetstream.RawStreamMsg) error) error {
	info, err := auditRead(ctx, func(attempt context.Context) (*jetstream.StreamInfo, error) { return stream.Info(attempt) })
	if err != nil {
		return err
	}
	if cutoff != nil && info.State.LastSeq > *cutoff {
		info.State.LastSeq = *cutoff
	}
	// Read a bounded window in parallel, then visit it in stream order. This
	// preserves exact sequence requests and hole detection without introducing
	// a consumer, while avoiding one network round trip per serial entry.
	const window = 32
	for first := info.State.FirstSeq; first <= info.State.LastSeq && first != 0; {
		count := uint64(window)
		if remaining := info.State.LastSeq - first + 1; remaining < count {
			count = remaining
		}
		type result struct {
			msg *jetstream.RawStreamMsg
			err error
		}
		results := make([]result, count)
		var readers sync.WaitGroup
		for offset := range results {
			readers.Add(1)
			go func() {
				defer readers.Done()
				sequence := first + uint64(offset)
				results[offset].msg, results[offset].err = auditRead(ctx, func(attempt context.Context) (*jetstream.RawStreamMsg, error) {
					return stream.GetMsg(attempt, sequence)
				})
			}()
		}
		readers.Wait()
		for offset, result := range results {
			if errors.Is(result.err, jetstream.ErrMsgNotFound) {
				continue
			}
			if result.err != nil {
				return result.err
			}
			sequence := first + uint64(offset)
			if result.msg == nil || result.msg.Sequence != sequence {
				return fmt.Errorf("retained scan: response does not match requested sequence %d", sequence)
			}
			if err := visit(result.msg); err != nil {
				return err
			}
		}
		if count > info.State.LastSeq-first {
			break
		}
		first += count
	}

	return nil
}

// Check verifies start-once, journal ordering, step outcome uniqueness and
// terminal state agreement over the retained streams. Run after quiescence.
func Check(ctx context.Context, js jetstream.JetStream) (Report, error) {
	return check(ctx, js, nil)
}

// CheckThroughInvocationSequence checks the retained cohort at or below a
// captured WF_INV high-water mark while later invocations may continue running.
// The caller must ensure the captured cohort is quiescent and is not purged or
// reused during the check. Its journals and snapshots are read at audit time,
// so later corruption of a cohort journal is still detected. Records belonging
// to later invocations are excluded; a final Check remains necessary to check
// the entire retained state, including journals without invocations.
func CheckThroughInvocationSequence(ctx context.Context, js jetstream.JetStream, cutoff uint64) (Report, error) {
	return check(ctx, js, &cutoff)
}

// CheckWithBatchedReads applies the same full retained-state invariants as Check
// using bounded pull batches. Gap and tail coverage is confirmed by leader
// next-message reads. Consumers are temporary; no stream config is changed.
func CheckWithBatchedReads(ctx context.Context, js jetstream.JetStream) (Report, error) {
	return checkUsing(ctx, js, nil, scanBatchThrough)
}

// CheckThroughInvocationSequenceWithBatchedReads has the same quiescent-cohort
// and no-purge/no-reuse requirements as CheckThroughInvocationSequence. Each
// call rereads retained journals, snapshots and terminal values; no old audit
// result is cached as evidence. A final full check still covers orphan journals.
func CheckThroughInvocationSequenceWithBatchedReads(ctx context.Context, js jetstream.JetStream, cutoff uint64) (Report, error) {
	return checkUsing(ctx, js, &cutoff, scanBatchThrough)
}

type retainedScanner func(context.Context, jetstream.Stream, *uint64, func(*jetstream.RawStreamMsg) error) error

func check(ctx context.Context, js jetstream.JetStream, cutoff *uint64) (Report, error) {
	return checkUsing(ctx, js, cutoff, scanThrough)
}

func checkUsing(ctx context.Context, js jetstream.JetStream, cutoff *uint64, read retainedScanner) (Report, error) {
	return checkUsingState(ctx, js, cutoff, read, false)
}

// CheckWithBatchedStateReads adds a fresh initial KV watch snapshot to the
// batched stream reader. The complete retained state must be quiescent.
func CheckWithBatchedStateReads(ctx context.Context, js jetstream.JetStream) (Report, error) {
	return checkUsingState(ctx, js, nil, scanBatchThrough, true)
}

// CheckThroughInvocationSequenceWithBatchedStateReads requires the same
// quiescent-cohort/no-purge/no-reuse contract as the point-read checker. Every
// call obtains a new state snapshot; no result survives between audits.
func CheckThroughInvocationSequenceWithBatchedStateReads(ctx context.Context, js jetstream.JetStream, cutoff uint64) (Report, error) {
	return checkUsingState(ctx, js, &cutoff, scanBatchThrough, true)
}

func checkUsingState(ctx context.Context, js jetstream.JetStream, cutoff *uint64, read retainedScanner, snapshotState bool) (Report, error) {
	return checkUsingOptions(ctx, js, cutoff, read, snapshotState, false)
}

// CheckWithStreamingReads validates every freshly decoded retained record while
// keeping protocol state per invocation instead of a complete decoded prefix.
// Compacted journals still use the original reconstruction and checker.
func CheckWithStreamingReads(ctx context.Context, js jetstream.JetStream) (Report, error) {
	return checkUsingOptions(ctx, js, nil, scanBatchThrough, false, true)
}

// CheckThroughInvocationSequenceWithStreamingReads keeps the original captured
// quiescent-cohort/no-purge/no-reuse contract and rereads all retained records.
func CheckThroughInvocationSequenceWithStreamingReads(ctx context.Context, js jetstream.JetStream, cutoff uint64) (Report, error) {
	return checkUsingOptions(ctx, js, &cutoff, scanBatchThrough, false, true)
}

// CheckWithStreamingStateReads combines the streaming journal accumulator with
// a fresh complete KV watch snapshot. The retained state must be quiescent.
func CheckWithStreamingStateReads(ctx context.Context, js jetstream.JetStream) (Report, error) {
	return checkUsingOptions(ctx, js, nil, scanBatchThrough, true, true)
}

// CheckThroughInvocationSequenceWithStreamingStateReads keeps the original
// quiescent-cohort/no-purge/no-reuse contract. Every call obtains fresh records
// and state; a final full audit remains necessary to cover orphan journals.
func CheckThroughInvocationSequenceWithStreamingStateReads(ctx context.Context, js jetstream.JetStream, cutoff uint64) (Report, error) {
	return checkUsingOptions(ctx, js, &cutoff, scanBatchThrough, true, true)
}

func checkUsingOptions(ctx context.Context, js jetstream.JetStream, cutoff *uint64, read retainedScanner, snapshotState, streaming bool) (Report, error) {
	var report Report
	inv, err := auditRead(ctx, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_INV") })
	if err != nil {
		return report, err
	}
	jrn, err := auditRead(ctx, func(attempt context.Context) (jetstream.Stream, error) { return js.Stream(attempt, "WF_JRN") })
	if err != nil {
		return report, err
	}
	state, err := auditRead(ctx, func(attempt context.Context) (jetstream.KeyValue, error) { return js.KeyValue(attempt, "WF_STATE") })
	if err != nil {
		return report, err
	}
	seen := map[string]struct{}{}
	if err := read(ctx, inv, cutoff, func(m *jetstream.RawStreamMsg) error {
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
	live := map[string][]journal.Record{}
	summaries := map[string]*journalAudit{}
	compacted := map[string]bool{}
	if err := read(ctx, jrn, nil, func(m *jetstream.RawStreamMsg) error {
		if cutoff != nil {
			if _, ok := seen[strings.Replace(m.Subject, "wf.jrn.", "wf.inv.", 1)]; !ok {
				return nil
			}
		}
		var e journal.Entry
		if err := journal.UnmarshalEntry(m.Data, &e); err != nil {
			return err
		}
		groups[m.Subject] = struct{}{}
		record := journal.Record{Entry: e, Sequence: m.Sequence}
		if streaming {
			if summaries[m.Subject] == nil {
				summaries[m.Subject] = &journalAudit{}
			}
			summaries[m.Subject].add(m.Subject, record)
		} else {
			live[m.Subject] = append(live[m.Subject], record)
		}
		return nil
	}); err != nil {
		return report, err
	}
	if snapshotState {
		state, err = auditRead(ctx, func(attempt context.Context) (jetstream.KeyValue, error) {
			return initialAuditState(attempt, state, func(key string) bool {
				if cutoff == nil {
					return true
				}
				_, ok := seen["wf.inv."+strings.TrimPrefix(key, "snap.")]
				return ok
			})
		})
		if err != nil {
			return report, err
		}
	}
	keys, err := auditRead(ctx, func(attempt context.Context) ([]string, error) { return state.Keys(attempt) })
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return report, err
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, "snap.") {
			continue
		}
		if cutoff != nil {
			if _, ok := seen["wf.inv."+strings.TrimPrefix(key, "snap.")]; !ok {
				continue
			}
		}
		parts := strings.Split(strings.TrimPrefix(key, "snap."), ".")
		if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
			return report, fmt.Errorf("invalid snapshot key %s", key)
		}
		subject := identity.JournalSubject(parts[0], parts[1])
		groups[subject] = struct{}{}
		compacted[subject] = true
	}
	subjects := make([]string, 0, len(groups))
	for subject := range groups {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	audited, err := auditJournals(ctx, subjects, func(ctx context.Context, subject string) (int, bool, error) {
		key, err := journalKey(subject, seen)
		if err != nil {
			return 0, false, err
		}
		parts := strings.Split(key, ".")
		records := live[subject]
		if compacted[subject] {
			records, _, err = journal.New(js).Read(ctx, parts[0], parts[1])
			if err != nil {
				return 0, false, fmt.Errorf("%s: %w", subject, err)
			}
		}
		terminalValue := func() ([]byte, error) {
			value, err := auditRead(ctx, func(attempt context.Context) (jetstream.KeyValueEntry, error) { return state.Get(attempt, key) })
			if err != nil {
				return nil, err
			}
			return value.Value(), nil
		}
		var entries int
		var terminal bool
		if streaming && !compacted[subject] {
			summary := summaries[subject]
			if summary == nil {
				return 0, false, fmt.Errorf("%s: missing streamed journal state", subject)
			}
			entries, terminal, err = summary.finish(subject, terminalValue)
		} else {
			entries, terminal, err = checkJournalRecords(subject, records, terminalValue)
		}
		if err != nil {
			return 0, false, err
		}
		return entries, terminal, nil
	})
	report.Journals, report.Entries, report.Terminal = audited.Journals, audited.Entries, audited.Terminal
	if err != nil {
		return report, err
	}

	return report, nil
}

// Check independent retained journals in bounded windows, then reduce in
// sorted subject order. Read completion order cannot hide an earlier invariant
// failure or change its reported subject. Maps captured by check are immutable.
func auditJournals(ctx context.Context, subjects []string, check func(context.Context, string) (int, bool, error)) (Report, error) {
	var report Report
	const window = 16
	type result struct {
		entries  int
		terminal bool
		err      error
	}
	for first := 0; first < len(subjects); first += window {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		count := min(window, len(subjects)-first)
		results := make([]result, count)
		var readers sync.WaitGroup
		for offset := range results {
			readers.Add(1)
			go func() {
				defer readers.Done()
				results[offset].entries, results[offset].terminal, results[offset].err = check(ctx, subjects[first+offset])
			}()
		}
		readers.Wait()
		for _, result := range results {
			report.Journals++
			if result.err != nil {
				return report, result.err
			}
			report.Entries += result.entries
			if result.terminal {
				report.Terminal++
			}
		}
	}
	return report, nil
}

// Retry a lost read at its current position rather than restarting a full
// retained-state scan. Semantic and invariant errors are never retried.
func auditRead[T any](ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	var value T
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return value, ctx.Err()
		}
		call, stop := context.WithTimeout(ctx, 2*time.Second)
		value, err = read(call)
		stop()
		if err == nil {
			return value, nil
		}
		transient := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse) || natsutil.IsUnavailable(err)
		if !transient || ctx.Err() != nil {
			break
		}
	}
	return value, err
}
