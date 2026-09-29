package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"plugin"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

type replayReport struct {
	Type           string          `json:"type"`
	ID             string          `json:"id"`
	InvSeq         uint64          `json:"inv_seq"`
	JournalEntries int             `json:"journal_entries"`
	Status         string          `json:"status"`
	Result         json.RawMessage `json:"result,omitempty"`
	Error          string          `json:"error,omitempty"`
	WaitingOn      string          `json:"waiting_on,omitempty"`
}

// replayBundle contains the durable inputs needed to replay a handler offline.
// []byte fields are base64-encoded by JSON.
type replayBundle struct {
	Type      string            `json:"type"`
	ID        string            `json:"id"`
	InvSeq    uint64            `json:"inv_seq"`
	Input     []byte            `json:"input"`
	InputHash string            `json:"input_hash"`
	Journal   []journal.Record  `json:"journal"`
	Objects   map[string][]byte `json:"objects"`
}

func replayInvocation(ctx context.Context, js jetstream.JetStream, typ, id, pluginPath, symbolName string) (replayReport, error) {
	bundle, err := fetchReplayBundle(ctx, js, typ, id)
	if err != nil {
		return replayReport{}, err
	}
	return runReplayBundle(bundle, pluginPath, symbolName)
}

func loadReplayBundle(path string) (replayBundle, error) {
	var bundle replayBundle
	file, err := os.Open(path)
	if err != nil {
		return bundle, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return replayBundle{}, fmt.Errorf("decode replay bundle: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return replayBundle{}, fmt.Errorf("trailing replay bundle data")
		}
		return replayBundle{}, fmt.Errorf("trailing replay bundle data: %w", err)
	}
	return bundle, nil
}

func fetchReplayBundle(ctx context.Context, js jetstream.JetStream, typ, id string) (replayBundle, error) {
	var bundle replayBundle
	if err := identity.Validate(typ, id); err != nil {
		return bundle, err
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return bundle, err
	}
	input, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil {
		return bundle, fmt.Errorf("read invocation: %w", err)
	}
	bundle = replayBundle{Type: typ, ID: id, InvSeq: input.Sequence, InputHash: input.Header.Get("Wf-Input-SHA256"), Objects: map[string][]byte{}}
	var objectStore jetstream.ObjectStore
	loadObject := func(name string) ([]byte, error) {
		if name == "" {
			return nil, fmt.Errorf("empty replay object name")
		}
		if objectStore == nil {
			var err error
			objectStore, err = js.ObjectStore(ctx, "WF_BLOB")
			if err != nil {
				return nil, err
			}
		}
		data, err := objectStore.GetBytes(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("load replay object %q: %w", name, err)
		}
		return data, nil
	}
	bundle.Input = input.Data
	if ref := input.Header.Get("Wf-Input-Ref"); ref != "" {
		if len(bundle.Input) != 0 {
			return replayBundle{}, fmt.Errorf("invocation has both inline input and object reference")
		}
		bundle.Input, err = loadObject(ref)
		if err != nil {
			return replayBundle{}, err
		}
	}
	if err := checkReplayInput(bundle); err != nil {
		return replayBundle{}, err
	}
	bundle.Journal, _, err = journal.New(js).Read(ctx, typ, id)
	if err != nil {
		return replayBundle{}, fmt.Errorf("read journal: %w", err)
	}
	for _, record := range bundle.Journal {
		if record.Kind != journal.StepCompleted && record.Kind != journal.SignalConsumed && record.Kind != journal.Completed {
			continue
		}
		var refs struct {
			Ref       string `json:"ref"`
			ResultRef string `json:"result_ref"`
		}
		if err := json.Unmarshal(record.Payload, &refs); err != nil {
			return replayBundle{}, fmt.Errorf("decode object references at journal index %d: %w", record.Index, err)
		}
		for _, ref := range []string{refs.Ref, refs.ResultRef} {
			if ref == "" {
				continue
			}
			if _, exists := bundle.Objects[ref]; !exists {
				bundle.Objects[ref], err = loadObject(ref)
				if err != nil {
					return replayBundle{}, err
				}
			}
		}
	}
	return bundle, nil
}

func checkReplayInput(bundle replayBundle) error {
	if err := identity.Validate(bundle.Type, bundle.ID); err != nil {
		return err
	}
	if bundle.InvSeq == 0 {
		return fmt.Errorf("replay bundle has no invocation sequence")
	}
	digest := sha256.Sum256(bundle.Input)
	if bundle.InputHash == "" || hex.EncodeToString(digest[:]) != bundle.InputHash {
		return fmt.Errorf("invocation input hash mismatch")
	}
	return nil
}

func runReplayBundle(bundle replayBundle, pluginPath, symbolName string) (replayReport, error) {
	var report replayReport
	if err := checkReplayInput(bundle); err != nil {
		return report, err
	}
	if len(bundle.Journal) == 0 {
		return report, fmt.Errorf("replay journal is empty")
	}
	tail := bundle.Journal[len(bundle.Journal)-1]
	if tail.Kind != journal.Completed && tail.Kind != journal.Failed && tail.Kind != journal.Suspended {
		return report, fmt.Errorf("replay requires a completed, failed or suspended journal tail")
	}
	loaded, err := plugin.Open(pluginPath)
	if err != nil {
		return report, fmt.Errorf("open replay handler: %w", err)
	}
	symbol, err := loaded.Lookup(symbolName)
	if err != nil {
		return report, fmt.Errorf("load replay handler %q: %w", symbolName, err)
	}
	handler, ok := symbol.(func(*wf.Context, json.RawMessage) (json.RawMessage, error))
	if !ok {
		return report, fmt.Errorf("replay handler %q must have signature func(*wf.Context, json.RawMessage) (json.RawMessage, error)", symbolName)
	}
	if tail.Kind == journal.Failed {
		var outcome wf.Outcome
		if err := json.Unmarshal(tail.Payload, &outcome); err != nil {
			return report, fmt.Errorf("decode failed outcome: %w", err)
		}
		if len(bundle.Journal) > 1 && bundle.Journal[len(bundle.Journal)-2].Kind == journal.Attempt {
			attempt, err := journal.DecodeAttempt(bundle.Journal[len(bundle.Journal)-2].Payload)
			if err != nil {
				return report, fmt.Errorf("decode final panic attempt: %w", err)
			}
			if attempt.Error != outcome.Error {
				return report, fmt.Errorf("final panic attempt differs from terminal failure: attempt=%q terminal=%q", attempt.Error, outcome.Error)
			}
		}
		if outcome.Error == client.ErrCancelled.Error() {
			return replayCancellation(bundle, handler)
		}
		if len(outcome.LimitRequest) != 0 {
			return replayJournalLimit(bundle, handler, outcome)
		}
		if outcome.LimitEntry != nil {
			return replayNonStepJournalLimit(bundle, handler, outcome)
		}
		if outcome.Error == journal.ErrTooLong.Error() {
			return report, fmt.Errorf("journal-limit failure lacks attempted entry metadata; exact offline verification is unavailable")
		}
	}
	journalBytes, err := json.Marshal(bundle.Journal)
	if err != nil {
		return report, err
	}
	var observed wf.ReplayObservation
	result, replayErr := wf.Replay(journalBytes, func(c *wf.Context) (json.RawMessage, error) {
		return handler(c, bundle.Input)
	}, wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects, Observation: &observed})
	if observed.PlayedSteps != observed.RecordedSteps {
		if errors.Is(replayErr, wf.ErrNonDeterministic) {
			return report, fmt.Errorf("replay %s.%s: %w", bundle.Type, bundle.ID, replayErr)
		}
		return report, fmt.Errorf("%w: replay consumed %d/%d recorded steps", wf.ErrNonDeterministic, observed.PlayedSteps, observed.RecordedSteps)
	}
	report = replayReport{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, JournalEntries: len(bundle.Journal)}
	if tail.Kind == journal.Suspended {
		var suspended struct {
			WaitingOn string `json:"waiting_on"`
		}
		if err := json.Unmarshal(tail.Payload, &suspended); err != nil || suspended.WaitingOn == "" {
			return replayReport{}, fmt.Errorf("invalid suspended journal tail: %v", err)
		}
		if !errors.Is(replayErr, wf.ErrSuspended) || observed.WaitingOn != suspended.WaitingOn {
			return replayReport{}, fmt.Errorf("replayed wait differs from suspended journal: replay=%q error=%v recorded=%q", observed.WaitingOn, replayErr, suspended.WaitingOn)
		}
		report.Status, report.WaitingOn = "suspended", suspended.WaitingOn
		return report, nil
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(tail.Payload, &outcome); err != nil {
		return report, fmt.Errorf("decode terminal outcome for %s.%s: %w", bundle.Type, bundle.ID, err)
	}
	if outcome.InvSeq != bundle.InvSeq {
		return report, fmt.Errorf("terminal outcome has a different invocation generation")
	}
	if tail.Kind == journal.Failed {
		if replayErr == nil || errors.Is(replayErr, wf.ErrNonDeterministic) || errors.Is(replayErr, wf.ErrCorruptJournal) || errors.Is(replayErr, wf.ErrReplayObjectMissing) {
			if replayErr != nil {
				return replayReport{}, fmt.Errorf("replay %s.%s did not reproduce terminal failure: %w", bundle.Type, bundle.ID, replayErr)
			}
			return replayReport{}, fmt.Errorf("replay %s.%s did not reproduce terminal failure: %v", bundle.Type, bundle.ID, replayErr)
		}
		if outcome.Error == "" || replayErr.Error() != outcome.Error {
			return replayReport{}, fmt.Errorf("replayed error differs from terminal outcome: replay=%q recorded=%q", replayErr, outcome.Error)
		}
		report.Status, report.Error = "failed", outcome.Error
		return report, nil
	}
	if replayErr != nil {
		return replayReport{}, fmt.Errorf("replay %s.%s: %w", bundle.Type, bundle.ID, replayErr)
	}
	if outcome.Error != "" {
		return report, fmt.Errorf("completed journal has a terminal error")
	}
	committed, err := outcome.ResultBytes(context.Background(), func(_ context.Context, name string) ([]byte, error) {
		data, ok := bundle.Objects[name]
		if !ok {
			return nil, fmt.Errorf("missing terminal result object %q", name)
		}
		return data, nil
	})
	if err != nil {
		return report, err
	}
	if !bytes.Equal(result, committed) {
		return report, fmt.Errorf("replayed result differs from terminal outcome for %s.%s", bundle.Type, bundle.ID)
	}
	report.Status, report.Result = "completed", result
	return report, nil
}

// The worker retains the attempted request in Failed when the journal has
// room for a terminal record but not another step. Substitute that request
// for Failed during offline replay: the handler must reach the same request,
// then stop at the pending effect without executing it.
func replayJournalLimit(bundle replayBundle, handler func(*wf.Context, json.RawMessage) (json.RawMessage, error), outcome wf.Outcome) (replayReport, error) {
	if outcome.Error != journal.ErrTooLong.Error() || outcome.InvSeq != bundle.InvSeq || len(bundle.Journal) < 2 || !json.Valid(outcome.LimitRequest) {
		return replayReport{}, fmt.Errorf("invalid journal-limit failure")
	}
	records := append([]journal.Record(nil), bundle.Journal...)
	records[len(records)-1].Kind = journal.StepRequested
	records[len(records)-1].Payload = outcome.LimitRequest
	journalBytes, err := json.Marshal(records)
	if err != nil {
		return replayReport{}, err
	}
	var observed wf.ReplayObservation
	_, replayErr := wf.Replay(journalBytes, func(c *wf.Context) (json.RawMessage, error) {
		return handler(c, bundle.Input)
	}, wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects, Observation: &observed})
	if !errors.Is(replayErr, wf.ErrReplayPendingStep) || observed.PlayedSteps != observed.RecordedSteps {
		return replayReport{}, fmt.Errorf("handler replay differs from journal-limit request: error=%v steps=%d/%d", replayErr, observed.PlayedSteps, observed.RecordedSteps)
	}
	return replayReport{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, JournalEntries: len(bundle.Journal), Status: "failed", Error: outcome.Error}, nil
}

func replayNonStepJournalLimit(bundle replayBundle, handler func(*wf.Context, json.RawMessage) (json.RawMessage, error), outcome wf.Outcome) (replayReport, error) {
	if outcome.Error != journal.ErrTooLong.Error() || outcome.InvSeq != bundle.InvSeq || len(outcome.LimitRequest) != 0 || outcome.LimitEntry == nil || !json.Valid(outcome.LimitEntry.Payload) {
		return replayReport{}, fmt.Errorf("invalid non-step journal-limit failure")
	}
	full, err := json.Marshal(bundle.Journal)
	if err != nil {
		return replayReport{}, err
	}
	validationStop := errors.New("journal validated")
	_, err = wf.Replay(full, func(*wf.Context) (json.RawMessage, error) { return nil, validationStop }, wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects})
	if !errors.Is(err, validationStop) {
		return replayReport{}, fmt.Errorf("invalid journal-limit history: %w", err)
	}
	var observed wf.ReplayObservation
	_, replayErr := wf.Replay(full, func(c *wf.Context) (json.RawMessage, error) {
		return handler(c, bundle.Input)
	}, wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects, Observation: &observed})
	if observed.PlayedSteps != observed.RecordedSteps {
		return replayReport{}, fmt.Errorf("handler replay differs from journal-limit history: error=%v steps=%d/%d", replayErr, observed.PlayedSteps, observed.RecordedSteps)
	}
	switch journal.Kind(outcome.LimitEntry.Kind) {
	case journal.Suspended:
		var suspended struct {
			WaitingOn string `json:"waiting_on"`
		}
		if json.Unmarshal(outcome.LimitEntry.Payload, &suspended) != nil || suspended.WaitingOn == "" || !errors.Is(replayErr, wf.ErrSuspended) || observed.WaitingOn != suspended.WaitingOn {
			return replayReport{}, fmt.Errorf("handler replay differs from journal-limit suspension: error=%v wait=%q", replayErr, observed.WaitingOn)
		}
	case journal.Attempt:
		attempt, err := journal.DecodeAttempt(outcome.LimitEntry.Payload)
		if err != nil || !observed.Panicked || replayErr == nil || replayErr.Error() != attempt.Error {
			return replayReport{}, fmt.Errorf("handler replay differs from journal-limit panic: error=%v", replayErr)
		}
		count := 0
		for _, record := range bundle.Journal {
			if record.Kind == journal.Attempt {
				count++
			}
		}
		if attempt.Count != count+1 {
			return replayReport{}, fmt.Errorf("journal-limit panic attempt count=%d, want %d", attempt.Count, count+1)
		}
	default:
		return replayReport{}, fmt.Errorf("journal-limit entry kind %q cannot be verified offline", outcome.LimitEntry.Kind)
	}
	return replayReport{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, JournalEntries: len(bundle.Journal), Status: "failed", Error: outcome.Error}, nil
}

// replayCancellation checks the last handler suspension, then the worker's
// cancellation decision. The worker handles the reserved signal before it
// calls the handler again, so there is no final handler result to reproduce.
func replayCancellation(bundle replayBundle, handler func(*wf.Context, json.RawMessage) (json.RawMessage, error)) (replayReport, error) {
	records := bundle.Journal
	tail := records[len(records)-1]
	var outcome wf.Outcome
	if err := json.Unmarshal(tail.Payload, &outcome); err != nil || outcome.InvSeq != bundle.InvSeq || outcome.Error != client.ErrCancelled.Error() {
		return replayReport{}, fmt.Errorf("invalid cancellation outcome")
	}
	full, err := json.Marshal(records)
	if err != nil {
		return replayReport{}, err
	}
	validationStop := errors.New("cancellation journal validated")
	_, err = wf.Replay(full, func(*wf.Context) (json.RawMessage, error) { return nil, validationStop }, wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects})
	if !errors.Is(err, validationStop) {
		return replayReport{}, fmt.Errorf("invalid cancellation journal: %w", err)
	}
	// Replay the handler state before the final signal drain. A pending Run
	// request stops replay before its effect can execute.
	firstTrailingSignal := len(records) - 1
	for firstTrailingSignal > 1 && records[firstTrailingSignal-1].Kind == journal.SignalConsumed {
		firstTrailingSignal--
	}
	foundCancel := false
	for _, record := range records[firstTrailingSignal : len(records)-1] {
		var signal struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(record.Payload, &signal); err != nil {
			return replayReport{}, err
		}
		if signal.Name == client.CancelSignalName {
			foundCancel = true
		}
	}
	if !foundCancel {
		return replayReport{}, fmt.Errorf("cancellation failure has no consumed cancellation signal")
	}
	if firstTrailingSignal > 1 {
		prefix, err := json.Marshal(records[:firstTrailingSignal])
		if err != nil {
			return replayReport{}, err
		}
		var observed wf.ReplayObservation
		_, err = wf.Replay(prefix, func(c *wf.Context) (json.RawMessage, error) { return handler(c, bundle.Input) }, wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects, Observation: &observed})
		last := records[firstTrailingSignal-1]
		if observed.PlayedSteps != observed.RecordedSteps || errors.Is(err, wf.ErrNonDeterministic) || errors.Is(err, wf.ErrCorruptJournal) || errors.Is(err, wf.ErrReplayObjectMissing) {
			if last.Kind == journal.Suspended {
				return replayReport{}, fmt.Errorf("handler replay differs from prior suspension: error=%v steps=%d/%d", err, observed.PlayedSteps, observed.RecordedSteps)
			}
			return replayReport{}, fmt.Errorf("handler replay differs from pre-cancellation journal: error=%v steps=%d/%d", err, observed.PlayedSteps, observed.RecordedSteps)
		}
		switch last.Kind {
		case journal.StepRequested:
			if !errors.Is(err, wf.ErrReplayPendingStep) {
				return replayReport{}, fmt.Errorf("handler replay differs from canceled pending step: %v", err)
			}
		case journal.Suspended:
			var suspended struct {
				WaitingOn string `json:"waiting_on"`
			}
			if json.Unmarshal(last.Payload, &suspended) != nil || suspended.WaitingOn == "" || !errors.Is(err, wf.ErrSuspended) || observed.WaitingOn != suspended.WaitingOn {
				return replayReport{}, fmt.Errorf("handler replay differs from prior suspension: wait=%q error=%v recorded=%q", observed.WaitingOn, err, suspended.WaitingOn)
			}
		default:
			if errors.Is(err, wf.ErrReplayPendingStep) || errors.Is(err, wf.ErrSuspended) {
				return replayReport{}, fmt.Errorf("handler replay differs from pre-cancellation tail: %v", err)
			}
		}
	}
	return replayReport{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, JournalEntries: len(records), Status: "failed", Error: outcome.Error}, nil
}
