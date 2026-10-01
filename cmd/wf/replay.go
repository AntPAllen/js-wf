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
	"strconv"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
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
	Type          string               `json:"type"`
	ID            string               `json:"id"`
	InvSeq        uint64               `json:"inv_seq"`
	Input         []byte               `json:"input"`
	InputHash     string               `json:"input_hash"`
	Journal       []journal.Record     `json:"journal"`
	Objects       map[string][]byte    `json:"objects"`
	PendingSignal *replayPendingSignal `json:"pending_signal,omitempty"`
}

type replayPendingSignal struct {
	Sequence uint64      `json:"sequence"`
	Subject  string      `json:"subject"`
	Header   nats.Header `json:"header"`
	Data     []byte      `json:"data"`
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
	if len(bundle.Journal) > 0 && bundle.Journal[len(bundle.Journal)-1].Kind == journal.Failed {
		tail := bundle.Journal[len(bundle.Journal)-1]
		var outcome wf.Outcome
		if err := json.Unmarshal(tail.Payload, &outcome); err != nil {
			return replayBundle{}, fmt.Errorf("decode failed outcome: %w", err)
		}
		if outcome.LimitEntry != nil && outcome.LimitEntry.Kind == string(journal.SignalConsumed) {
			var attempted struct {
				Sequence uint64 `json:"sig_seq"`
			}
			if err := json.Unmarshal(outcome.LimitEntry.Payload, &attempted); err != nil || attempted.Sequence == 0 {
				return replayBundle{}, fmt.Errorf("invalid rejected signal metadata: %v", err)
			}
			signals, err := js.Stream(ctx, "WF_SIG")
			if err != nil {
				return replayBundle{}, err
			}
			message, err := signals.GetMsg(ctx, attempted.Sequence)
			if err != nil {
				return replayBundle{}, fmt.Errorf("read rejected signal source: %w", err)
			}
			bundle.PendingSignal = &replayPendingSignal{Sequence: message.Sequence, Subject: message.Subject, Header: message.Header, Data: message.Data}
			if ref := message.Header.Get("Wf-Signal-Ref"); ref != "" {
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

// replayWorkflow uses the same definition as the live worker plugin.
type replayWorkflow struct {
	initial worker.Handler
	stages  map[string]worker.ContinuationHandler
}

func (h replayWorkflow) replay(raw []byte, bundle replayBundle, observation *wf.ReplayObservation) (json.RawMessage, error) {
	opts := wf.ReplayOptions{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, Objects: bundle.Objects, Observation: observation}
	initial := func(c *wf.Context) (json.RawMessage, error) { return h.initial(c, bundle.Input) }
	if len(h.stages) == 0 {
		return wf.Replay(raw, initial, opts)
	}
	stages := make(map[string]wf.ReplayContinuation[json.RawMessage], len(h.stages))
	for name, stage := range h.stages {
		stages[name] = func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
			return stage(c, bundle.Input, locals)
		}
	}
	return wf.ReplayWithContinuations(raw, initial, stages, opts)
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
	var handler replayWorkflow
	switch value := symbol.(type) {
	case func(*wf.Context, json.RawMessage) (json.RawMessage, error):
		handler.initial = value
	case *worker.WorkflowDefinition:
		handler.initial, handler.stages = value.Handler, value.Continuations
	case func() worker.WorkflowDefinition:
		definition := value()
		handler.initial, handler.stages = definition.Handler, definition.Continuations
	default:
		return report, fmt.Errorf("replay handler %q must be a workflow function or worker.WorkflowDefinition", symbolName)
	}
	if err := (worker.WorkflowDefinition{Handler: handler.initial, Continuations: handler.stages}).Validate(); err != nil {
		return report, fmt.Errorf("replay handler %q: %w", symbolName, err)
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
	result, replayErr := handler.replay(journalBytes, bundle, &observed)
	if observed.PlayedSteps != observed.RecordedSteps {
		if errors.Is(replayErr, wf.ErrNonDeterministic) || errors.Is(replayErr, wf.ErrCorruptJournal) || errors.Is(replayErr, wf.ErrReplayObjectMissing) || errors.Is(replayErr, wf.ErrUnknownContinuation) {
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
		replayedError := replayErr.Error()
		if observed.Panicked {
			replayedError = journal.AttemptError(replayedError)
		}
		if outcome.Error == "" || replayedError != outcome.Error {
			return replayReport{}, fmt.Errorf("replayed error differs from terminal outcome: replay=%q recorded=%q", replayedError, outcome.Error)
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
func replayJournalLimit(bundle replayBundle, handler replayWorkflow, outcome wf.Outcome) (replayReport, error) {
	if outcome.Error != journal.ErrTooLong.Error() || outcome.InvSeq != bundle.InvSeq || len(bundle.Journal) < 2 || !json.Valid(outcome.LimitRequest) {
		return replayReport{}, fmt.Errorf("invalid journal-limit failure")
	}
	journalBytes, err := json.Marshal(bundle.Journal)
	if err != nil {
		return replayReport{}, err
	}
	var observed wf.ReplayObservation
	_, replayErr := handler.replay(journalBytes, bundle, &observed)
	if !errors.Is(replayErr, wf.ErrReplayPendingStep) || observed.PlayedSteps != observed.RecordedSteps {
		return replayReport{}, fmt.Errorf("handler replay differs from journal-limit request: error=%v steps=%d/%d", replayErr, observed.PlayedSteps, observed.RecordedSteps)
	}
	return replayReport{Type: bundle.Type, ID: bundle.ID, InvSeq: bundle.InvSeq, JournalEntries: len(bundle.Journal), Status: "failed", Error: outcome.Error}, nil
}

func replayNonStepJournalLimit(bundle replayBundle, handler replayWorkflow, outcome wf.Outcome) (replayReport, error) {
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
	_, replayErr := handler.replay(full, bundle, &observed)
	if observed.PlayedSteps != observed.RecordedSteps {
		return replayReport{}, fmt.Errorf("handler replay differs from journal-limit history: error=%v steps=%d/%d", replayErr, observed.PlayedSteps, observed.RecordedSteps)
	}
	switch journal.Kind(outcome.LimitEntry.Kind) {
	case journal.SignalConsumed:
		if err := replayRejectedSignal(bundle, outcome.LimitEntry.Payload, replayErr, observed); err != nil {
			return replayReport{}, err
		}
	case journal.Suspended:
		var suspended struct {
			WaitingOn string `json:"waiting_on"`
		}
		if json.Unmarshal(outcome.LimitEntry.Payload, &suspended) != nil || suspended.WaitingOn == "" || !errors.Is(replayErr, wf.ErrSuspended) || observed.WaitingOn != suspended.WaitingOn {
			return replayReport{}, fmt.Errorf("handler replay differs from journal-limit suspension: error=%v wait=%q", replayErr, observed.WaitingOn)
		}
	case journal.Attempt:
		attempt, err := journal.DecodeAttempt(outcome.LimitEntry.Payload)
		if err != nil || !observed.Panicked || replayErr == nil || journal.AttemptError(replayErr.Error()) != attempt.Error {
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

func replayRejectedSignal(bundle replayBundle, raw json.RawMessage, replayErr error, observed wf.ReplayObservation) error {
	source := bundle.PendingSignal
	if source == nil {
		return fmt.Errorf("rejected signal source is missing from replay bundle")
	}
	var attempted struct {
		Sequence uint64 `json:"sig_seq"`
		Name     string `json:"name"`
		Payload  []byte `json:"payload"`
		Ref      string `json:"ref"`
		Hash     string `json:"hash"`
	}
	if err := json.Unmarshal(raw, &attempted); err != nil || attempted.Sequence == 0 || identity.ValidateToken(attempted.Name) != nil {
		return fmt.Errorf("invalid rejected signal entry: %v", err)
	}
	if source.Sequence != attempted.Sequence || source.Subject != "wf.sig."+bundle.Type+"."+bundle.ID+"."+attempted.Name {
		return fmt.Errorf("rejected signal source differs from attempted drain")
	}
	if generation := source.Header.Get("Wf-Inv-Seq"); generation != "" {
		seq, err := strconv.ParseUint(generation, 10, 64)
		if err != nil || seq != bundle.InvSeq {
			return fmt.Errorf("rejected signal source has a different invocation generation")
		}
	}
	ref := source.Header.Get("Wf-Signal-Ref")
	hash := source.Header.Get("Wf-Input-SHA256")
	payload := source.Data
	if ref != "" {
		if len(payload) != 0 {
			return fmt.Errorf("rejected signal source has both inline data and an object reference")
		}
		var ok bool
		payload, ok = bundle.Objects[ref]
		if !ok {
			return fmt.Errorf("rejected signal object %q is missing from replay bundle", ref)
		}
	}
	if attempted.Ref != ref || attempted.Hash != hash || ref == "" && !bytes.Equal(attempted.Payload, payload) || ref != "" && len(attempted.Payload) != 0 {
		return fmt.Errorf("rejected signal payload differs from retained source")
	}
	if hash != "" {
		digest := sha256.Sum256(payload)
		if hex.EncodeToString(digest[:]) != hash {
			return fmt.Errorf("rejected signal payload hash mismatch")
		}
	}
	for _, record := range bundle.Journal[:len(bundle.Journal)-1] {
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var previous struct {
			Sequence uint64 `json:"sig_seq"`
		}
		if json.Unmarshal(record.Payload, &previous) != nil || previous.Sequence >= attempted.Sequence {
			return fmt.Errorf("rejected signal is not newer than consumed signals")
		}
	}
	if errors.Is(replayErr, wf.ErrCorruptJournal) || errors.Is(replayErr, wf.ErrReplayObjectMissing) {
		return fmt.Errorf("handler replay differs before rejected signal: %w", replayErr)
	}
	prior := bundle.Journal[len(bundle.Journal)-2]
	switch prior.Kind {
	case journal.StepRequested:
		if !errors.Is(replayErr, wf.ErrReplayPendingStep) {
			return fmt.Errorf("handler replay differs from prior pending step: %v", replayErr)
		}
	case journal.Suspended:
		var suspended struct {
			WaitingOn string `json:"waiting_on"`
		}
		if json.Unmarshal(prior.Payload, &suspended) != nil || suspended.WaitingOn == "" || !errors.Is(replayErr, wf.ErrSuspended) || observed.WaitingOn != suspended.WaitingOn {
			return fmt.Errorf("handler replay differs from prior suspension: error=%v wait=%q", replayErr, observed.WaitingOn)
		}
	}
	return nil
}

// replayCancellation checks the last handler suspension, then the worker's
// cancellation decision. The worker handles the reserved signal before it
// calls the handler again, so there is no final handler result to reproduce.
func replayCancellation(bundle replayBundle, handler replayWorkflow) (replayReport, error) {
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
		_, err = handler.replay(prefix, bundle, &observed)
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
