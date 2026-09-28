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
	if tail.Kind == journal.Failed {
		var outcome wf.Outcome
		if json.Unmarshal(tail.Payload, &outcome) == nil && outcome.Error == client.ErrCancelled.Error() {
			for _, record := range bundle.Journal {
				if record.Kind != journal.SignalConsumed {
					continue
				}
				var signal struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(record.Payload, &signal) == nil && signal.Name == client.CancelSignalName {
					return report, fmt.Errorf("replay cannot verify cancellation: the worker committed failure before calling the handler")
				}
			}
		}
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
