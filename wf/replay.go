package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/internal/journalwire"
	"js-wf/journal"
)

var ErrReplayObjectMissing = errors.New("offline replay object is missing")
var ErrReplayPendingStep = errors.New("offline replay reached an incomplete step")

type ReplayOptions struct {
	// Format declares the export contract; graph-v1 requires canonical markers.
	Format string
	// InputHash binds the supplied invocation input to its Started record.
	InputHash   string
	Type        string
	ID          string
	InvSeq      uint64
	Objects     map[string][]byte
	Observation *ReplayObservation
}

// ReplayObservation describes the handler state reached while replaying
// recorded steps. A caller can compare it with a Suspended or Failed tail.
type ReplayObservation struct {
	WaitingOn     string
	PlayedSteps   int
	RecordedSteps int
	Stage         string
	Continuations int
	Panicked      bool
}

// Replay runs a workflow against serialized []journal.Record without NATS.
// Referenced step and signal objects must be supplied in ReplayOptions.Objects.
// It checks journal ordering and that the function consumed every step. A Failed
// journal-limit tail with LimitRequest also audits the rejected declaration as
// a pending SDK step; supply invocation identity for this terminal audit.
func Replay[T any](journalBytes []byte, fn func(*Context) (T, error), options ...ReplayOptions) (T, error) {
	return replayWithStages(journalBytes, fn, nil, options...)
}

// ReplayContinuation receives the locals saved by the preceding boundary.
// Capture the original invocation input in the closure, as with Replay's initial
// handler. Replay never invokes recorded effect callbacks or publishes writes.
type ReplayContinuation[T any] func(*Context, json.RawMessage) (T, error)

// ReplayWithContinuations audits the complete logical journal from its initial
// handler through named stages. Every boundary rebuilds and verifies its stored
// frame before restoration and verifies a Completed return value. Terminal
// identity is checked before user code. A Failed tail with an unfinished effect
// still returns ErrReplayPendingStep without executing its callback. Supply
// invocation identity and all referenced objects; unknown stage registrations
// fail before running user code.
func ReplayWithContinuations[T any](journalBytes []byte, initial func(*Context) (T, error), stages map[string]ReplayContinuation[T], options ...ReplayOptions) (T, error) {
	registered := make(map[string]ReplayContinuation[T], len(stages))
	for stage, handler := range stages {
		if identity.ValidateToken(stage) != nil || handler == nil {
			var zero T
			return zero, fmt.Errorf("invalid replay continuation %q", stage)
		}
		registered[stage] = handler
	}
	return replayWithStages(journalBytes, initial, registered, options...)
}

func replayWithStages[T any](journalBytes []byte, fn func(*Context) (T, error), stages map[string]ReplayContinuation[T], options ...ReplayOptions) (result T, err error) {
	if len(options) > 1 {
		return result, fmt.Errorf("at most one replay options value is allowed")
	}
	var opts ReplayOptions
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.Type != "" || opts.ID != "" {
		if err := identity.Validate(opts.Type, opts.ID); err != nil {
			return result, err
		}
		if opts.InvSeq == 0 {
			return result, ErrInvocationIdentity
		}
	} else if opts.InvSeq != 0 {
		return result, ErrInvocationIdentity
	}
	if stages != nil && opts.Type == "" {
		return result, ErrInvocationIdentity
	}
	records, decodeErr := journalwire.Decode(journalBytes)
	if decodeErr != nil {
		return result, fmt.Errorf("%w: decode journal: %v", ErrCorruptJournal, decodeErr)
	}
	if err := validateReplayRecordOrder(records, opts.InvSeq); err != nil {
		return result, err
	}
	if stages != nil {
		subject := identity.JournalSubject(opts.Type, opts.ID)
		snapshot := integrity.Snapshot{Invocations: []string{identity.InvocationSubject(opts.Type, opts.ID)}, Journals: map[string][]journal.Record{subject: records}, TerminalState: map[string][]byte{}}
		last := records[len(records)-1]
		if last.Kind == journal.Completed || last.Kind == journal.Failed {
			snapshot.TerminalState[identity.Key(opts.Type, opts.ID)] = last.Payload
		}
		if _, checkErr := integrity.CheckSnapshot(snapshot); checkErr != nil {
			return result, fmt.Errorf("%w: %v", ErrCorruptJournal, checkErr)
		}
	}
	if err := validateReplayGraphFormat(records, opts.Objects, opts.Format); err != nil {
		return result, err
	}
	if err := validateReplayInputBinding(records, opts.Format, opts.InputHash); err != nil {
		return result, err
	}
	if err := ValidateReplayGraphCheckpoints(records, opts.Objects, opts.Type, opts.ID, opts.InvSeq); err != nil {
		return result, err
	}
	childValidator, err := replayGraphChildValidator(records, opts.Objects, opts.Format == ReplayFormatGraphV1)
	if err != nil {
		return result, err
	}
	anchors := make([]ContinuationAnchor, len(records))
	var entries []Entry
	var signals []Signal
	var lastSignal uint64
	var lastAttempt int
	loadObject := func(_ context.Context, name string) ([]byte, error) {
		data, ok := opts.Objects[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrReplayObjectMissing, name)
		}
		return data, nil
	}
	for i, record := range records {

		switch record.Kind {
		case journal.Started, journal.Suspended, journal.Completed, journal.Failed:
		case journal.Attempt:
			attempt, err := journal.DecodeAttempt(record.Payload)
			if err != nil || attempt.Count != lastAttempt+1 {
				return result, ErrCorruptJournal
			}
			lastAttempt = attempt.Count
		case journal.StepRequested, journal.StepCompleted:
			if stages != nil && record.Kind == journal.StepRequested {
				var declaration request
				if json.Unmarshal(record.Payload, &declaration) != nil {
					return result, ErrCorruptJournal
				}
				if declaration.Kind == "checkpoint" && stages[declaration.Name] == nil {
					return result, fmt.Errorf("%w: %s", ErrUnknownContinuation, declaration.Name)
				}
			}
			entries = append(entries, Entry{Index: record.Index, Kind: Kind(record.Kind), Payload: record.Payload})
		case journal.SignalConsumed:
			var event struct {
				Sequence uint64 `json:"sig_seq"`
				Name     string `json:"name"`
				Payload  []byte `json:"payload"`
				Ref      string `json:"ref"`
				Hash     string `json:"hash"`
			}
			if json.Unmarshal(record.Payload, &event) != nil || event.Sequence <= lastSignal || identity.ValidateToken(event.Name) != nil {
				return result, ErrCorruptJournal
			}
			lastSignal = event.Sequence
			if event.Ref != "" {
				if len(event.Payload) != 0 || event.Hash == "" {
					return result, ErrCorruptJournal
				}
				data, err := loadObject(context.Background(), event.Ref)
				if err != nil {
					return result, err
				}
				event.Payload = data
			}
			if event.Hash != "" {
				digest := sha256.Sum256(event.Payload)
				if hex.EncodeToString(digest[:]) != event.Hash {
					return result, ErrCorruptJournal
				}
			}
			signals = append(signals, Signal{Sequence: event.Sequence, Name: event.Name, Payload: event.Payload})
		default:
			return result, ErrCorruptJournal
		}
		anchors[i] = ContinuationAnchor{Index: record.Index, Epoch: record.Epoch, PanicAttempts: uint64(lastAttempt), SignalCursor: lastSignal}
	}
	// A limit failure retains the declaration that could not be appended. Audit
	// it as an SDK-only pending step while preserving the original terminal and
	// raw-record anchors (including at the hard journal-length boundary).
	last := records[len(records)-1]
	if last.Kind == journal.Failed {
		var outcome Outcome
		if json.Unmarshal(last.Payload, &outcome) == nil && len(outcome.LimitRequest) != 0 {
			if opts.InvSeq == 0 {
				return result, ErrInvocationIdentity
			}
			var declaration request
			if outcome.InvSeq != opts.InvSeq || outcome.Error != journal.ErrTooLong.Error() || outcome.LimitEntry != nil || len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "" || json.Unmarshal(outcome.LimitRequest, &declaration) != nil || declaration.Kind == "" || len(entries)%2 != 0 {
				return result, ErrCorruptJournal
			}
			for i, entry := range entries {
				want := StepRequested
				if i%2 == 1 {
					want = StepCompleted
				}
				if entry.Kind != want {
					return result, ErrCorruptJournal
				}
			}
			if stages != nil && declaration.Kind == "checkpoint" && stages[declaration.Name] == nil {
				return result, fmt.Errorf("%w: %s", ErrUnknownContinuation, declaration.Name)
			}
			entries = append(entries, Entry{Index: last.Index, Kind: StepRequested, Payload: bytes.Clone(outcome.LimitRequest)})
		}
	}
	c := NewContext(context.Background(), entries, nil, signals...)
	configure := func(c *Context) {
		c.replay = true
		c.SetChildResultValidator(childValidator)
		c.SetResultStore(nil, loadObject)
		// Recorded waits never publish a schedule or start a child offline.
		c.SetTimerSupport(time.Time{}, nil, func(context.Context, uint64, time.Time) error { return nil })
		if opts.Type != "" {
			c.SetChildSupport(opts.Type, opts.ID, opts.InvSeq, func(context.Context, string, string, []byte, string) error { return nil })
		}
		if stages != nil {
			c.SetContinuationSupport(func(stage string) bool { return stages[stage] != nil }, func(completed uint64, _ bool) (ContinuationAnchor, error) {
				if completed == 0 || completed >= uint64(len(records)) || records[completed].Kind != journal.StepCompleted {
					return ContinuationAnchor{}, ErrCorruptJournal
				}
				return anchors[completed], nil
			})
		}
	}
	configure(c)
	var currentStage string
	var continuations int
	var stagePanicked bool
	defer func() {
		panicked := stagePanicked
		if panicValue := recover(); panicValue != nil {
			panicked = true
			var zero T
			result = zero
			err = fmt.Errorf("workflow panic: %v", panicValue)
		}
		if opts.Observation != nil {
			*opts.Observation = ReplayObservation{WaitingOn: c.WaitingOn(), PlayedSteps: int(c.stepPosition()), RecordedSteps: len(entries), Panicked: panicked, Stage: currentStage, Continuations: continuations}
		}
	}()
	if stages == nil {
		result, err = fn(c)
		if err != nil {
			return result, err
		}
		return result, c.CheckComplete()
	}
	current := ReplayContinuation[T](func(c *Context, _ json.RawMessage) (T, error) { return fn(c) })
	var locals json.RawMessage
	for {
		result, err, stagePanicked = invokeReplayStage(current, c, locals)
		if failure := c.ContinuationFailure(); failure != nil {
			return result, failure
		}
		point, committed := c.Continuation()
		if !committed {
			if err != nil {
				return result, err
			}
			if completeErr := c.CheckComplete(); completeErr != nil {
				return result, completeErr
			}
			return result, verifyReplayTerminal(records, result, opts.InvSeq, loadObject)
		}
		continuations++
		if !hasReplayStageProgress(records[point.Index+1:]) {
			return result, ErrContinuation
		}
		var suffixSignals []Signal
		for _, signal := range signals {
			if signal.Sequence > point.SignalCursor {
				suffixSignals = append(suffixSignals, signal)
			}
		}
		raw, loadErr := loadObject(context.Background(), point.Object)
		if loadErr != nil {
			return result, loadErr
		}
		nextContext, info, restoreErr := NewCheckpointContext(context.Background(), c.entries[c.position:], nil, raw, CheckpointLocation{Type: opts.Type, ID: opts.ID, InvSeq: opts.InvSeq, Index: point.Index, Epoch: point.Epoch, Hash: point.SHA256}, suffixSignals...)
		if restoreErr != nil {
			return result, restoreErr
		}
		c = nextContext
		configure(c)
		currentStage, locals, current = info.Stage, info.Data, stages[info.Stage]
		stagePanicked = false
	}
}

func invokeReplayStage[T any](fn ReplayContinuation[T], c *Context, locals json.RawMessage) (result T, err error, panicked bool) {
	defer func() {
		if value := recover(); value != nil {
			var zero T
			result = zero
			err = fmt.Errorf("workflow panic: %v", value)
			panicked = true
		}
	}()
	result, err = fn(c, locals)
	return result, err, false
}

func hasReplayStageProgress(records []journal.Record) bool {
	for _, record := range records {
		if record.Kind != journal.Suspended {
			return true
		}
		var wait struct {
			WaitingOn string `json:"waiting_on"`
		}
		if json.Unmarshal(record.Payload, &wait) != nil || !strings.HasPrefix(wait.WaitingOn, "continuation:") {
			return true
		}
	}
	return false
}

// Completed histories bind the replayed return value as well as declarations.
// RawMessage is the worker Handler's exact result representation; other SDK
// return types use the JSON encoding a typed handler wrapper would publish.
func verifyReplayTerminal[T any](records []journal.Record, result T, invSeq uint64, load func(context.Context, string) ([]byte, error)) error {
	last := records[len(records)-1]
	if last.Kind != journal.Completed {
		return nil
	}
	var outcome Outcome
	if json.Unmarshal(last.Payload, &outcome) != nil || outcome.InvSeq != invSeq || outcome.Error != "" || outcome.LimitRequest != nil || outcome.LimitEntry != nil {
		return ErrCorruptJournal
	}
	expected, err := outcome.ResultBytes(context.Background(), load)
	if err != nil {
		return err
	}
	var actual []byte
	if raw, ok := any(result).(json.RawMessage); ok {
		actual = raw
	} else {
		actual, err = json.Marshal(result)
		if err != nil {
			return fmt.Errorf("%w: encode replay result: %v", ErrNonDeterministic, err)
		}
	}
	if !bytes.Equal(actual, expected) {
		return fmt.Errorf("%w: replay result differs from recorded terminal", ErrNonDeterministic)
	}
	return nil
}
