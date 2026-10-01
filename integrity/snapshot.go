package integrity

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"js-wf/identity"
	"js-wf/journal"
)

// Snapshot is a retained invocation list, reconstructed logical journals,
// and terminal KV values. Simulated transports can supply this without
// implementing the whole JetStream client interface.
type Snapshot struct {
	Invocations   []string
	Journals      map[string][]journal.Record
	TerminalState map[string][]byte
}

// CheckSnapshot applies the same journal checks as Check to a modeled state.
// The caller supplies retained state rather than expected test-local results.
func CheckSnapshot(snapshot Snapshot) (Report, error) {
	var report Report
	seen := map[string]struct{}{}
	for _, subject := range snapshot.Invocations {
		if _, ok := seen[subject]; ok {
			return report, fmt.Errorf("duplicate invocation %s", subject)
		}
		seen[subject] = struct{}{}
		report.Invocations++
	}
	subjects := make([]string, 0, len(snapshot.Journals))
	for subject := range snapshot.Journals {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	for _, subject := range subjects {
		key, err := journalKey(subject, seen)
		if err != nil {
			return report, err
		}
		report.Journals++
		entries, terminal, err := checkJournalRecords(subject, snapshot.Journals[subject], func() ([]byte, error) {
			value, ok := snapshot.TerminalState[key]
			if !ok {
				return nil, fmt.Errorf("terminal state missing")
			}
			return value, nil
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

func journalKey(subject string, seen map[string]struct{}) (string, error) {
	key := strings.TrimPrefix(subject, "wf.jrn.")
	parts := strings.Split(key, ".")
	if len(parts) != 2 || !strings.HasPrefix(subject, "wf.jrn.") {
		return "", fmt.Errorf("invalid journal subject %s", subject)
	}
	if _, ok := seen["wf.inv."+key]; !ok {
		return "", fmt.Errorf("journal %s has no invocation", subject)
	}
	return key, nil
}

func checkJournalRecords(subject string, records []journal.Record, terminalValue func() ([]byte, error)) (int, bool, error) {
	var terminal []byte
	hasTerminal := false
	outstanding := false
	var requestPayload, completionPayload json.RawMessage
	var lastSignalSeq uint64
	var lastAttempt int
	epochWorkers := map[uint64]string{}
	for i, record := range records {
		e := record.Entry
		if record.Sequence == 0 || i > 0 && record.Sequence <= records[i-1].Sequence {
			return 0, false, fmt.Errorf("%s: invalid retained journal sequence", subject)
		}
		if e.Index != uint64(i) {
			return 0, false, fmt.Errorf("%s: index %d at position %d", subject, e.Index, i)
		}
		if i == 0 && e.Kind != journal.Started {
			return 0, false, fmt.Errorf("%s: missing Started", subject)
		}
		if i > 0 && (e.Epoch < records[i-1].Epoch || e.Kind == journal.Started) {
			return 0, false, fmt.Errorf("%s: epoch or Started violation", subject)
		}
		if e.Epoch != 0 && e.WorkerID != "" {
			if prior := epochWorkers[e.Epoch]; prior != "" && prior != e.WorkerID {
				return 0, false, fmt.Errorf("%s: epoch %d used by workers %s and %s", subject, e.Epoch, prior, e.WorkerID)
			}
			epochWorkers[e.Epoch] = e.WorkerID
		}
		if hasTerminal {
			return 0, false, fmt.Errorf("%s: entry after terminal", subject)
		}
		switch e.Kind {
		case journal.Started, journal.Failed:
		case journal.Completed:
			if outstanding {
				return 0, false, fmt.Errorf("%s: successful terminal with unresolved request", subject)
			}
		case journal.StepRequested:
			if outstanding {
				return 0, false, fmt.Errorf("%s: overlapping step requests", subject)
			}
			outstanding = true
			requestPayload = e.Payload
			completionPayload = nil
		case journal.StepCompleted:
			if !outstanding {
				return 0, false, fmt.Errorf("%s: completion without request", subject)
			}
			outstanding = false
			completionPayload = e.Payload
		case journal.Suspended:
			if !outstanding && !validContinuationSuspension(requestPayload, completionPayload, e.Payload) {
				return 0, false, fmt.Errorf("%s: suspension without request", subject)
			}
		case journal.SignalConsumed:
			var sig struct {
				Sequence uint64 `json:"sig_seq"`
			}
			if err := json.Unmarshal(e.Payload, &sig); err != nil || sig.Sequence <= lastSignalSeq {
				return 0, false, fmt.Errorf("%s: signal sequence out of order", subject)
			}
			lastSignalSeq = sig.Sequence
		case journal.Attempt:
			attempt, err := journal.DecodeAttempt(e.Payload)
			if err != nil || attempt.Count != lastAttempt+1 {
				return 0, false, fmt.Errorf("%s: handler attempt out of order", subject)
			}
			lastAttempt = attempt.Count
		default:
			return 0, false, fmt.Errorf("%s: unknown entry kind %q", subject, e.Kind)
		}
		if e.Kind == journal.Completed || e.Kind == journal.Failed {
			terminal = e.Payload
			hasTerminal = true
		}
	}
	if hasTerminal {
		value, err := terminalValue()
		if err != nil {
			return 0, false, fmt.Errorf("%s: terminal state missing: %w", subject, err)
		}
		if !bytes.Equal(value, terminal) {
			return 0, false, fmt.Errorf("%s: terminal state differs", subject)
		}
	}
	return len(records), hasTerminal, nil
}

// A continuation suspends after its completed checkpoint rather than with an
// outstanding await. Require the matching declaration and content reference.
func validContinuationSuspension(request, completion, suspension json.RawMessage) bool {
	var req struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
	var done struct {
		Result    json.RawMessage `json:"result"`
		Ref       string          `json:"result_ref"`
		Hash      string          `json:"result_hash"`
		Error     string          `json:"error"`
		ErrorKind string          `json:"error_kind"`
		Signal    uint64          `json:"signal_seq"`
		Selected  string          `json:"selected"`
	}
	var wait struct {
		WaitingOn string `json:"waiting_on"`
	}
	if json.Unmarshal(request, &req) != nil || json.Unmarshal(completion, &done) != nil || json.Unmarshal(suspension, &wait) != nil || req.Kind != "checkpoint" || identity.ValidateToken(req.Name) != nil || wait.WaitingOn != "continuation:"+req.Name {
		return false
	}
	hash, err := hex.DecodeString(done.Hash)
	return err == nil && len(hash) == 32 && hex.EncodeToString(hash) == done.Hash && done.Ref == "step-result-"+done.Hash && len(done.Result) == 0 && done.Error == "" && done.ErrorKind == "" && done.Signal == 0 && done.Selected == ""
}
