package sim

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"

	"github.com/nats-io/nats.go/jetstream"
)

// StartLivenessReport counts retained invocations that have not entered a
// journal and names those without a generation-matched retained run message.
type StartLivenessReport struct {
	Unstarted int
	Missing   []string
}

// CheckStartWakeupLiveness checks the start-to-run gap after modeled actors
// have quiesced. A run for an older generation of a reused ID does not count.
func CheckStartWakeupLiveness(model *StartTransport) (StartLivenessReport, error) {
	var report StartLivenessReport
	if model == nil {
		return report, fmt.Errorf("nil start liveness model")
	}
	model.mu.Lock()
	defer model.mu.Unlock()
	invocations := make([]jetstream.RawStreamMsg, 0, len(model.invocations))
	for _, invocation := range model.invocations {
		invocations = append(invocations, invocation)
	}
	sort.Slice(invocations, func(i, j int) bool { return invocations[i].Sequence < invocations[j].Sequence })
	for _, invocation := range invocations {
		parts := strings.Split(invocation.Subject, ".")
		if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
			return report, fmt.Errorf("invalid retained invocation %q", invocation.Subject)
		}
		typ, id := parts[2], parts[3]
		if model.journals[identity.JournalSubject(typ, id)] == invocation.Sequence {
			continue
		}
		report.Unstarted++
		key := identity.Key(typ, id)
		messageID := fmt.Sprintf("start:%s:%d", key, invocation.Sequence)
		entry, ok := model.runIDs[messageID]
		retained := false
		if ok {
			for _, run := range model.runs {
				if run.Sequence == entry.sequence && run.Subject == identity.RunSubject(typ, id, provision.Partitions) && string(run.Data) == key {
					retained = true
					break
				}
			}
		}
		if !retained {
			report.Missing = append(report.Missing, key)
		}
	}
	if len(report.Missing) != 0 {
		return report, fmt.Errorf("unstarted invocations lack WF_RUN: %v", report.Missing)
	}
	return report, nil
}

// SuspendedLivenessReport names waits that are not yet enabled and enabled
// waits whose reconciler wakeup is missing. It covers suspended timer, signal,
// and timer/signal select waits after modeled actors have quiesced.
type SuspendedLivenessReport struct {
	Enabled int
	Waiting map[string]string
	Missing []string
}

// CheckSuspendedWakeupLiveness checks retained model state independently of
// SuspendedScan's candidate list. It uses the virtual server time supplied by
// the caller and requires each enabled wait's stable reconcile message ID to
// identify a retained WF_RUN message.
func CheckSuspendedWakeupLiveness(model *SignalTransport, now time.Time, grace time.Duration) (SuspendedLivenessReport, error) {
	report := SuspendedLivenessReport{Waiting: map[string]string{}}
	if model == nil || grace < 0 {
		return report, fmt.Errorf("invalid suspended liveness model or grace")
	}
	model.StartTransport.mu.Lock()
	invocations := make([]jetstream.RawStreamMsg, 0, len(model.invocations))
	for _, message := range model.invocations {
		invocations = append(invocations, message)
	}
	runs := append([]Message(nil), model.runs...)
	runIDs := make(map[string]runDedupEntry, len(model.runIDs))
	for id, entry := range model.runIDs {
		runIDs[id] = entry
	}
	model.StartTransport.mu.Unlock()
	model.mu.Lock()
	journals := make(map[string][]journal.Record, len(model.journals))
	for key, records := range model.journals {
		journals[key] = cloneJournalRecords(records)
	}
	signals := make([]jetstream.RawStreamMsg, 0, len(model.signals))
	for _, message := range model.signals {
		signals = append(signals, message)
	}
	model.mu.Unlock()
	sort.Slice(invocations, func(i, j int) bool { return invocations[i].Sequence < invocations[j].Sequence })
	sort.Slice(signals, func(i, j int) bool { return signals[i].Sequence < signals[j].Sequence })
	for _, invocation := range invocations {
		parts := strings.Split(invocation.Subject, ".")
		if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
			return report, fmt.Errorf("invalid retained invocation %q", invocation.Subject)
		}
		typ, id := parts[2], parts[3]
		key := identity.Key(typ, id)
		records := journals[key]
		if len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
			continue
		}
		var wait struct {
			WaitingOn string `json:"waiting_on"`
		}
		if err := json.Unmarshal(records[len(records)-1].Payload, &wait); err != nil {
			return report, fmt.Errorf("%s suspended wait: %w", key, err)
		}
		enabled, reason, err := suspendedWaitEnabled(records, signals, invocation.Sequence, typ, id, wait.WaitingOn, now, grace)
		if err != nil {
			return report, fmt.Errorf("%s: %w", key, err)
		}
		if !enabled {
			report.Waiting[key] = reason
			continue
		}
		report.Enabled++
		messagePrefix := fmt.Sprintf("reconcile:%s:%s:%d:", typ, id, records[len(records)-1].Sequence)
		retained := false
		for messageID, entry := range runIDs {
			if !strings.HasPrefix(messageID, messagePrefix) {
				continue
			}
			if _, err := strconv.ParseInt(strings.TrimPrefix(messageID, messagePrefix), 10, 64); err != nil {
				continue
			}
			for _, run := range runs {
				if run.Sequence == entry.sequence && string(run.Data) == key {
					retained = true
					break
				}
			}
			if retained {
				break
			}
		}
		if !retained {
			report.Missing = append(report.Missing, key+" ("+reason+")")
		}
	}
	if len(report.Missing) != 0 {
		return report, fmt.Errorf("enabled suspended waits lack WF_RUN: %v", report.Missing)
	}
	return report, nil
}

func suspendedWaitEnabled(records []journal.Record, signals []jetstream.RawStreamMsg, invSeq uint64, typ, id, waitingOn string, now time.Time, grace time.Duration) (bool, string, error) {
	if waitingOn == "select_many" {
		return retainedSelectionEnabled(records, signals, invSeq, typ, id, now, grace)
	}
	parts := strings.Split(waitingOn, ":")
	switch {
	case len(parts) == 2 && parts[0] == "timer":
		return pendingTimerDue(records, parts[1], now, grace)
	case len(parts) == 2 && parts[0] == "signal":
		return retainedSignalAvailable(records, signals, invSeq, typ, id, parts[1])
	case len(parts) == 3 && parts[0] == "select":
		available, _, err := retainedSignalAvailable(records, signals, invSeq, typ, id, parts[2])
		if err != nil || available {
			return available, "select signal", err
		}
		due, reason, err := pendingTimerDue(records, parts[1], now, grace)
		if due {
			return true, "select timer", err
		}
		return false, reason, err
	default:
		return false, "", fmt.Errorf("unsupported suspended wait %q", waitingOn)
	}
}

// Inspect retained cases independently of the production scanner's transport
// reads. A ready case must have a retained repair wakeup after faults heal.
func retainedSelectionEnabled(records []journal.Record, signals []jetstream.RawStreamMsg, invSeq uint64, typ, id string, now time.Time, grace time.Duration) (bool, string, error) {
	var pending *journal.Record
	for i := range records {
		if records[i].Kind == journal.StepRequested {
			pending = &records[i]
		} else if records[i].Kind == journal.StepCompleted {
			pending = nil
		}
	}
	if pending == nil {
		return false, "", fmt.Errorf("selection has no pending request")
	}
	var request struct {
		Kind  string `json:"kind"`
		Cases []struct {
			Kind   string    `json:"kind"`
			Name   string    `json:"name"`
			FireAt time.Time `json:"fire_at"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(pending.Payload, &request); err != nil {
		return false, "", err
	}
	if request.Kind != "select_many" || len(request.Cases) == 0 {
		return false, "", fmt.Errorf("invalid selection request")
	}
	for _, c := range request.Cases {
		if identity.ValidateToken(c.Name) != nil || c.Kind != "timer" && c.Kind != "signal" && c.Kind != "promise" {
			return false, "", fmt.Errorf("invalid selection case")
		}
	}
	for _, c := range request.Cases {
		if c.Kind == "timer" {
			if c.FireAt.IsZero() || !now.Before(c.FireAt.Add(grace)) {
				return true, "selection timer " + c.Name, nil
			}
			continue
		}
		ready, _, err := retainedSignalAvailable(records, signals, invSeq, typ, id, c.Name)
		if err != nil {
			return false, "", err
		}
		if ready {
			return true, "selection " + c.Kind + " " + c.Name, nil
		}
	}
	return false, "selection has no ready case", nil
}

func pendingTimerDue(records []journal.Record, name string, now time.Time, grace time.Duration) (bool, string, error) {
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
		return false, "", fmt.Errorf("timer wait has no pending request")
	}
	var request struct {
		Kind      string    `json:"kind"`
		Name      string    `json:"name"`
		TimerName string    `json:"timer_name"`
		FireAt    time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(pending.Payload, &request); err != nil {
		return false, "", err
	}
	valid := request.Kind == "timer_signal_select" && request.TimerName == name ||
		(request.Kind == "timer" || request.Kind == "timer_await") && request.Name == name
	if !valid || request.FireAt.IsZero() {
		return false, "", fmt.Errorf("timer wait differs from pending request")
	}
	if now.Before(request.FireAt.Add(grace)) {
		return false, "future timer until " + request.FireAt.Add(grace).UTC().Format(time.RFC3339Nano), nil
	}
	return true, "due timer", nil
}

func retainedSignalAvailable(records []journal.Record, signals []jetstream.RawStreamMsg, invSeq uint64, typ, id, name string) (bool, string, error) {
	used := map[uint64]bool{}
	var lastConsumed uint64
	var matchingConsumed []uint64
	for _, record := range records {
		switch record.Kind {
		case journal.StepCompleted:
			var done struct {
				SignalSeq uint64 `json:"signal_seq"`
			}
			if err := json.Unmarshal(record.Payload, &done); err != nil {
				return false, "", err
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
				return false, "", fmt.Errorf("invalid consumed signal")
			}
			if event.Sequence > lastConsumed {
				lastConsumed = event.Sequence
			}
			if event.Name == name {
				matchingConsumed = append(matchingConsumed, event.Sequence)
			}
		}
	}
	for _, sequence := range matchingConsumed {
		if !used[sequence] {
			return true, "consumed signal", nil
		}
	}
	subject := "wf.sig." + typ + "." + id + "." + name
	for _, signal := range signals {
		if signal.Sequence <= lastConsumed || signal.Subject != subject {
			continue
		}
		if generation := signal.Header.Get("Wf-Inv-Seq"); generation != "" && generation != strconv.FormatUint(invSeq, 10) {
			continue
		}
		if !used[signal.Sequence] {
			return true, "retained signal", nil
		}
	}
	return false, "signal unavailable", nil
}
