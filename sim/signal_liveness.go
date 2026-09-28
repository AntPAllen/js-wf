package sim

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// SignalLivenessReport describes retained signals that still need a worker
// wakeup, and those that have a named reason to remain without one.
type SignalLivenessReport struct {
	Enabled int
	Waiting map[string]string
	Missing []string
}

// CheckSignalWakeupLiveness checks quiesced retained model state independently
// of SignalScan's candidate list. An eligible signal must have a retained
// WF_RUN identified by the scanner's stable signal-wakeup message ID.
func CheckSignalWakeupLiveness(model *SignalTransport) (SignalLivenessReport, error) {
	report := SignalLivenessReport{Waiting: map[string]string{}}
	if model == nil {
		return report, fmt.Errorf("nil signal liveness model")
	}
	model.StartTransport.mu.Lock()
	invocations := make(map[string]jetstream.RawStreamMsg, len(model.invocations))
	for subject, invocation := range model.invocations {
		invocations[subject] = invocation
	}
	runs := append([]Message(nil), model.runs...)
	runIDs := make(map[string]runDedupEntry, len(model.runIDs))
	for id, entry := range model.runIDs {
		runIDs[id] = entry
	}
	model.StartTransport.mu.Unlock()
	model.mu.Lock()
	signals := make([]jetstream.RawStreamMsg, 0, len(model.signals))
	for _, signal := range model.signals {
		signals = append(signals, signal)
	}
	journals := make(map[string][]journal.Record, len(model.journals))
	for key, records := range model.journals {
		journals[key] = cloneJournalRecords(records)
	}
	model.mu.Unlock()
	sort.Slice(signals, func(i, j int) bool { return signals[i].Sequence < signals[j].Sequence })
	for _, signal := range signals {
		parts := strings.Split(signal.Subject, ".")
		if len(parts) != 5 || parts[0] != "wf" || parts[1] != "sig" || identity.Validate(parts[2], parts[3]) != nil || identity.ValidateToken(parts[4]) != nil {
			return report, fmt.Errorf("invalid retained signal %q", signal.Subject)
		}
		typ, id := parts[2], parts[3]
		label := fmt.Sprintf("%s#%d", signal.Subject, signal.Sequence)
		invocation, exists := invocations[identity.InvocationSubject(typ, id)]
		if !exists {
			report.Waiting[label] = "invocation absent"
			continue
		}
		if generation := signal.Header.Get("Wf-Inv-Seq"); generation != "" && generation != strconv.FormatUint(invocation.Sequence, 10) {
			report.Waiting[label] = "stale invocation generation"
			continue
		}
		consumed := false
		terminal := false
		for _, record := range journals[identity.Key(typ, id)] {
			if record.Kind == journal.Completed || record.Kind == journal.Failed {
				terminal = true
			}
			if record.Kind == journal.SignalConsumed {
				var event struct {
					Sequence uint64 `json:"sig_seq"`
				}
				if err := json.Unmarshal(record.Payload, &event); err != nil {
					return report, fmt.Errorf("%s consumed signal: %w", label, err)
				}
				if event.Sequence == signal.Sequence {
					consumed = true
				}
			}
		}
		if terminal {
			report.Waiting[label] = "terminal invocation"
			continue
		}
		if consumed {
			report.Waiting[label] = "signal consumed"
			continue
		}
		report.Enabled++
		messageID := fmt.Sprintf("signal-wakeup:%d", signal.Sequence)
		entry, exists := runIDs[messageID]
		retained := false
		if exists {
			for _, run := range runs {
				if run.Sequence == entry.sequence && string(run.Data) == identity.Key(typ, id) {
					retained = true
					break
				}
			}
		}
		if !retained {
			report.Missing = append(report.Missing, label)
		}
	}
	if len(report.Missing) != 0 {
		return report, fmt.Errorf("eligible signals lack WF_RUN: %v", report.Missing)
	}
	return report, nil
}
