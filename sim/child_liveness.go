package sim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// ChildLivenessReport describes terminal child outcomes awaiting delivery to
// their current parent generation. Waiting names already satisfied/obsolete
// obligations; Missing names absent notifications or retained parent wakeups.
type ChildLivenessReport struct {
	Enabled int
	Waiting map[string]string
	Missing []string
}

// CheckChildWakeupLiveness checks quiesced retained state, starting from child
// terminal journals rather than the signals that a notifier managed to write.
// Call it after notification/repair attempts, not during an in-flight publish.
func CheckChildWakeupLiveness(model *SignalTransport) (ChildLivenessReport, error) {
	report := ChildLivenessReport{Waiting: map[string]string{}}
	if model == nil {
		return report, fmt.Errorf("nil child liveness model")
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
	subjects := make([]string, 0, len(invocations))
	for subject := range invocations {
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	for _, subject := range subjects {
		child := invocations[subject]
		parentType := child.Header.Get(client.ParentTypeHeader)
		if parentType == "" {
			continue
		}
		childKey := strings.TrimPrefix(subject, "wf.inv.")
		label := fmt.Sprintf("%s#%d", subject, child.Sequence)
		records := journals[childKey]
		if len(records) == 0 || records[len(records)-1].Kind != journal.Completed && records[len(records)-1].Kind != journal.Failed {
			report.Waiting[label] = "child not terminal"
			continue
		}
		payload := records[len(records)-1].Payload
		var outcome struct {
			Generation uint64 `json:"inv_seq"`
		}
		if err := json.Unmarshal(payload, &outcome); err != nil || outcome.Generation == 0 {
			return report, fmt.Errorf("%s: invalid child outcome generation", label)
		}
		if outcome.Generation != child.Sequence {
			report.Waiting[label] = "stale child generation"
			continue
		}
		parentID := child.Header.Get(client.ParentIDHeader)
		name := child.Header.Get(client.ParentSignalHeader)
		generation, err := strconv.ParseUint(child.Header.Get(client.ParentInvSeqHeader), 10, 64)
		if err != nil || generation == 0 || identity.Validate(parentType, parentID) != nil || identity.ValidateToken(name) != nil {
			return report, fmt.Errorf("%s: invalid parent identity", label)
		}
		parent, exists := invocations[identity.InvocationSubject(parentType, parentID)]
		if !exists {
			report.Waiting[label] = "parent absent"
			continue
		}
		if parent.Sequence != generation {
			report.Waiting[label] = "stale parent generation"
			continue
		}
		parentKey := identity.Key(parentType, parentID)
		consumed := map[uint64]bool{}
		satisfied := ""
		for _, record := range journals[parentKey] {
			if record.Kind == journal.Completed || record.Kind == journal.Failed {
				var terminal struct {
					Generation uint64 `json:"inv_seq"`
				}
				if json.Unmarshal(record.Payload, &terminal) == nil && terminal.Generation == generation {
					satisfied = "parent terminal"
					break
				}
			}
			if record.Kind == journal.SignalConsumed {
				var event struct {
					Sequence uint64 `json:"sig_seq"`
					Name     string `json:"name"`
					Payload  []byte `json:"payload"`
					Hash     string `json:"hash"`
					Ref      string `json:"ref"`
				}
				if err := json.Unmarshal(record.Payload, &event); err != nil {
					return report, fmt.Errorf("%s: malformed parent consumption: %w", label, err)
				}
				consumed[event.Sequence] = true
				if event.Name == name && (event.Ref == "" && bytes.Equal(event.Payload, payload) || event.Ref != "" && event.Hash == digest(payload)) {
					satisfied = "child outcome consumed"
				}
			}
		}
		if satisfied != "" {
			report.Waiting[label] = satisfied
			continue
		}
		report.Enabled++
		notified := false
		awakened := false
		for _, signal := range signals {
			if signal.Subject != "wf.sig."+parentKey+"."+name || signal.Header.Get("Wf-Inv-Seq") != strconv.FormatUint(generation, 10) {
				continue
			}
			if signal.Header.Get("Wf-Signal-Ref") == "" && !bytes.Equal(signal.Data, payload) || signal.Header.Get("Wf-Signal-Ref") != "" && signal.Header.Get("Wf-Input-SHA256") != digest(payload) {
				continue
			}
			notified = true
			if consumed[signal.Sequence] {
				awakened = true
				break
			}
			entry, exists := runIDs[fmt.Sprintf("signal-wakeup:%d", signal.Sequence)]
			if !exists {
				continue
			}
			for _, run := range runs {
				if run.Sequence == entry.sequence && string(run.Data) == parentKey {
					awakened = true
					break
				}
			}
			if awakened {
				break
			}
		}
		if !notified {
			report.Missing = append(report.Missing, label+": notification missing")
		} else if !awakened {
			report.Missing = append(report.Missing, label+": parent wakeup missing")
		}
	}
	if len(report.Missing) > 0 {
		return report, fmt.Errorf("terminal children lack parent delivery: %v", report.Missing)
	}
	return report, nil
}
