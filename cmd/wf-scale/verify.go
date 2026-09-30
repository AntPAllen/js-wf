package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
)

// verifyLiveReport checks retained cohort state and report consistency without
// connecting to NATS. Timing, spill and RSS observations are reported metrics;
// an offline audit cannot independently observe those original operations.
func verifyLiveReport(root string) error {
	if root == "" {
		return fmt.Errorf("offline verification requires -root")
	}
	raw, err := os.ReadFile(filepath.Join(root, "report.json"))
	if err != nil {
		return err
	}
	var report report
	if err := json.Unmarshal(raw, &report); err != nil {
		return err
	}
	if report.Status != "completed" || report.Error != "" || report.LiveRequested < 1 || len(report.Live) == 0 || len(report.Live) != len(report.Samples) {
		return fmt.Errorf("report lacks completed live phases")
	}
	previous, priorLive, priorEntries := 0, 0, 0
	for i, phase := range report.Live {
		sample := report.Samples[i]
		count := report.LiveRequested
		if count > 100000 || phase.Invocations != count || phase.InputBytes < 64 || phase.InputBytes > 5*1024*1024 || phase.BackgroundSubjects != sample.Subjects || sample.Subjects <= previous || sample.Subjects > 10000000 {
			return fmt.Errorf("phase %d invalid workload/cardinality", i)
		}
		if sample.LiveInvocations != priorLive || sample.LiveEntries != priorEntries || sample.InvocationMessages != uint64(sample.Subjects+priorLive) || sample.JournalMessages != uint64(sample.Subjects+priorEntries) || sample.InvocationSubjects != uint64(sample.Subjects+priorLive) || sample.JournalSubjects != uint64(sample.Subjects+priorLive) {
			return fmt.Errorf("phase %d prior cohort counts differ", i)
		}
		want := integrity.Report{Invocations: count, Journals: count, Entries: count * 4, Terminal: count}
		if phase.Audit != want {
			return fmt.Errorf("phase %d audit counts differ", i)
		}
		raw, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("live-%d-audit.json", sample.Subjects)))
		if err != nil {
			return err
		}
		var evidence liveEvidence
		if err := json.Unmarshal(raw, &evidence); err != nil {
			return err
		}
		if evidence.BackgroundSubjects != sample.Subjects || evidence.Scope != "live cohort only; background subjects are opaque capacity data" || len(evidence.Expected) != count {
			return fmt.Errorf("phase %d audit scope/results differ", i)
		}
		actual, err := integrity.CheckSnapshot(evidence.Snapshot)
		if err != nil {
			return fmt.Errorf("phase %d retained invariants: %w", i, err)
		}
		if actual != want || len(evidence.Snapshot.TerminalState) != count {
			return fmt.Errorf("phase %d retained audit counts differ", i)
		}
		subjects := make(map[string]bool, count)
		for _, subject := range evidence.Snapshot.Invocations {
			subjects[subject] = true
		}
		for index := 0; index < count; index++ {
			id := fmt.Sprintf("checkpoint-%d-%d", sample.Subjects, index)
			input, err := liveInput(index, phase.InputBytes)
			if err != nil {
				return err
			}
			expected := inputValue(input)
			recorded, ok := evidence.Expected[id]
			if !ok || recorded != expected || !subjects[identity.InvocationSubject("scalelive", id)] {
				return fmt.Errorf("phase %d input/identity %s differs", i, id)
			}
			records := evidence.Snapshot.Journals[identity.JournalSubject("scalelive", id)]
			if len(records) != 4 || records[0].Kind != journal.Started || records[1].Kind != journal.StepRequested || records[2].Kind != journal.StepCompleted || records[3].Kind != journal.Completed {
				return fmt.Errorf("phase %d workflow %s journal shape differs", i, id)
			}
			var terminal struct {
				Encoded []byte `json:"result"`
			}
			if err := json.Unmarshal(evidence.Snapshot.TerminalState[identity.Key("scalelive", id)], &terminal); err != nil {
				return err
			}
			var result liveValue
			if err := json.Unmarshal(terminal.Encoded, &result); err != nil || result != expected {
				return fmt.Errorf("phase %d result %s differs: %v", i, id, err)
			}
			if !bytes.Equal(records[3].Payload, evidence.Snapshot.TerminalState[identity.Key("scalelive", id)]) {
				return fmt.Errorf("phase %d terminal bytes differ", i)
			}
		}
		wantSpills := 0
		if phase.InputBytes > client.MaxInlineInput {
			wantSpills = count
		}
		if phase.SpilledInputs != wantSpills || phase.Effects < int64(count) || phase.P99Seconds < 0 || math.IsNaN(phase.P99Seconds) || math.IsInf(phase.P99Seconds, 0) || phase.MaxSeconds < phase.P99Seconds || math.IsNaN(phase.MaxSeconds) || math.IsInf(phase.MaxSeconds, 0) {
			return fmt.Errorf("phase %d inconsistent recorded metrics", i)
		}
		for node := 0; node < 3; node++ {
			wantSubjects := uint64(sample.Subjects + priorLive + count)
			if phase.Subjects[node] != [2]uint64{wantSubjects, wantSubjects} || phase.Messages[node] != [2]uint64{wantSubjects, uint64(sample.Subjects + priorEntries + count*4)} {
				return fmt.Errorf("phase %d node %d aggregate counts differ", i, node)
			}
		}
		previous, priorLive, priorEntries = sample.Subjects, priorLive+count, priorEntries+count*4
	}
	return nil
}
