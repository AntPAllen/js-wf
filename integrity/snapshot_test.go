package integrity

import (
	"encoding/json"
	"strings"
	"testing"

	"js-wf/journal"
)

func terminalSnapshot() Snapshot {
	return Snapshot{
		Invocations: []string{"wf.inv.test.negative"},
		Journals: map[string][]journal.Record{
			"wf.jrn.test.negative": {
				{Entry: journal.Entry{Index: 0, Kind: journal.Started}, Sequence: 1},
				{Entry: journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "worker-a"}, Sequence: 2},
				{Entry: journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "worker-a"}, Sequence: 3},
				{Entry: journal.Entry{Epoch: 1, Index: 3, Kind: journal.Completed, Payload: []byte(`"ok"`), WorkerID: "worker-a"}, Sequence: 4},
			},
		},
		TerminalState: map[string][]byte{"test.negative": []byte(`"ok"`)},
	}
}

func TestSnapshotInvariantNegativeControls(t *testing.T) {
	if report, err := CheckSnapshot(terminalSnapshot()); err != nil || report != (Report{Invocations: 1, Journals: 1, Entries: 4, Terminal: 1}) {
		t.Fatalf("valid baseline: report=%+v err=%v", report, err)
	}
	for _, control := range []struct {
		name   string
		mutate func(*Snapshot)
		want   string
	}{
		{"I1 duplicate start", func(s *Snapshot) { s.Invocations = append(s.Invocations, s.Invocations[0]) }, "duplicate invocation"},
		{"I2 shared epoch", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][2].WorkerID = "worker-b" }, "used by workers"},
		{"I2 descending epoch", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][2].Epoch = 0 }, "epoch or Started violation"},
		{"I2 zero sequence", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][0].Sequence = 0 }, "invalid retained journal sequence"},
		{"I2 duplicate sequence", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][2].Sequence = 2 }, "invalid retained journal sequence"},
		{"I2 descending sequence", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][2].Sequence = 1 }, "invalid retained journal sequence"},
		{"unknown entry kind", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][2].Kind = "Unknown" }, "unknown entry kind"},
		{"I3 terminal with unresolved request", func(s *Snapshot) {
			r := &s.Journals["wf.jrn.test.negative"][2]
			r.Kind = journal.Attempt
			r.Payload = json.RawMessage(`{"count":1,"error":"panic"}`)
		}, "successful terminal with unresolved request"},
		{"I3 outcome without request", func(s *Snapshot) { s.Journals["wf.jrn.test.negative"][1].Kind = journal.StepCompleted }, "completion without request"},
		{"I6 changed terminal state", func(s *Snapshot) { s.TerminalState["test.negative"] = []byte(`"changed"`) }, "terminal state differs"},
	} {
		t.Run(control.name, func(t *testing.T) {
			snapshot := terminalSnapshot()
			control.mutate(&snapshot)
			if _, err := CheckSnapshot(snapshot); err == nil || !strings.Contains(err.Error(), control.want) {
				t.Fatalf("mutation survived: err=%v want=%q", err, control.want)
			}
		})
	}
}

// Cancellation, exhausted attempts and journal limits may fail a pending step.
// A live request or suspension may also be retained at an intermediate cut.
func TestSnapshotAllowsPendingRequestWithoutSuccessfulTerminal(t *testing.T) {
	for _, kind := range []journal.Kind{journal.Failed, journal.Suspended, journal.StepRequested} {
		t.Run(string(kind), func(t *testing.T) {
			s := terminalSnapshot()
			records := s.Journals["wf.jrn.test.negative"]
			if kind == journal.StepRequested {
				records = records[:2]
			} else {
				records = records[:3]
				records[2].Kind = kind
				records[2].Payload = []byte(`"ok"`)
			}
			s.Journals["wf.jrn.test.negative"] = records
			if _, err := CheckSnapshot(s); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotAllowsGlobalSequenceHoles(t *testing.T) {
	s := terminalSnapshot()
	for i := range s.Journals["wf.jrn.test.negative"] {
		s.Journals["wf.jrn.test.negative"][i].Sequence = uint64(10 + i*7)
	}
	if _, err := CheckSnapshot(s); err != nil {
		t.Fatal(err)
	}
}
