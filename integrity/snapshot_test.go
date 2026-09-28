package integrity

import (
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
