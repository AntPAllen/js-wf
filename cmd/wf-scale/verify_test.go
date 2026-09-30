package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfflineLiveVerifierRejectsIncompleteAndChangedEvidence(t *testing.T) {
	fixture := filepath.Join("..", "..", "docs", "scale", "live-cardinality-smoke-2026-09-30")
	if err := verifyLiveReport(fixture); err != nil {
		t.Fatal(err)
	}
	reportBytes, err := os.ReadFile(filepath.Join(fixture, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	auditBytes, err := os.ReadFile(filepath.Join(fixture, "live-100-audit.json"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, marker string
		changeReport func(*report)
		changeAudit  func(*liveEvidence)
		pretty       bool
	}{
		{name: "unfinished", marker: "completed live", changeReport: func(r *report) { r.Status = "running" }},
		{name: "missing cohort", marker: "completed live", changeReport: func(r *report) { r.Live = r.Live[:1] }},
		{name: "prior counts omitted", marker: "prior cohort", changeReport: func(r *report) { r.Samples[1].LiveInvocations = 0 }},
		{name: "aggregate count", marker: "aggregate counts", changeReport: func(r *report) { r.Live[1].Messages[2][1]-- }},
		{name: "spill count", marker: "recorded metrics", changeReport: func(r *report) { r.Live[0].SpilledInputs = 0 }},
		{name: "changed expected input", marker: "input/identity", changeAudit: func(e *liveEvidence) {
			v := e.Expected["checkpoint-100-0"]
			v.SHA256 = strings.Repeat("0", 64)
			e.Expected["checkpoint-100-0"] = v
		}},
		{name: "reordered journal", marker: "retained invariants", changeAudit: func(e *liveEvidence) { r := e.Snapshot.Journals["wf.jrn.scalelive.checkpoint-100-0"]; r[1].Index = 3 }},
		{name: "changed immutable result", marker: "terminal state differs", changeAudit: func(e *liveEvidence) {
			e.Snapshot.TerminalState["scalelive.checkpoint-100-0"] = []byte(`{"result":"eA=="}`)
		}},
		{name: "consistent but wrong outcome", marker: "result checkpoint", changeAudit: func(e *liveEvidence) {
			key := "scalelive.checkpoint-100-0"
			var state map[string]json.RawMessage
			if err := json.Unmarshal(e.Snapshot.TerminalState[key], &state); err != nil {
				panic(err)
			}
			encoded, err := json.Marshal([]byte(`{"bytes":1,"sha256":"wrong"}`))
			if err != nil {
				panic(err)
			}
			state["result"] = encoded
			terminal, err := json.Marshal(state)
			if err != nil {
				panic(err)
			}
			e.Snapshot.TerminalState[key] = terminal
			records := e.Snapshot.Journals["wf.jrn."+key]
			records[3].Payload = terminal
		}},
		{name: "pretty print changes payload bytes", marker: "terminal state differs", pretty: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			var r report
			if err := json.Unmarshal(reportBytes, &r); err != nil {
				t.Fatal(err)
			}
			var e liveEvidence
			if err := json.Unmarshal(auditBytes, &e); err != nil {
				t.Fatal(err)
			}
			if tt.changeReport != nil {
				tt.changeReport(&r)
			}
			if tt.changeAudit != nil {
				tt.changeAudit(&e)
			}
			changedReport, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			changedAudit, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if tt.pretty {
				changedAudit, err = json.MarshalIndent(e, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
			}
			second, err := os.ReadFile(filepath.Join(fixture, "live-200-audit.json"))
			if err != nil {
				t.Fatal(err)
			}
			for name, data := range map[string][]byte{"report.json": changedReport, "live-100-audit.json": changedAudit, "live-200-audit.json": second} {
				if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := verifyLiveReport(root); err == nil || !strings.Contains(err.Error(), tt.marker) {
				t.Fatalf("expected %q rejection, got %v", tt.marker, err)
			}
		})
	}
}
