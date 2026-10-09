package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestTerminalAuditCLIAdmission(t *testing.T) {
	graph := &workerGraphSelection{}
	for _, tc := range []struct {
		enabled, repair bool
		graph           *workerGraphSelection
		interval        time.Duration
		budget          int
	}{
		{true, true, nil, time.Second, 1},
		{true, false, graph, time.Second, 1},
		{true, true, graph, 0, 1},
		{true, true, graph, 11 * time.Second, 1},
		{true, true, graph, time.Second, 0},
		{false, true, graph, time.Second, 1},
	} {
		if _, err := selectTerminalAudit(tc.enabled, tc.repair, tc.graph, tc.interval, tc.budget); err == nil {
			t.Fatal("invalid audit selection accepted", tc)
		}
	}
	selected, err := selectTerminalAudit(true, true, graph, time.Second, 1)
	if err != nil || selected.interval != time.Second || selected.budget != 1 {
		t.Fatal(selected, err)
	}
	if selected, err := selectTerminalAudit(false, true, nil, 0, 0); err != nil || selected != nil {
		t.Fatal(selected, err)
	}
	base := []string{"-id", "audit", "-handler-plugin", "unused"}
	for _, args := range [][]string{
		{"-graph-terminal-audit"},
		{"-graph-terminal-audit-interval", "1s"},
	} {
		if err := run(context.Background(), append(append([]string(nil), base...), args...)); err == nil || !strings.Contains(err.Error(), "graph-terminal-audit") {
			t.Fatal(err)
		}
	}
}
