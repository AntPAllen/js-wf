package worker

import (
	"fmt"
	"testing"
)

func TestTerminalHintsRequireSnapshotRepairAndStayBounded(t *testing.T) {
	var hints terminalHints
	hints.add("pending", false)
	if hints.contains("pending") {
		t.Fatal("unrepaired snapshot enables shortcut")
	}
	hints.snapshotReady("pending")
	if !hints.contains("pending") {
		t.Fatal("successful snapshot cannot enable shortcut")
	}
	// A later terminal generation must finish its own snapshot, even if the
	// previous generation completed all snapshot work.
	hints.add("pending", false)
	if hints.contains("pending") {
		t.Fatal("previous snapshot readiness survives replacement")
	}
	for i := 0; i < 1024; i++ {
		hints.add(fmt.Sprint(i), true)
	}
	if hints.contains("pending") || len(hints.keys) != 1024 {
		t.Fatal("terminal hints do not evict at their bound")
	}
	hints.snapshotReady("evicted")
	if hints.contains("evicted") || len(hints.keys) != 1024 {
		t.Fatal("snapshot completion resurrects an evicted hint")
	}
	// Repeated wakeups for one resident must not evict another resident.
	for i := 0; i < 1024; i++ {
		hints.add("1023", true)
	}
	if !hints.contains("0") {
		t.Fatal("duplicate hint evicts unrelated terminal")
	}
}
