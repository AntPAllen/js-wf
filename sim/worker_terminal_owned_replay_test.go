package sim

import (
	"context"
	"fmt"
	"js-wf/journal"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

// Charge one observed client-read cost per complete history scan after terminal
// commitment. This models the cost, not its underlying NATS server mechanism.
type terminalReadCost struct {
	*JournalTransport
	schedule *Scheduler
	enabled  bool
	reads    int
}

func (p *terminalReadCost) Next(ctx context.Context, subject string, from uint64) (journal.AppendTail, error) {
	if p.enabled && from == 1 {
		p.reads++
		if err := p.schedule.AdvanceMillis(100); err != nil {
			return journal.AppendTail{}, err
		}
		p.schedule.RecordTransport(TransportEvent{Operation: "terminal_history_cost", Subject: subject, Outcome: "100ms"})
	}
	return p.JournalTransport.Next(ctx, subject, from)
}

func TestSeededTerminalOwnedReplay(t *testing.T) {
	if os.Getenv("SIM_TERMINAL_OWNED_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededTerminalOwned(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_TERMINAL_OWNED_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runSeededTerminalOwned(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "terminal-owned-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededTerminalOwned(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 11 {
		t.Fatalf("covered %d/11 modes", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("terminal-owned-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededTerminalOwnedReplay$")
		cmd.Env = append(os.Environ(), "SIM_TERMINAL_OWNED_HELPER=1", "SIM_TERMINAL_OWNED_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("terminal-owned trace changed across processes")
	}
}

func TestTerminalOwnedFiveHundred(t *testing.T) {
	// Match the real backlog cardinality at the observed 100ms read cost.
	// All seeded modes must drain without 500 repeated full history reads.
	modes := map[string]bool{}
	for seed := int64(1); len(modes) < 11 && seed <= 100; seed++ {
		trace, err := runSeededTerminalDelivery(seed, nil, true, 500)
		if err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		modes[trace.Decisions[0].Chosen] = true
		replayed, err := runSeededTerminalDelivery(seed, &trace, true, 500)
		if err != nil || !reflect.DeepEqual(trace, replayed) {
			t.Fatalf("seed=%d replay: %v", seed, err)
		}
	}
	if len(modes) != 11 {
		t.Fatalf("500-duplicate modes=%d", len(modes))
	}
}
