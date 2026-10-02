package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func runSeededClientSignalScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("client_signal_repair_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewSignalTransport(schedule)
	c := client.NewWithSignalPorts(model, model)
	scan := reconcile.NewSignalScanWithPort(model)
	const typ = "test"
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"normal", "duplicate", "mismatch", "drop", "lost_ack", "lost_enqueue", "dropped_enqueue", "consumed_purged", "stale_generation", "terminal_required", "no_invocation", "signal_with_start"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("case-%02d", i)
		payload := []byte(fmt.Sprintf("payload-%02d", i))
		beforeRuns := len(model.Runs())
		var generation uint64
		if mode != "no_invocation" && mode != "signal_with_start" {
			generation, err = model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")})
			if err != nil {
				return trace, err
			}
		}
		call := func() (uint64, error) { return c.Signal(ctx, typ, id, "go", payload, "key") }
		var sequence uint64
		switch mode {
		case "normal":
			sequence, err = call()
		case "duplicate", "mismatch":
			sequence, err = call()
			if err == nil && mode == "duplicate" {
				var again uint64
				again, err = call()
				if err == nil && again != sequence {
					return trace, fmt.Errorf("seed %d case %d duplicate seq=%d first=%d", seed, i, again, sequence)
				}
			} else if err == nil {
				_, mismatchErr := c.Signal(ctx, typ, id, "go", []byte("changed"), "key")
				if !errors.Is(mismatchErr, client.ErrSignalMismatch) {
					return trace, fmt.Errorf("seed %d case %d changed duplicate: %v", seed, i, mismatchErr)
				}
			}
		case "drop", "lost_ack":
			fault := SignalDropBeforeCommit
			if mode == "lost_ack" {
				fault = SignalLoseAckAfterCommit
			}
			if err := model.QueueSignalFault(fault); err != nil {
				return trace, err
			}
			_, firstErr := call()
			if !errors.Is(firstErr, client.ErrSignalUnknown) {
				return trace, fmt.Errorf("seed %d case %d mode=%s publish: %v", seed, i, mode, firstErr)
			}
			if mode == "lost_ack" {
				stored := model.SignalFor(typ, id, "go")
				if len(stored) != 1 {
					return trace, fmt.Errorf("seed %d case %d lost acknowledgment retained %d signals", seed, i, len(stored))
				}
				if _, err := scan.Scan(ctx, stored[0].Sequence, 1, false); err != nil {
					return trace, err
				}
			}
			sequence, err = call()
		case "lost_enqueue", "dropped_enqueue":
			kind := "lose_ack_after_commit"
			if mode == "dropped_enqueue" {
				kind = "drop_before_commit"
			}
			if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
				return trace, err
			}
			var firstErr error
			sequence, firstErr = call()
			if !errors.Is(firstErr, client.ErrEnqueueUnknown) || sequence == 0 {
				return trace, fmt.Errorf("seed %d case %d mode=%s enqueue seq=%d err=%v", seed, i, mode, sequence, firstErr)
			}
			if _, err := scan.Scan(ctx, sequence, 1, false); err != nil {
				return trace, err
			}
			var again uint64
			again, err = call()
			if err == nil && again != sequence {
				return trace, fmt.Errorf("seed %d case %d enqueue retry seq=%d first=%d", seed, i, again, sequence)
			}
		case "consumed_purged":
			sequence, err = call()
			if err == nil {
				hash := sha256.Sum256(payload)
				consumed := []byte(fmt.Sprintf(`{"sig_seq":%d,"name":"go","hash":"%s"}`, sequence, hex.EncodeToString(hash[:])))
				model.SetJournal(typ, id, []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: consumed}}})
				model.PurgeSignal(sequence)
				var again uint64
				again, err = call()
				if err == nil && again != sequence {
					return trace, fmt.Errorf("seed %d case %d purged retry seq=%d first=%d", seed, i, again, sequence)
				}
			}
		case "stale_generation":
			_, err = c.SignalToGeneration(ctx, typ, id, "go", payload, "key", generation+1)
			if errors.Is(err, client.ErrStaleGeneration) {
				err = nil
			} else {
				return trace, fmt.Errorf("seed %d case %d stale generation: %v", seed, i, err)
			}
		case "terminal_required":
			model.SetJournal(typ, id, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}})
			_, err = c.SignalWithOptions(ctx, typ, id, "go", payload, "key", client.SignalOptions{RequireRunning: true})
			if errors.Is(err, client.ErrNotRunning) {
				err = nil
			} else {
				return trace, fmt.Errorf("seed %d case %d terminal signal: %v", seed, i, err)
			}
		case "no_invocation":
			_, err = call()
			if errors.Is(err, client.ErrNotFound) {
				err = nil
			} else {
				return trace, fmt.Errorf("seed %d case %d absent invocation: %v", seed, i, err)
			}
		case "signal_with_start":
			var handle client.Handle
			handle, sequence, err = c.SignalWithStart(ctx, typ, id, "go", payload, "key", []byte("input"))
			if err == nil && (handle.InvSeq == 0 || sequence == 0) {
				return trace, fmt.Errorf("seed %d case %d signal with start: handle=%+v seq=%d", seed, i, handle, sequence)
			}
		}
		if err != nil {
			return trace, fmt.Errorf("seed %d case %d mode=%s: %w", seed, i, mode, err)
		}
		wantSignals, wantRuns := 1, 1
		switch mode {
		case "consumed_purged":
			wantSignals = 0
		case "stale_generation", "terminal_required", "no_invocation":
			wantSignals, wantRuns = 0, 0
		case "signal_with_start":
			wantRuns = 2
		}
		if gotSignals, gotRuns := len(model.SignalFor(typ, id, "go")), len(model.Runs())-beforeRuns; gotSignals != wantSignals || gotRuns != wantRuns {
			return trace, fmt.Errorf("seed %d case %d mode=%s signals=%d/%d runs=%d/%d", seed, i, mode, gotSignals, wantSignals, gotRuns, wantRuns)
		}
	}
	report, err := CheckSignalWakeupLiveness(model)
	if err != nil || len(report.Missing) != 0 {
		return trace, fmt.Errorf("seed %d client signal liveness: enabled=%d waiting=%v missing=%v err=%v", seed, report.Enabled, report.Waiting, report.Missing, err)
	}
	waiting, err := json.Marshal(report.Waiting)
	if err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_signal_liveness", Sequence: uint64(report.Enabled), DataSHA256: digest(waiting), Outcome: "ok", AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededClientSignalReplay(t *testing.T) {
	if os.Getenv("SIM_CLIENT_SIGNAL_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededClientSignalScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CLIENT_SIGNAL_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededClientSignalScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-client-signal-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-client-signal.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededClientSignalScenario(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d client signal replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("client-signal-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededClientSignalReplay$")
		cmd.Env = append(os.Environ(), "SIM_CLIENT_SIGNAL_HELPER=1", "SIM_CLIENT_SIGNAL_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("client signal child %d: %v: %s", i, err, output)
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
	if !bytes.Equal(first, second) {
		t.Fatal("seeded client signal trace changed across processes")
	}
}
