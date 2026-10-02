package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func runSeededSignalRepairScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("signal_repair_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewSignalTransport(schedule)
	scan := reconcile.NewSignalScanWithPort(model)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"missing_wakeup", "lost_enqueue_ack", "dropped_enqueue", "consumed", "terminal", "stale_generation", "purged", "no_invocation", "expired_retry"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("case-%02d", i)
		var generation uint64
		if mode != "no_invocation" {
			generation, err = model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
			if err != nil {
				return trace, err
			}
		}
		if mode == "stale_generation" {
			generation++
		}
		message := &nats.Msg{Subject: "wf.sig.test." + id + ".go", Data: []byte("payload"), Header: nats.Header{}}
		if generation != 0 {
			message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
		}
		sequence := model.CommitSignal(message)
		switch mode {
		case "consumed":
			model.SetJournal("test", id, []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: []byte(fmt.Sprintf(`{"sig_seq":%d}`, sequence))}}})
		case "terminal":
			model.SetJournal("test", id, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}})
		case "purged":
			model.PurgeSignal(sequence)
		}
		before := len(model.Runs())
		dry, err := scan.Scan(ctx, sequence, 1, true)
		if err != nil || len(model.Runs()) != before {
			return trace, fmt.Errorf("seed %d case %d mode=%s dry scan: result=%+v runs=%d err=%v", seed, i, mode, dry, len(model.Runs())-before, err)
		}
		eligible := mode == "missing_wakeup" || mode == "lost_enqueue_ack" || mode == "dropped_enqueue" || mode == "expired_retry"
		if eligible != (dry.Reenqueued == 1) {
			return trace, fmt.Errorf("seed %d case %d mode=%s dry candidates=%d", seed, i, mode, dry.Reenqueued)
		}
		if mode == "lost_enqueue_ack" || mode == "dropped_enqueue" {
			kind := "lose_ack_after_commit"
			if mode == "dropped_enqueue" {
				kind = "drop_before_commit"
			}
			if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
				return trace, err
			}
		}
		_, err = scan.Scan(ctx, sequence, 1, false)
		if mode == "lost_enqueue_ack" || mode == "dropped_enqueue" {
			if !errors.Is(err, ErrTransportLost) {
				return trace, fmt.Errorf("seed %d case %d mode=%s first enqueue: %v", seed, i, mode, err)
			}
			_, err = scan.Scan(ctx, sequence, 1, false)
		}
		if err != nil {
			return trace, fmt.Errorf("seed %d case %d mode=%s repair: %w", seed, i, mode, err)
		}
		want := 0
		if eligible {
			want = 1
		}
		if got := len(model.Runs()) - before; got != want {
			return trace, fmt.Errorf("seed %d case %d mode=%s runs=%d want=%d", seed, i, mode, got, want)
		}
		if mode == "expired_retry" {
			if err := model.Wait(ctx, 3*time.Minute); err != nil {
				return trace, err
			}
			if _, err := scan.Scan(ctx, sequence, 1, false); err != nil {
				return trace, err
			}
			if got := len(model.Runs()) - before; got != 2 {
				return trace, fmt.Errorf("seed %d case %d expired repair retained %d runs", seed, i, got)
			}
			model.SetJournal("test", id, []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: []byte(fmt.Sprintf(`{"sig_seq":%d}`, sequence))}}})
			if _, err := scan.Scan(ctx, sequence, 1, false); err != nil || len(model.Runs())-before != 2 {
				return trace, fmt.Errorf("seed %d case %d consumed repair: err=%v runs=%d", seed, i, err, len(model.Runs())-before)
			}
		}
	}
	report, err := CheckSignalWakeupLiveness(model)
	if err != nil || len(report.Missing) != 0 {
		return trace, fmt.Errorf("seed %d signal liveness: enabled=%d waiting=%v missing=%v err=%v", seed, report.Enabled, report.Waiting, report.Missing, err)
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

func TestSeededSignalRepairReplay(t *testing.T) {
	if os.Getenv("SIM_SIGNAL_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSignalRepairScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SIGNAL_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed := range seededSchedules(t) {
		generated, err := runSeededSignalRepairScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-signal-sim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-signal.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSignalRepairScenario(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d signal repair replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("signal-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSignalRepairReplay$")
		cmd.Env = append(os.Environ(), "SIM_SIGNAL_TRACE_HELPER=1", "SIM_SIGNAL_TRACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("seeded signal child %d: %v: %s", i, err, output)
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
		t.Fatal("seeded signal repair trace changed across processes")
	}
}
