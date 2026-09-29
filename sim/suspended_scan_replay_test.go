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

func suspendedFixture(sequence uint64, mode string, fireAt time.Time, signalSeq uint64) ([]journal.Record, error) {
	add := func(records []journal.Record, kind journal.Kind, payload []byte) []journal.Record {
		index := uint64(len(records))
		return append(records, journal.Record{Entry: journal.Entry{Kind: kind, Index: index, Payload: payload}, Sequence: sequence*10 + index + 1})
	}
	records := add(nil, journal.Started, nil)
	if mode == "terminal" {
		return add(records, journal.Completed, []byte(`null`)), nil
	}
	if mode == "no_journal" {
		return nil, nil
	}
	request := struct {
		Kind      string    `json:"kind"`
		Name      string    `json:"name,omitempty"`
		TimerName string    `json:"timer_name,omitempty"`
		FireAt    time.Time `json:"fire_at,omitempty"`
	}{Kind: "signal", Name: "go"}
	waitingOn := "signal:go"
	switch mode {
	case "due_timer", "future_timer", "lost_ack", "drop_enqueue":
		request.Kind, request.Name, request.FireAt = "timer", "sleep", fireAt
		waitingOn = "timer:sleep"
	case "due_select", "signal_select":
		request.Kind, request.Name, request.TimerName, request.FireAt = "timer_signal_select", "go", "sleep", fireAt
		waitingOn = "select:sleep:go"
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	records = add(records, journal.StepRequested, payload)
	if mode == "consumed_unused" || mode == "consumed_used" {
		records = add(records, journal.SignalConsumed, []byte(fmt.Sprintf(`{"sig_seq":%d,"name":"go"}`, signalSeq)))
	}
	if mode == "consumed_used" {
		records = add(records, journal.StepCompleted, []byte(fmt.Sprintf(`{"signal_seq":%d}`, signalSeq)))
		records = add(records, journal.StepRequested, payload)
	}
	records = add(records, journal.Suspended, []byte(fmt.Sprintf(`{"waiting_on":%q}`, waitingOn)))
	return records, nil
}

func runSeededSuspendedScan(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("suspended_scan_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewSignalTransport(schedule)
	scan := reconcile.NewSuspendedScanWithPort(model)
	base := time.Unix(1_700_000_000, 0).UTC()
	scan.Now = func() time.Time { return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond) }
	var future []uint64
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"due_timer", "future_timer", "due_select", "matching_signal", "signal_select", "stale_then_matching", "consumed_unused", "consumed_used", "unrelated_signal", "terminal", "no_journal", "hole", "lost_ack", "drop_enqueue"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("suspended-%02d", i)
		sequence, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
		if err != nil {
			return trace, err
		}
		signal := func(name string, generation uint64) uint64 {
			message := &nats.Msg{Subject: "wf.sig.test." + id + "." + name, Data: []byte("payload"), Header: nats.Header{}}
			message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
			return model.CommitSignal(message)
		}
		var signalSeq uint64
		switch mode {
		case "matching_signal", "signal_select", "consumed_unused", "consumed_used":
			signalSeq = signal("go", sequence)
		case "stale_then_matching":
			signal("go", sequence+1000)
			signalSeq = signal("go", sequence)
		case "unrelated_signal":
			signal("other", sequence)
		}
		fireAt := base.Add(-2 * time.Second)
		if mode == "future_timer" || mode == "signal_select" {
			fireAt = base.Add(2 * time.Second)
		}
		records, err := suspendedFixture(sequence, mode, fireAt, signalSeq)
		if err != nil {
			return trace, err
		}
		if mode != "no_journal" {
			model.SetJournal("test", id, records)
		}
		if mode == "hole" {
			model.PurgeInvocation(identity.InvocationSubject("test", id))
		}
		eligible := mode == "due_timer" || mode == "due_select" || mode == "matching_signal" || mode == "signal_select" || mode == "stale_then_matching" || mode == "consumed_unused" || mode == "lost_ack" || mode == "drop_enqueue"
		before := len(model.Runs())
		dry, err := scan.Scan(ctx, sequence, 1, true)
		if err != nil || len(model.Runs()) != before || (dry.Reenqueued == 1) != eligible {
			return trace, fmt.Errorf("seed %d case %d mode=%s dry=%+v runs=%d/%d err=%v", seed, i, mode, dry, len(model.Runs()), before, err)
		}
		if mode == "future_timer" {
			future = append(future, sequence)
		}
		if mode == "lost_ack" || mode == "drop_enqueue" {
			kind := "lose_ack_after_commit"
			if mode == "drop_enqueue" {
				kind = "drop_before_commit"
			}
			if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: kind}); err != nil {
				return trace, err
			}
			if result, err := scan.Scan(ctx, sequence, 1, false); !errors.Is(err, ErrTransportLost) || result.Reenqueued != 1 {
				return trace, fmt.Errorf("seed %d case %d mode=%s lost enqueue result=%+v err=%v", seed, i, mode, result, err)
			}
		}
		result, err := scan.Scan(ctx, sequence, 1, false)
		want := before
		if eligible {
			want++
		}
		if err != nil || (result.Reenqueued == 1) != eligible || len(model.Runs()) != want {
			return trace, fmt.Errorf("seed %d case %d mode=%s result=%+v runs=%d want=%d err=%v", seed, i, mode, result, len(model.Runs()), want, err)
		}
		if eligible {
			if _, err := scan.Scan(ctx, sequence, 1, false); err != nil || len(model.Runs()) != want {
				return trace, fmt.Errorf("seed %d case %d mode=%s dedup runs=%d want=%d err=%v", seed, i, mode, len(model.Runs()), want, err)
			}
		}
	}
	if err := model.Wait(ctx, 4*time.Second); err != nil {
		return trace, err
	}
	for _, sequence := range future {
		before := len(model.Runs())
		result, err := scan.Scan(ctx, sequence, 1, false)
		if err != nil || result.Reenqueued != 1 || len(model.Runs()) != before+1 {
			return trace, fmt.Errorf("seed %d future %d result=%+v runs=%d/%d err=%v", seed, sequence, result, len(model.Runs()), before, err)
		}
	}
	report, err := CheckSuspendedWakeupLiveness(model, scan.Now(), scan.Grace)
	if err != nil || len(report.Missing) != 0 {
		return trace, fmt.Errorf("seed %d suspended liveness: enabled=%d waiting=%v missing=%v err=%v", seed, report.Enabled, report.Waiting, report.Missing, err)
	}
	waiting, err := json.Marshal(report.Waiting)
	if err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_suspended_liveness", Sequence: uint64(report.Enabled), DataSHA256: digest(waiting), Outcome: "ok", AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSuspendedScanReplay(t *testing.T) {
	if os.Getenv("SIM_SUSPENDED_SCAN_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededSuspendedScan(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_SUSPENDED_SCAN_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededSuspendedScan(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-suspended-scan-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-suspended-scan.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededSuspendedScan(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("suspended-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSuspendedScanReplay$")
		cmd.Env = append(os.Environ(), "SIM_SUSPENDED_SCAN_HELPER=1", "SIM_SUSPENDED_SCAN_OUT="+files[i], "FAULT_SEED=42")
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
	if !bytes.Equal(first, second) {
		t.Fatal("suspended scan trace changed across processes")
	}
}
