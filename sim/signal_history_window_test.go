package sim

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"

	"github.com/anishathalye/porcupine"
)

func runSignalHistoryWindow(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("signal_history_duplicate_windows"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"distinct_keys", "distinct_invocations", "distinct_names", "distinct_generations", "reverse_order", "expired_retry"})
	if err != nil {
		return trace, err
	}
	type signalArgs struct {
		Type       string `json:"type"`
		ID         string `json:"id"`
		Name       string `json:"name"`
		Hash       string `json:"payload_hash"`
		Key        string `json:"idempotency_key"`
		Generation uint64 `json:"inv_seq"`
	}
	first := signalArgs{"test", "one", "go", "hash", "key-one", 7}
	second := first
	sequence := uint64(12)
	want := porcupine.Ok
	switch mode {
	case "distinct_keys":
		second.Key = "key-two"
	case "distinct_invocations":
		second.ID = "two"
	case "distinct_names":
		second.Name = "another"
	case "distinct_generations":
		second.Generation++
	case "reverse_order":
		second.Key, sequence, want = "key-two", 9, porcupine.Illegal
	case "expired_retry":
		sequence, want = 10, porcupine.Unknown
	}
	operation := func(args signalArgs, call, returned int64, seq uint64) client.Operation {
		input, _ := json.Marshal(args)
		output, _ := json.Marshal(struct {
			Status   string `json:"status"`
			Sequence uint64 `json:"signal_seq"`
		}{"signaled", seq})
		return client.Operation{Op: "signal", Args: input, Result: output, InvokeTS: time.UnixMilli(call).UTC(), ReturnTS: time.UnixMilli(returned).UTC()}
	}
	ops := []client.Operation{operation(first, 0, 1, 10), operation(second, 180000, 180001, sequence)}
	if err := schedule.AdvanceMillis(180001); err != nil {
		return trace, err
	}
	result, checkErr := history.CheckSignals(ops, time.Second)
	if result != want || (checkErr != nil) != (want == porcupine.Unknown) {
		return trace, fmt.Errorf("mode=%s signal history=%s want=%s err=%v", mode, result, want, checkErr)
	}
	if want == porcupine.Unknown && !strings.Contains(checkErr.Error(), "configured duplicate window") {
		return trace, fmt.Errorf("unexpected refusal reason: %w", checkErr)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_signal_history_window", Subject: mode, Outcome: fmt.Sprint(result), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSignalHistoryWindowReplay(t *testing.T) {
	if path := os.Getenv("SIM_SIGNAL_HISTORY_WINDOW_OUT"); path != "" {
		trace, err := runSignalHistoryWindow(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSignalHistoryWindow(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "signal-history-window-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runSignalHistoryWindow(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 6 {
		t.Fatalf("signal window mode coverage: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "signal-history-window.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededSignalHistoryWindowReplay$")
		cmd.Env = append(os.Environ(), "SIM_SIGNAL_HISTORY_WINDOW_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, data) {
			t.Fatal("signal window trace changed across processes")
		}
		previous = data
	}
}
