package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/natsutil"
	"js-wf/journal"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func runJournalSerialRead(seed int64, replay *Trace) (trace Trace, runErr error) {
	return runJournalSerialReadMode(seed, replay, false)
}

func runJournalSerialReadMode(seed int64, replay *Trace, direct bool) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	workload := "journal_serial_read_recovery"
	if direct {
		workload = "journal_direct_unavailable_recovery"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"one_retry", "two_retries", "exhausted", "gap"})
	if err != nil {
		return trace, err
	}
	kinds := []string{"timeout", "no_responders", "unavailable"}
	if direct {
		kinds = []string{"direct_unavailable", "wrapped_direct_unavailable", "unavailable_lookalike"}
	}
	kind, err := schedule.Choose(kinds)
	if err != nil {
		return trace, err
	}
	model := NewJournalTransport(schedule)
	store := journal.NewWithPorts(model, model)
	ctx := context.Background()
	entries := []journal.Entry{{Index: 0, Kind: journal.Started}, {Index: 1, Kind: journal.StepRequested}, {Index: 2, Kind: journal.StepCompleted}, {Index: 3, Kind: journal.Completed}}
	var tail uint64
	for _, entry := range entries {
		tail, err = store.Append(ctx, "test", "serial-read", entry, tail)
		if err != nil {
			return trace, err
		}
	}
	count := 1
	if mode == "two_retries" || mode == "gap" {
		count = 2
	}
	if mode == "exhausted" {
		count = 3
	}
	for i := 0; i < count; i++ {
		if err := model.QueueNextFault(NextReadFault{Sequence: 2, Kind: kind}); err != nil {
			return trace, err
		}
	}
	if mode == "gap" {
		model.DeleteMessage("wf.jrn.test.serial-read", 2)
	}
	records, observedTail, readErr := store.Read(ctx, "test", "serial-read")
	if kind == "unavailable_lookalike" {
		if readErr == nil || natsutil.IsUnavailable(readErr) || len(model.nextFaults) != count-1 || schedule.NowMillis() != 0 {
			return trace, fmt.Errorf("permanent lookalike retried or accepted: err=%v pending=%d time=%d", readErr, len(model.nextFaults), schedule.NowMillis())
		}
	} else {
		switch mode {
		case "exhausted":
			var api *jetstream.APIError
			expected := kind == "timeout" && errors.Is(readErr, context.DeadlineExceeded) || kind == "no_responders" && errors.Is(readErr, nats.ErrNoResponders) || kind == "unavailable" && errors.As(readErr, &api) && api.ErrorCode == 10008
			if direct {
				expected = natsutil.IsUnavailable(readErr) && schedule.NowMillis() == 50 && len(model.nextFaults) == 0
			}
			if !expected || len(records) != 0 || observedTail != 0 {
				return trace, fmt.Errorf("exhaustion result=%v", readErr)
			}
		case "gap":
			if !errors.Is(readErr, journal.ErrGap) {
				return trace, fmt.Errorf("gap result=%v", readErr)
			}
		default:
			if readErr != nil || observedTail != tail || len(records) != len(entries) {
				return trace, fmt.Errorf("read count=%d tail=%d err=%v", len(records), observedTail, readErr)
			}
			for i, record := range records {
				got, _ := json.Marshal(record.Entry)
				want, _ := json.Marshal(entries[i])
				if !bytes.Equal(got, want) || record.Sequence != uint64(i+1) {
					return trace, fmt.Errorf("record %d changed", i)
				}
			}
		}
	}
	if schedule.NowMillis() > 6100 {
		return trace, fmt.Errorf("serial read retries took %d virtual milliseconds", schedule.NowMillis())
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_serial_read", Subject: "wf.jrn.test.serial-read", Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededJournalSerialReadReplay(t *testing.T) {
	if path := os.Getenv("SIM_JOURNAL_SERIAL_READ_OUT"); path != "" {
		trace, err := runJournalSerialRead(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	kinds := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runJournalSerialRead(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "journal-serial-read-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		kinds[generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runJournalSerialRead(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 4 || len(kinds) != 3 {
		t.Fatalf("serial read mode coverage: %v", modes)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "journal-serial-read.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededJournalSerialReadReplay$")
		cmd.Env = append(os.Environ(), "SIM_JOURNAL_SERIAL_READ_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, data) {
			t.Fatal("serial read trace changed across processes")
		}
		previous = data
	}
}

func TestSeededJournalDirectUnavailableReplay(t *testing.T) {
	if path := os.Getenv("SIM_JOURNAL_DIRECT_READ_OUT"); path != "" {
		seed := int64(42)
		if raw := os.Getenv("FAULT_SEED"); raw != "" {
			var err error
			seed, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		trace, err := runJournalSerialReadMode(seed, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	kinds := map[string]bool{}
	combinations := map[string]bool{}
	for seed := range seededSchedules(t) {
		generated, err := runJournalSerialReadMode(seed, nil, true)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "journal-serial-read-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		kinds[generated.Decisions[1].Chosen] = true
		combinations[generated.Decisions[0].Chosen+"/"+generated.Decisions[1].Chosen] = true
		if seed <= 10 {
			replayed, err := runJournalSerialReadMode(seed, &generated, true)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 4 || len(kinds) != 3 || len(combinations) != 12 {
		t.Fatalf("direct read mode coverage: %v", combinations)
	}
	var previous []byte
	for i := 0; i < 2; i++ {
		path := filepath.Join(t.TempDir(), "journal-serial-read.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededJournalDirectUnavailableReplay$")
		cmd.Env = append(os.Environ(), "SIM_JOURNAL_DIRECT_READ_OUT="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 && !bytes.Equal(previous, data) {
			t.Fatal("serial read trace changed across processes")
		}
		previous = data
	}
}
