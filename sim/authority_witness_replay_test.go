package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"js-wf/internal/blobpublication"
)

type witnessModelValue struct {
	Revision, Generation uint64
	Content              string
}

// The physical model records one authority subject and unrelated stream
// sequence churn. It cannot reaffirm a stale coordinate or change logical
// metadata when committing a read witness. No real time or goroutines enter
// its schedules; native envelope/API routing remain separate component gates.
func runAuthorityWitness(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("authority_read_witness"); err != nil {
		return trace, err
	}
	defer func() {
		if runErr == nil {
			runErr = s.Finish()
		}
		trace = s.Trace()
	}()
	modes := []string{"fresh_value", "fresh_absence", "stale_value", "stale_absence", "speculative_value", "persistent_stale", "replacement", "lost_ack", "no_quorum", "snapshot_error", "cancel_before", "cancel_after", "zero_ack", "same_ack"}
	mode, err := s.Choose(modes)
	if err != nil {
		return trace, err
	}
	kind, err := s.Choose([]string{"root", "blob"})
	if err != nil {
		return trace, err
	}
	coord, err := s.Choose([]string{"7", "19", "31"})
	if err != nil {
		return trace, err
	}
	var physical uint64
	switch coord {
	case "7":
		physical = 7
	case "19":
		physical = 19
	case "31":
		physical = 31
	}
	global := physical + 3 // unrelated subject writes separate global and logical sequences
	current := witnessModelValue{Revision: 7, Content: kind + "-current"}
	if kind == "blob" {
		current.Generation = 2
	}
	initial := current
	if mode == "fresh_absence" {
		current = witnessModelValue{}
		physical = 0
		initial = current
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if mode == "cancel_before" {
		cancel()
	}
	reads, writes, commits := 0, 0, 0
	var firstSnapshot witnessModelValue
	var firstSequence uint64
	snapshot := func(ctx context.Context) (witnessModelValue, uint64, error) {
		reads++
		value, seq := current, physical
		if mode == "snapshot_error" {
			s.RecordTransport(TransportEvent{Operation: "authority_snapshot", Subject: kind, Outcome: "error"})
			return witnessModelValue{}, 0, ErrTransportLost
		}
		if reads == 1 || mode == "persistent_stale" {
			switch mode {
			case "stale_value", "persistent_stale":
				value.Revision--
				value.Content = "stale"
				seq--
			case "stale_absence":
				value = witnessModelValue{}
				seq = 0
			case "speculative_value":
				value.Revision += 100
				value.Content = "unconfirmed"
				seq = global + 100
			}
		}
		if reads == 1 {
			firstSnapshot, firstSequence = value, seq
		}
		s.RecordTransport(TransportEvent{Operation: "authority_snapshot", Subject: kind, Sequence: seq, Expected: value.Revision, Outcome: mode})
		if mode == "cancel_after" {
			cancel()
		}
		return value, seq, nil
	}
	witness := func(ctx context.Context, expected uint64, value witnessModelValue) (uint64, error) {
		writes++
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if mode == "replacement" && writes == 1 {
			current.Revision++
			if kind == "blob" {
				current.Generation++
			}
			current.Content = "replacement"
			global += 4
			physical = global
			s.RecordTransport(TransportEvent{Operation: "authority_concurrent_replace", Subject: kind, Sequence: physical, Expected: current.Revision, Outcome: "committed"})
		}
		if mode == "no_quorum" {
			s.RecordTransport(TransportEvent{Operation: "authority_witness", Subject: kind, Expected: expected, Outcome: "no_quorum"})
			return 0, ErrTransportLost
		}
		if expected != physical {
			s.RecordTransport(TransportEvent{Operation: "authority_witness", Subject: kind, Sequence: physical, Expected: expected, Outcome: "conflict"})
			return 0, blobpublication.ErrConflict
		}
		if value != current {
			return 0, errors.New("model attempted to overwrite logical metadata")
		}
		global++
		physical = global
		commits++
		outcome := "acknowledged"
		if mode == "lost_ack" {
			outcome = "committed_reply_lost"
		}
		s.RecordTransport(TransportEvent{Operation: "authority_witness", Subject: kind, Sequence: physical, Expected: expected, Outcome: outcome})
		switch mode {
		case "lost_ack":
			return 0, ErrTransportLost
		case "zero_ack":
			return 0, nil
		case "same_ack":
			return expected, nil
		}
		return physical, nil
	}
	got, confirmed, readErr := blobpublication.ReadWithWitness(ctx, snapshot, witness)
	rejected := mode == "persistent_stale" || mode == "lost_ack" || mode == "no_quorum" || mode == "snapshot_error" || mode == "cancel_before" || mode == "cancel_after" || mode == "zero_ack" || mode == "same_ack"
	if rejected {
		if readErr == nil || got != (witnessModelValue{}) || confirmed != 0 {
			return trace, fmt.Errorf("unconfirmed read escaped mode=%s value=%+v seq=%d err=%v", mode, got, confirmed, readErr)
		}
		switch mode {
		case "persistent_stale":
			if !errors.Is(readErr, blobpublication.ErrConflict) || reads != 16 || writes != 16 || commits != 0 {
				return trace, errors.New("stale retries not bounded")
			}
		case "lost_ack":
			if !errors.Is(readErr, ErrTransportLost) || reads != 1 || writes != 1 || commits != 1 {
				return trace, errors.New("lost witness reply retried or adopted")
			}
		case "no_quorum":
			if !errors.Is(readErr, ErrTransportLost) || commits != 0 {
				return trace, errors.New("minority read committed")
			}
		case "cancel_before":
			if !errors.Is(readErr, context.Canceled) || reads != 0 || writes != 0 {
				return trace, errors.New("canceled read contacted transport")
			}
		case "cancel_after":
			if !errors.Is(readErr, context.Canceled) || reads != 1 || writes != 0 {
				return trace, errors.New("canceled snapshot sent witness")
			}
		case "zero_ack", "same_ack":
			if !errors.Is(readErr, blobpublication.ErrInvalidWitness) || commits != 1 {
				return trace, errors.New("bad acknowledgment accepted")
			}
		case "snapshot_error":
			if writes != 0 {
				return trace, errors.New("failed snapshot sent witness")
			}
		}
	} else {
		if readErr != nil || got != current || confirmed != physical || commits != 1 {
			return trace, fmt.Errorf("confirmed read differs mode=%s got=%+v current=%+v err=%v", mode, got, current, readErr)
		}
		wantReads := 1
		if mode == "stale_value" || mode == "stale_absence" || mode == "speculative_value" || mode == "replacement" {
			wantReads = 2
		}
		if reads != wantReads || writes != wantReads {
			return trace, errors.New("witness did not reload conflict")
		}
	}
	if mode != "replacement" && current != initial {
		return trace, errors.New("read changed logical authority")
	}
	if mode == "stale_value" || mode == "stale_absence" || mode == "speculative_value" || mode == "persistent_stale" {
		// GET-only control would have returned the first unsafe snapshot. Preserve
		// the failure witness independently of the corrected helper's outcome.
		if firstSnapshot == initial {
			return trace, errors.New("GET-only negative control did not expose an unsafe snapshot")
		}
		s.RecordTransport(TransportEvent{Operation: "authority_get_only_control", Subject: kind, Sequence: firstSequence, Expected: initial.Revision, Outcome: "unsafe_without_witness"})
	}
	s.RecordTransport(TransportEvent{Operation: "authority_read_check", Subject: kind, Sequence: confirmed, Expected: uint64(reads), Outcome: mode})
	return trace, nil
}
func TestSeededAuthorityReadWitnessReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		trace, err := runAuthorityWitness(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "witness-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("seed=%d trace=%s: %v", seed, path, err)
		}
		again, err := runAuthorityWitness(seed, &trace)
		if err != nil || !reflect.DeepEqual(trace, again) {
			t.Fatalf("witness replay seed=%d: %v", seed, err)
		}
		mode := trace.Decisions[0].Chosen
		observed[mode]++
		if root := os.Getenv("SIM_AUTHORITY_WITNESS_ROOT"); root != "" && observed[mode] == 1 {
			if err := trace.Save(filepath.Join(root, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != 14 {
		t.Fatal("missing witness modes", observed)
	}
	t.Logf("authority witness modes=%v; every generated trace exactly replayed; production decision helper bounds/cancel/quorum acknowledgment checks", observed)
}
