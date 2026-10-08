package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
)

// Read requests within each immutable committed cohort can run concurrently.
// Only ordered accepted reads enter the trace; no scheduler is touched from
// the point scanner's goroutines. Metadata may lag those committed bytes.
type cohortReadModel struct {
	schedule              *Scheduler
	reported, truth       uint64
	empty, wrong, missing bool
}

func (m *cohortReadModel) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	first := uint64(1)
	if m.empty {
		first = 0
	}
	m.schedule.RecordTransport(TransportEvent{Operation: "retained_cohort_metadata", Sequence: m.reported, Outcome: "snapshot"})
	return &jetstream.StreamInfo{State: jetstream.StreamState{FirstSeq: first, LastSeq: m.reported}}, nil
}
func (m *cohortReadModel) GetMsg(ctx context.Context, seq uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if seq > m.truth || m.missing && seq == m.truth {
		return nil, jetstream.ErrMsgNotFound
	}
	actual := seq
	if m.wrong && seq == m.truth {
		actual--
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.inv.audit.id-%d", seq), Sequence: actual, Data: []byte("committed")}, nil
}
func runRetainedCohort(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("retained_cohort_boundary"); err != nil {
		return trace, err
	}
	defer func() {
		if runErr == nil {
			runErr = s.Finish()
		}
		trace = s.Trace()
	}()
	modes := []string{"metadata_behind", "metadata_empty", "metadata_ahead", "wrong_coordinate", "missing_tail", "invalid_tail_subject", "leader_behind_ack", "canceled"}
	if seed == 31 {
		modes = []string{"metadata_behind"}
	}
	mode, err := s.Choose(modes)
	if err != nil {
		return trace, err
	}
	truth, reported, minimum := uint64(8), uint64(4), uint64(5)
	// Preserve the observed native cut/ack/cohort tuple in a saved seeded case.
	if seed == 31 {
		truth, reported, minimum = 840, 812, 822
	}
	m := &cohortReadModel{schedule: s, truth: truth, reported: reported, empty: mode == "metadata_empty", wrong: mode == "wrong_coordinate", missing: mode == "missing_tail"}
	if m.empty {
		m.reported = 0
	}
	if mode == "metadata_ahead" {
		m.reported = truth + 3
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if mode == "canceled" {
		cancel()
	}
	cutoff, err := integrity.CaptureInvocationCutoff(ctx, minimum, func(context.Context) (*jetstream.RawStreamMsg, error) {
		subject := "wf.inv.audit.last"
		head := truth
		if mode == "invalid_tail_subject" {
			subject = "wf.jrn.audit.last"
		}
		if mode == "leader_behind_ack" {
			head = minimum - 1
		}
		s.RecordTransport(TransportEvent{Operation: "invocation_cohort_leader_tail", Subject: subject, Sequence: head, Expected: minimum, Outcome: mode})
		return &jetstream.RawStreamMsg{Subject: subject, Sequence: head}, nil
	})
	if mode == "invalid_tail_subject" || mode == "leader_behind_ack" || mode == "canceled" {
		if err == nil {
			return trace, errors.New("invalid cohort capture accepted")
		}
		s.RecordTransport(TransportEvent{Operation: "check_retained_cohort", Outcome: mode + "_rejected"})
		return trace, nil
	}
	if err != nil || cutoff != truth {
		return trace, fmt.Errorf("captured cutoff=%d: %v", cutoff, err)
	}
	var visited []uint64
	err = integrity.ScanRetainedPointReadsThrough(ctx, m, cutoff, func(raw *jetstream.RawStreamMsg) error {
		visited = append(visited, raw.Sequence)
		s.RecordTransport(TransportEvent{Operation: "retained_cohort_accept", Subject: raw.Subject, Sequence: raw.Sequence, Outcome: "committed"})
		return nil
	})
	if mode == "wrong_coordinate" {
		if err == nil {
			return trace, errors.New("incorrect response coordinate accepted")
		}
	} else if mode == "missing_tail" {
		if err != nil || len(visited) != int(truth-1) {
			return trace, fmt.Errorf("missing tail control=%d err=%v", len(visited), err)
		}
		// The surrounding matrix retains its strict cohort count gate.
		if integrity.ValidateCompletedCohortReport(integrity.Report{Invocations: len(visited), Journals: len(visited), Terminal: len(visited)}, int(truth)) == nil {
			return trace, errors.New("missing invocation passed count gate")
		}
	} else {
		if err != nil || len(visited) != int(truth) {
			return trace, fmt.Errorf("metadata truncated captured cohort count=%d want=%d err=%v", len(visited), truth, err)
		}
		for i, seq := range visited {
			if seq != uint64(i+1) {
				return trace, errors.New("ordered cohort differs")
			}
		}
		if mode == "metadata_behind" || mode == "metadata_empty" {
			// The old clamp/capture policy produces a strictly smaller report, exactly
			// the mismatch that failed the native campaign, without a missing start.
			if m.reported >= cutoff {
				return trace, errors.New("old-policy failure control absent")
			}
			s.RecordTransport(TransportEvent{Operation: "old_cohort_boundary_control", Sequence: m.reported, Expected: cutoff, Outcome: "count_mismatch"})
		}
	}
	s.RecordTransport(TransportEvent{Operation: "check_retained_cohort", Sequence: uint64(len(visited)), Expected: cutoff, Outcome: mode})
	return trace, nil
}
func TestSeededRetainedCohortBoundaryReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		trace, err := runRetainedCohort(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "cohort-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("seed=%d trace=%s: %v", seed, path, err)
		}
		again, err := runRetainedCohort(seed, &trace)
		if err != nil || !reflect.DeepEqual(trace, again) {
			t.Fatalf("cohort replay seed=%d: %v", seed, err)
		}
		mode := trace.Decisions[0].Chosen
		observed[mode]++
		if root := os.Getenv("SIM_COHORT_BOUNDARY_ROOT"); root != "" && (observed[mode] == 1 || seed == 31) {
			name := mode
			if seed == 31 {
				name = "native-seed31"
			}
			if err = trace.Save(filepath.Join(root, name+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != 8 {
		t.Fatal("missing cohort modes", observed)
	}
	t.Logf("retained cohort: modes=%v; every generated trace exactly replayed; stale cut and missing/corrupt controls detected", observed)
}
