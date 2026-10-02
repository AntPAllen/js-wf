//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go/jetstream"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/worker"
)

func TestMatrixPendingClockTimerAdmissionRejectsStaleAndUnprovenWaits(t *testing.T) {
	at := time.Date(2026, 10, 1, 23, 18, 57, 0, time.UTC)
	for _, offset := range []time.Duration{-time.Minute, time.Minute} {
		server := at.Add(offset)
		payload, _ := json.Marshal(map[string]any{"kind": "timer", "name": "timer-5", "duration_nanos": int64(250 * time.Millisecond), "fire_at": server.Add(250 * time.Millisecond)})
		receipts := []matrixControllerJournalReceipt{
			{Sequence: 11, Subject: "wf.jrn.matrixtimer.inv", Entry: journal.Entry{Index: 16, Kind: journal.StepRequested, WorkerID: "worker", Payload: payload}, ObservedAt: at.Add(10 * time.Millisecond)},
			{Sequence: 12, Subject: "wf.jrn.matrixtimer.inv", Entry: journal.Entry{Index: 17, Kind: journal.Suspended, WorkerID: "worker", Payload: json.RawMessage(`{"waiting_on":"timer:timer-5"}`)}, ObservedAt: at.Add(20 * time.Millisecond)},
		}
		origin := worker.OperationEvent{Type: "matrixtimer", ID: "inv", Worker: "worker", JournalIndex: 16, JournalKind: journal.StepRequested, Operation: "timer_clock", At: at.Add(time.Millisecond), Duration: time.Millisecond, ServerTime: &server}
		for _, mode := range []string{"valid", "short_for_docker", "long_for_docker", "late", "completed", "wrong_wait", "wrong_owner", "unknown_clock", "unshifted_clock", "ambiguous", "receipt_not_observed"} {
			t.Run(offset.String()+"/"+mode, func(t *testing.T) {
				rs := append([]matrixControllerJournalReceipt(nil), receipts...)
				op := origin
				ops := []worker.OperationEvent{op}
				now := at.Add(30 * time.Millisecond)
				lead := 100 * time.Millisecond
				duration := 250 * time.Millisecond
				switch mode {
				case "short_for_docker":
					lead = 750 * time.Millisecond
				case "long_for_docker":
					lead = 750 * time.Millisecond
					duration = 2 * time.Second
					var p map[string]any
					_ = json.Unmarshal(payload, &p)
					p["duration_nanos"] = int64(duration)
					p["fire_at"] = server.Add(duration)
					rs[0].Entry.Payload, _ = json.Marshal(p)
				case "late":
					now = at.Add(200 * time.Millisecond)
				case "completed":
					rs = append(rs, matrixControllerJournalReceipt{Sequence: 13, Subject: rs[0].Subject, Entry: journal.Entry{Index: 18, Kind: journal.StepCompleted}, ObservedAt: now})
				case "wrong_wait":
					rs[1].Entry.Payload = json.RawMessage(`{"waiting_on":"signal:go"}`)
				case "wrong_owner":
					ops[0].Worker = "other"
				case "unknown_clock":
					ops[0].Error = "unknown"
				case "unshifted_clock":
					bad := at
					ops[0].ServerTime = &bad
					var p map[string]any
					_ = json.Unmarshal(payload, &p)
					p["fire_at"] = at.Add(250 * time.Millisecond)
					rs[0].Entry.Payload, _ = json.Marshal(p)
				case "ambiguous":
					ops = append(ops, op)
				case "receipt_not_observed":
					rs[1].ObservedAt = now.Add(time.Second)
				}
				selected, err := selectMatrixPendingClockTimer(rs, ops, now, offset, lead)
				if mode == "valid" || mode == "long_for_docker" {
					if err != nil || selected == nil || selected.ID != "inv" || !selected.EarliestDue.Equal(at.Add(duration)) {
						t.Fatalf("selected=%+v err=%v", selected, err)
					}
				} else if mode == "ambiguous" {
					if err == nil {
						t.Fatal("ambiguous origin admitted")
					}
				} else if err != nil || selected != nil {
					t.Fatalf("unproven candidate admitted: %+v err=%v", selected, err)
				}
			})
		}
	}
}

// Embed the full API interface while replacing only the retained-tail read.
// Every other API operation would fail if accidentally used by this helper.
type matrixClockTailReadStub struct {
	jetstream.Stream
	tail *jetstream.RawStreamMsg
	err  error
}

func (s matrixClockTailReadStub) GetLastMsgForSubject(context.Context, string) (*jetstream.RawStreamMsg, error) {
	return s.tail, s.err
}

func TestMatrixClockTimerRefreshRejectsAdvancedOrLateTail(t *testing.T) {
	for _, mode := range []string{"valid", "advanced_sequence", "changed_entry", "late", "corrupt", "transport_error"} {
		t.Run(mode, func(t *testing.T) {
			entry := journal.Entry{Index: 17, Kind: journal.Suspended, WorkerID: "worker", Payload: json.RawMessage(`{"waiting_on":"timer:wait"}`)}
			receipt := matrixControllerJournalReceipt{Sequence: 12, Subject: "wf.jrn.matrixtimer.inv", Entry: entry}
			candidate := &matrixClockTimerAdmission{Suspended: receipt, EarliestDue: time.Now().Add(time.Second)}
			data, _ := json.Marshal(entry)
			tail := &jetstream.RawStreamMsg{Sequence: 12, Subject: receipt.Subject, Data: data}
			stub := matrixClockTailReadStub{tail: tail}
			switch mode {
			case "advanced_sequence":
				tail.Sequence = 13
			case "changed_entry":
				entry.Kind = journal.Completed
				tail.Data, _ = json.Marshal(entry)
			case "late":
				candidate.EarliestDue = time.Now()
			case "corrupt":
				tail.Data = []byte("invalid")
			case "transport_error":
				stub.err = context.DeadlineExceeded
			}
			selected, err := refreshMatrixClockTimerCandidate(context.Background(), stub, candidate)
			if mode == "valid" {
				if err != nil || selected == nil {
					t.Fatalf("selected=%+v err=%v", selected, err)
				}
			} else if mode == "corrupt" || mode == "transport_error" {
				if err == nil {
					t.Fatal("unreadable tail admitted")
				}
			} else if err != nil || selected != nil {
				t.Fatalf("stale tail admitted: %+v err=%v", selected, err)
			}
		})
	}
}

func TestMatrixCanonicalTimerAdmissionRequiresShiftedAcknowledgedHint(t *testing.T) {
	at := time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC)
	for _, offset := range []time.Duration{-time.Minute, time.Minute} {
		for _, mode := range []string{"valid", "missing_hint", "wrong_domain", "wrong_bounds", "wrong_deadline", "unshifted_hint", "wrong_translation", "duplicate_ack", "unknown_ack", "ambiguous_hint", "ambiguous_origin", "late_hint", "wrong_owner"} {
			t.Run(offset.String()+"/"+mode, func(t *testing.T) {
				lower, upper := at.Add(-10*time.Millisecond), at.Add(20*time.Millisecond)
				fire := upper.Add(2 * time.Second)
				payload, _ := json.Marshal(map[string]any{"kind": "timer", "name": "wait", "duration_nanos": int64(2 * time.Second), "fire_at": fire, "clock_domain": "utc-quorum-v1"})
				receipts := []matrixControllerJournalReceipt{
					{Sequence: 11, Subject: "wf.jrn.matrixtimer.inv", Entry: journal.Entry{Index: 16, Kind: journal.StepRequested, WorkerID: "worker", Payload: payload}, ObservedAt: at.Add(40 * time.Millisecond)},
					{Sequence: 12, Subject: "wf.jrn.matrixtimer.inv", Entry: journal.Entry{Index: 17, Kind: journal.Suspended, WorkerID: "worker", Payload: json.RawMessage(`{"waiting_on":"timer:wait"}`)}, ObservedAt: at.Add(50 * time.Millisecond)},
				}
				origin := worker.OperationEvent{Type: "matrixtimer", ID: "inv", Worker: "worker", JournalIndex: 16, Operation: "timer_domain_clock", At: at.Add(time.Millisecond), Duration: time.Millisecond, ClockDomain: "utc-quorum-v1", ClockLower: &lower, ClockUpper: &upper}
				physical := at.Add(offset)
				schedule := physical.Add(fire.Sub(lower))
				published := true
				hint := worker.OperationEvent{Type: "matrixtimer", ID: "inv", Worker: "worker", JournalIndex: 16, JournalKind: journal.StepRequested, Operation: "timer_native_hint", At: at.Add(30 * time.Millisecond), Duration: 20 * time.Millisecond, ClockDomain: "utc-quorum-v1", ServerTime: &physical, ClockLower: &lower, ClockUpper: &upper, TimerDeadline: &fire, TimerScheduleAt: &schedule, TimerPublished: &published}
				switch mode {
				case "wrong_domain":
					origin.ClockDomain = "unknown"
				case "wrong_bounds":
					bad := upper.Add(time.Minute)
					origin.ClockUpper = &bad
				case "wrong_deadline":
					bad := fire.Add(time.Second)
					hint.TimerDeadline = &bad
				case "unshifted_hint":
					physical = at
					schedule = physical.Add(fire.Sub(lower))
				case "wrong_translation":
					schedule = schedule.Add(time.Second)
				case "duplicate_ack":
					published = false
				case "unknown_ack":
					hint.Error = "outcome unknown"
				case "late_hint":
					hint.At = at.Add(time.Second)
				case "wrong_owner":
					hint.Worker = "other"
				}
				ops := []worker.OperationEvent{origin, hint}
				if mode == "missing_hint" {
					ops = ops[:1]
				}
				if mode == "ambiguous_hint" {
					ops = append(ops, hint)
				}
				if mode == "ambiguous_origin" {
					ops = append(ops, origin)
				}
				selected, err := selectMatrixPendingClockTimer(receipts, ops, at.Add(60*time.Millisecond), offset, 750*time.Millisecond)
				if mode == "valid" {
					if err != nil || selected == nil || selected.NativeHint == nil || !selected.EarliestDue.Equal(at.Add(2*time.Second)) {
						t.Fatalf("selected=%+v err=%v", selected, err)
					}
				} else if mode == "ambiguous_hint" || mode == "ambiguous_origin" {
					if err == nil {
						t.Fatal("ambiguous proof accepted")
					}
				} else if err != nil || selected != nil {
					t.Fatalf("invalid proof accepted: %+v %v", selected, err)
				}
			})
		}
	}
}

// Virtual transport receipts exercise the real admission selector. The initial
// hint comes from a healthy node just before leader preference; the next request
// arrives six seconds later after repair/takeover, as in the retained failure.
// Later waits must offer the same removal interval, rather than assuming the
// first request will always be created after the fault controller moves leaders.
func TestMatrixClockAdmissionAfterHealthyFirstHint(t *testing.T) {
	at := time.Date(2026, 10, 2, 5, 44, 58, 0, time.UTC)
	for _, offset := range []time.Duration{-time.Minute, time.Minute} {
		for seed := int64(1); seed <= 256; seed++ {
			rng := rand.New(rand.NewSource(seed))
			delay := time.Duration(20+rng.Intn(180)) * time.Millisecond
			build := func(start time.Time, duration, timeShift time.Duration, index uint64) ([]matrixControllerJournalReceipt, []worker.OperationEvent) {
				lower, upper := start.Add(-10*time.Millisecond), start.Add(20*time.Millisecond)
				fire := upper.Add(duration)
				payload, _ := json.Marshal(map[string]any{"kind": "timer", "name": "wait", "duration_nanos": int64(duration), "fire_at": fire, "clock_domain": "utc-quorum-v1"})
				receipts := []matrixControllerJournalReceipt{
					{Sequence: index + 1, Subject: "wf.jrn.matrixtimer.inv", Entry: journal.Entry{Index: index, Kind: journal.StepRequested, WorkerID: "worker", Payload: payload}, ObservedAt: start.Add(delay)},
					{Sequence: index + 2, Subject: "wf.jrn.matrixtimer.inv", Entry: journal.Entry{Index: index + 1, Kind: journal.Suspended, WorkerID: "worker", Payload: json.RawMessage(`{"waiting_on":"timer:wait"}`)}, ObservedAt: start.Add(delay + time.Millisecond)},
				}
				physical := start.Add(timeShift)
				schedule := physical.Add(fire.Sub(lower))
				published := true
				ops := []worker.OperationEvent{
					{Type: "matrixtimer", ID: "inv", Worker: "worker", JournalIndex: index, Operation: "timer_domain_clock", At: start.Add(time.Millisecond), Duration: time.Millisecond, ClockDomain: "utc-quorum-v1", ClockLower: &lower, ClockUpper: &upper},
					{Type: "matrixtimer", ID: "inv", Worker: "worker", JournalIndex: index, JournalKind: journal.StepRequested, Operation: "timer_native_hint", At: start.Add(delay), Duration: delay - time.Millisecond, ClockDomain: "utc-quorum-v1", ServerTime: &physical, ClockLower: &lower, ClockUpper: &upper, TimerDeadline: &fire, TimerScheduleAt: &schedule, TimerPublished: &published},
				}
				return receipts, ops
			}
			rs, ops := build(at.Add(-500*time.Millisecond), 2*time.Second, 0, 1)
			got, err := selectMatrixPendingClockTimer(rs, ops, at, offset, 750*time.Millisecond)
			if err != nil || got != nil {
				t.Fatalf("seed%d healthy initial hint admitted: %+v %v", seed, got, err)
			}
			for _, oldProfile := range []bool{true, false} {
				admitted := false
				start := at.Add(6 * time.Second)
				for step := 1; step < 8 && start.Before(at.Add(10*time.Second)); step++ {
					duration := matrixClockTimerWait(true)
					if oldProfile {
						duration = 250 * time.Millisecond
					}
					rs, ops = build(start, duration, offset, uint64(step*4+1))
					now := start.Add(delay + 2*time.Millisecond)
					candidate, err := selectMatrixPendingClockTimer(rs, ops, now, offset, 750*time.Millisecond)
					if err != nil {
						t.Fatal(err)
					}
					if candidate != nil {
						admitted = true
						break
					}
					start = start.Add(duration + delay + time.Duration(rng.Intn(200))*time.Millisecond)
				}
				if admitted == oldProfile {
					t.Fatalf("seed%d offset%s oldProfile%v admitted%v", seed, offset, oldProfile, admitted)
				}
			}
		}
	}
}

// Optional replay of the original failed native artifact, preserving all
// receipt/operation timestamps. It makes no synthetic changes to native data.
func TestMatrixClockAdmissionNativeBatch32Replay(t *testing.T) {
	root := os.Getenv("WF_CLOCK_ADMISSION_REPLAY_ROOT")
	if root == "" {
		t.Skip("requires retained failed-native artifact")
	}
	load := func(name string, dst any) {
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, dst); err != nil {
			t.Fatal(err)
		}
	}
	var receipts []matrixControllerJournalReceipt
	var operations []worker.OperationEvent
	load("controller-receipts.json", &receipts)
	load("controller-operations.json", &operations)
	var selectedReceipts []matrixControllerJournalReceipt
	var selectedOperations []worker.OperationEvent
	for _, r := range receipts {
		if strings.HasPrefix(r.Subject, "wf.jrn.matrixtimer.tier3-1-batch-32-") {
			selectedReceipts = append(selectedReceipts, r)
		}
	}
	for _, op := range operations {
		if op.Type == "matrixtimer" && strings.HasPrefix(op.ID, "tier3-1-batch-32-") {
			selectedOperations = append(selectedOperations, op)
		}
	}
	if len(selectedReceipts) == 0 || len(selectedOperations) == 0 {
		t.Fatal("missing batch32 native evidence")
	}
	start := time.Date(2026, 10, 2, 5, 44, 58, 188530631, time.UTC)
	checked := 0
	for now := start; now.Before(start.Add(10 * time.Second)); now = now.Add(5 * time.Millisecond) {
		var rs []matrixControllerJournalReceipt
		var ops []worker.OperationEvent
		for _, r := range selectedReceipts {
			if !r.ObservedAt.After(now) {
				rs = append(rs, r)
			}
		}
		for _, op := range selectedOperations {
			if !op.At.After(now) {
				ops = append(ops, op)
			}
		}
		got, err := selectMatrixPendingClockTimer(rs, ops, now, -time.Minute, 750*time.Millisecond)
		if err != nil || got != nil {
			t.Fatalf("native replay at%s got%+v err%v", now, got, err)
		}
		checked++
	}
	t.Logf("native batch32: %d observed receipts, %d operations, %d snapshots; no provable pending timer with750ms lead", len(selectedReceipts), len(selectedOperations), checked)
}
