//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go/jetstream"
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
