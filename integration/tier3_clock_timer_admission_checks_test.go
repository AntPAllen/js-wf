//go:build linux

package integration_test

import (
	"encoding/json"
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
		for _, mode := range []string{"valid", "late", "completed", "wrong_wait", "wrong_owner", "unknown_clock", "unshifted_clock", "ambiguous", "receipt_not_observed"} {
			t.Run(offset.String()+"/"+mode, func(t *testing.T) {
				rs := append([]matrixControllerJournalReceipt(nil), receipts...)
				op := origin
				ops := []worker.OperationEvent{op}
				now := at.Add(30 * time.Millisecond)
				switch mode {
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
				selected, err := selectMatrixPendingClockTimer(rs, ops, now, offset, 100*time.Millisecond)
				if mode == "valid" {
					if err != nil || selected == nil || selected.ID != "inv" || !selected.EarliestDue.Equal(at.Add(250*time.Millisecond)) {
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
