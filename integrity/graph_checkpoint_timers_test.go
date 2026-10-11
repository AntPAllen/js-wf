package integrity

import (
	"encoding/json"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"testing"
)

func timerHistoryStep(s *auditedGraphJournal, request, completion string) error {
	s.journal.request = json.RawMessage(request)
	s.sdkPosition++
	err := observeSDKTimerOperation(s, journal.Entry{Kind: journal.StepCompleted, Payload: json.RawMessage(completion)})
	s.sdkPosition++
	return err
}

func TestRawGraphCheckpointSDKTimerHistory(t *testing.T) {
	t.Run("absolute-position-and-deadline", func(t *testing.T) {
		s := &auditedGraphJournal{sdkPosition: 8}
		for _, pair := range [][2]string{
			{`{"kind":"timer_start","name":"later","duration_nanos":1,"fire_at":"2030-01-01T00:00:00Z","clock_domain":"bounded"}`, `{}`},
			{`{"kind":"timer_await","name":"later","timer_step":8,"fire_at":"2030-01-01T00:00:00Z","clock_domain":"bounded"}`, `{}`},
			{`{"kind":"timer_start","name":"cancelled"}`, `{}`},
			{`{"kind":"timer_cancel","name":"cancelled","timer_step":12}`, `{"cancelled":true}`},
		} {
			if err := timerHistoryStep(s, pair[0], pair[1]); err != nil {
				t.Fatal(err)
			}
		}
		if err := auditSDKCheckpointTimers(s, checkpoint.Frame{CancelledTimers: []uint64{12}}); err != nil {
			t.Fatal(err)
		}
	})

	for _, mode := range []string{"cancel", "await", "timer-select", "many-select", "signal-then-cancel", "carried-cancel", "empty"} {
		t.Run(mode, func(t *testing.T) {
			s := &auditedGraphJournal{}
			frame := checkpoint.Frame{}
			run := func(req, done string) {
				t.Helper()
				if err := timerHistoryStep(s, req, done); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "empty" {
				run(`{"kind":"timer_start","name":"clock"}`, `{}`)
				switch mode {
				case "await":
					run(`{"kind":"timer_await","name":"clock"}`, `{}`)
				case "timer-select":
					run(`{"kind":"timer_signal_select","timer_name":"clock"}`, `{"selected":"timer"}`)
				case "many-select":
					run(`{"kind":"select_many","cases":[{"kind":"timer","name":"clock"}]}`, `{"case_index":0}`)
				default:
					if mode == "signal-then-cancel" {
						run(`{"kind":"timer_signal_select","timer_name":"clock"}`, `{"selected":"signal","signal_seq":7}`)
					}
					run(`{"kind":"timer_cancel","name":"clock"}`, `{"cancelled":true}`)
					frame.CancelledTimers = []uint64{0}
				}
			}
			if err := auditSDKCheckpointTimers(s, frame); err != nil {
				t.Fatal(err)
			}
			if mode == "carried-cancel" {
				run(`{"kind":"checkpoint","name":"next"}`, `{}`)
				if err := auditSDKCheckpointTimers(s, frame); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	for _, mode := range []string{"missing-creation", "wrong-name", "wrong-deadline", "wrong-domain", "unconfirmed-cancel", "cancel-after-fire", "await-after-cancel", "duplicate-cancel", "live-checkpoint", "omitted-cancel", "fabricated-cancel", "duplicate-cancel-id", "bad-select-index", "unselected-dead-timer"} {
		t.Run(mode, func(t *testing.T) {
			s := &auditedGraphJournal{}
			start := `{"kind":"timer_start","name":"clock","duration_nanos":1,"fire_at":"2030-01-01T00:00:00Z","clock_domain":"clock-domain"}`
			if mode != "missing-creation" {
				if err := timerHistoryStep(s, start, `{}`); err != nil {
					t.Fatal(err)
				}
			}
			req, done := `{"kind":"timer_cancel","name":"clock"}`, `{"cancelled":true}`
			switch mode {
			case "wrong-name":
				req = `{"kind":"timer_cancel","name":"foreign"}`
			case "wrong-deadline":
				req = `{"kind":"timer_await","name":"clock","fire_at":"2031-01-01T00:00:00Z","clock_domain":"clock-domain"}`
			case "wrong-domain":
				req = `{"kind":"timer_await","name":"clock","fire_at":"2030-01-01T00:00:00Z","clock_domain":"foreign"}`
			case "unconfirmed-cancel":
				done = `{}`
			case "cancel-after-fire":
				if err := timerHistoryStep(s, `{"kind":"timer_await","name":"clock","fire_at":"2030-01-01T00:00:00Z","clock_domain":"clock-domain"}`, `{}`); err != nil {
					t.Fatal(err)
				}
			case "await-after-cancel", "duplicate-cancel", "unselected-dead-timer":
				if err := timerHistoryStep(s, req, done); err != nil {
					t.Fatal(err)
				}
				if mode == "await-after-cancel" {
					req = `{"kind":"timer_await","name":"clock"}`
				}
				if mode == "unselected-dead-timer" {
					req = `{"kind":"select_many","cases":[{"kind":"signal","name":"go"},{"kind":"timer","name":"clock"}]}`
					done = `{"case_index":0,"signal_seq":7}`
				}
			case "bad-select-index":
				req = `{"kind":"select_many","cases":[]}`
				done = `{"case_index":0}`
			case "live-checkpoint":
				if err := auditSDKCheckpointTimers(s, checkpoint.Frame{}); err == nil {
					t.Fatal("live timer accepted")
				}
				return
			case "omitted-cancel", "fabricated-cancel", "duplicate-cancel-id":
				if err := timerHistoryStep(s, req, done); err != nil {
					t.Fatal(err)
				}
				frame := checkpoint.Frame{}
				if mode == "fabricated-cancel" {
					frame.CancelledTimers = []uint64{2}
				}
				if mode == "duplicate-cancel-id" {
					frame.CancelledTimers = []uint64{0, 0}
				}
				if err := auditSDKCheckpointTimers(s, frame); err == nil {
					t.Fatal("corrupt cancellation census accepted")
				}
				return
			}
			if err := timerHistoryStep(s, req, done); err == nil {
				t.Fatal("invalid timer action accepted")
			}
		})
	}
}
