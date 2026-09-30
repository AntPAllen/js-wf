package sim

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func TestChildWakeupLivenessNegativeControls(t *testing.T) {
	for _, mode := range []string{"skipped_notifier", "dropped_signal", "dropped_wakeup", "removed_wakeup", "changed_notification", "changed_notification_hash", "consumed_purged_signal", "wrong_consumed_outcome", "wrong_consumed_hash", "parent_terminal", "stale_parent_terminal", "parent_reused", "parent_absent", "child_nonterminal", "stale_child_terminal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			model := NewSignalTransport(NewScheduler(42))
			parent, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "parent"), Data: []byte("parent")})
			if err != nil {
				t.Fatal(err)
			}
			headers := nats.Header{}
			headers.Set(client.ParentTypeHeader, "test")
			headers.Set(client.ParentIDHeader, "parent")
			headers.Set(client.ParentInvSeqHeader, strconv.FormatUint(parent, 10))
			headers.Set(client.ParentSignalHeader, "child_done")
			child, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "child"), Data: []byte("child"), Header: headers})
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte(`{"inv_seq":` + strconv.FormatUint(child, 10) + `,"result":42}`)
			model.SetJournal("test", "child", []journal.Record{{Entry: journal.Entry{Kind: journal.Completed, Payload: payload}, Sequence: 1}})
			checkMissing := func(reason string) {
				t.Helper()
				report, err := CheckChildWakeupLiveness(model)
				if err == nil || report.Enabled != 1 || len(report.Missing) != 1 || !strings.Contains(report.Missing[0], reason) {
					t.Fatalf("missing %s: report=%+v err=%v", reason, report, err)
				}
			}
			// Existing signal-only checks cannot see a notifier that never published.
			if report, err := CheckSignalWakeupLiveness(model); err != nil || report.Enabled != 0 {
				t.Fatalf("signal-only baseline: report=%+v err=%v", report, err)
			}
			checkMissing("notification missing")
			c := client.NewWithSignalPorts(model, model)
			notify := func() error { return worker.NotifyParentWithClient(ctx, c, "test", "child", child, payload, headers) }
			if mode == "dropped_signal" {
				if err := model.QueueSignalFault(SignalDropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "dropped_wakeup" {
				if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"}); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "skipped_notifier" {
				err := notify()
				switch mode {
				case "dropped_signal":
					if !errors.Is(err, client.ErrSignalUnknown) {
						t.Fatalf("dropped signal: %v", err)
					}
					checkMissing("notification missing")
				case "dropped_wakeup":
					if !errors.Is(err, client.ErrEnqueueUnknown) {
						t.Fatalf("dropped wakeup: %v", err)
					}
					checkMissing("parent wakeup missing")
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := notify(); err != nil {
					t.Fatal(err)
				}
				if report, err := CheckChildWakeupLiveness(model); err != nil || report.Enabled != 1 {
					t.Fatalf("repaired baseline: report=%+v err=%v", report, err)
				}
			}
			signals := model.SignalFor("test", "parent", "child_done")
			wantMissing, reason := false, ""
			switch mode {
			case "skipped_notifier":
				wantMissing, reason = true, "notification missing"
			case "removed_wakeup":
				model.StartTransport.mu.Lock()
				model.runs = nil
				model.StartTransport.mu.Unlock()
				wantMissing, reason = true, "parent wakeup missing"
			case "changed_notification", "changed_notification_hash":
				model.mu.Lock()
				signal := model.signals[signals[0].Sequence]
				signal.Data = []byte("wrong")
				signal.Header.Del("Wf-Input-SHA256")
				if mode == "changed_notification_hash" {
					signal.Header.Set("Wf-Input-SHA256", digest(payload))
				}
				model.signals[signal.Sequence] = signal
				model.mu.Unlock()
				wantMissing, reason = true, "notification missing"
			case "consumed_purged_signal", "wrong_consumed_outcome", "wrong_consumed_hash":
				consumedPayload := payload
				if mode == "wrong_consumed_outcome" || mode == "wrong_consumed_hash" {
					consumedPayload = []byte("wrong")
					wantMissing, reason = true, "notification missing"
				}
				event, _ := json.Marshal(struct {
					Sequence uint64 `json:"sig_seq"`
					Name     string `json:"name"`
					Payload  []byte `json:"payload"`
					Hash     string `json:"hash,omitempty"`
				}{signals[0].Sequence, "child_done", consumedPayload, func() string {
					if mode == "wrong_consumed_hash" {
						return digest(payload)
					}
					return ""
				}()})
				model.SetJournal("test", "parent", []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: event}, Sequence: 2}})
				model.PurgeSignal(signals[0].Sequence)
			case "parent_terminal", "stale_parent_terminal":
				gen := parent
				if mode == "stale_parent_terminal" {
					gen = parent + 99
					model.StartTransport.mu.Lock()
					model.runs = nil
					model.StartTransport.mu.Unlock()
					wantMissing, reason = true, "parent wakeup missing"
				}
				model.SetJournal("test", "parent", []journal.Record{{Entry: journal.Entry{Kind: journal.Completed, Payload: []byte(`{"inv_seq":` + strconv.FormatUint(gen, 10) + `,"result":0}`)}, Sequence: 2}})
			case "parent_absent", "parent_reused":
				model.PurgeInvocation(identity.InvocationSubject("test", "parent"))
				if mode == "parent_reused" {
					if _, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "parent"), Data: []byte("new")}); err != nil {
						t.Fatal(err)
					}
				}
			case "child_nonterminal":
				model.SetJournal("test", "child", nil)
			case "stale_child_terminal":
				model.SetJournal("test", "child", []journal.Record{{Entry: journal.Entry{Kind: journal.Completed, Payload: []byte(`{"inv_seq":999,"result":42}`)}, Sequence: 1}})
			}
			if wantMissing {
				checkMissing(reason)
			} else if report, err := CheckChildWakeupLiveness(model); err != nil || len(report.Missing) != 0 {
				t.Fatalf("valid/obsolete child: report=%+v err=%v", report, err)
			}
		})
	}
}
