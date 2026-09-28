package sim

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func TestSuspendedWakeupLivenessNegativeControl(t *testing.T) {
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	model := NewSignalTransport(NewScheduler(42))
	scan := reconcile.NewSuspendedScanWithPort(model)
	scan.Now = func() time.Time { return base.Add(time.Duration(model.schedule.NowMillis()) * time.Millisecond) }
	modes := []string{"due_timer", "matching_signal", "due_select", "signal_select", "future_timer"}
	for i, mode := range modes {
		id := fmt.Sprintf("liveness-%02d", i)
		sequence, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", id), Data: []byte("input")})
		if err != nil {
			t.Fatal(err)
		}
		var signalSeq uint64
		if mode == "matching_signal" || mode == "signal_select" {
			message := &nats.Msg{Subject: "wf.sig.test." + id + ".go", Data: []byte("payload"), Header: nats.Header{}}
			message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(sequence, 10))
			signalSeq = model.CommitSignal(message)
		}
		fireAt := base.Add(-2 * time.Second)
		if mode == "future_timer" || mode == "signal_select" {
			fireAt = base.Add(2 * time.Second)
		}
		records, err := suspendedFixture(sequence, mode, fireAt, signalSeq)
		if err != nil {
			t.Fatal(err)
		}
		model.SetJournal("test", id, records)
	}
	assertMissing := func(want int) SuspendedLivenessReport {
		t.Helper()
		report, err := CheckSuspendedWakeupLiveness(model, scan.Now(), scan.Grace)
		if err == nil || report.Enabled != 4 || len(report.Missing) != want || report.Waiting[identity.Key("test", "liveness-04")] != "future timer until 2023-11-14T22:13:23Z" {
			t.Fatalf("enabled=%d waiting=%v missing=%v err=%v", report.Enabled, report.Waiting, report.Missing, err)
		}
		return report
	}
	report := assertMissing(4) // Deliberately skip the reconciler.
	for _, missing := range report.Missing {
		if !strings.Contains(missing, "test.liveness-") {
			t.Fatalf("unnamed missing wakeup: %q", missing)
		}
	}
	if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"}); err != nil {
		t.Fatal(err)
	}
	if result, err := scan.Scan(ctx, 1, len(modes), false); !errors.Is(err, ErrTransportLost) || result.Reenqueued != 4 {
		t.Fatalf("faulted scan result=%+v err=%v", result, err)
	}
	assertMissing(4)
	if result, err := scan.Scan(ctx, 1, len(modes), false); err != nil || result.Reenqueued != 4 {
		t.Fatalf("recovery scan result=%+v err=%v", result, err)
	}
	if report, err := CheckSuspendedWakeupLiveness(model, scan.Now(), scan.Grace); err != nil || report.Enabled != 4 || len(report.Waiting) != 1 {
		t.Fatalf("recovered enabled=%d waiting=%v missing=%v err=%v", report.Enabled, report.Waiting, report.Missing, err)
	}
	if err := model.Wait(ctx, 4*time.Second); err != nil {
		t.Fatal(err)
	}
	if result, err := scan.Scan(ctx, 5, 1, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("future timer scan result=%+v err=%v", result, err)
	}
	if report, err := CheckSuspendedWakeupLiveness(model, scan.Now(), scan.Grace); err != nil || report.Enabled != 5 || len(report.Waiting) != 0 {
		t.Fatalf("advanced enabled=%d waiting=%v missing=%v err=%v", report.Enabled, report.Waiting, report.Missing, err)
	}
	model.StartTransport.mu.Lock()
	model.runs = model.runs[1:] // Deliberately lose a retained wakeup after recovery.
	model.StartTransport.mu.Unlock()
	report, err := CheckSuspendedWakeupLiveness(model, scan.Now(), scan.Grace)
	if err == nil || report.Enabled != 5 || len(report.Missing) != 1 || !strings.Contains(report.Missing[0], "test.liveness-00") {
		t.Fatalf("lost retained wakeup enabled=%d missing=%v err=%v", report.Enabled, report.Missing, err)
	}
}
