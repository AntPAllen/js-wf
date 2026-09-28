package sim

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"js-wf/identity"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func TestSignalWakeupLivenessNegativeControl(t *testing.T) {
	ctx := context.Background()
	model := NewSignalTransport(NewScheduler(42))
	invocation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "signal-liveness"), Data: []byte("input")})
	if err != nil {
		t.Fatal(err)
	}
	signal := &nats.Msg{Subject: "wf.sig.test.signal-liveness.go", Data: []byte("payload"), Header: nats.Header{}}
	signal.Header.Set("Wf-Inv-Seq", strconv.FormatUint(invocation, 10))
	sequence := model.CommitSignal(signal)
	checkMissing := func() {
		t.Helper()
		report, err := CheckSignalWakeupLiveness(model)
		if err == nil || report.Enabled != 1 || len(report.Missing) != 1 || report.Missing[0] != "wf.sig.test.signal-liveness.go#1" {
			t.Fatalf("enabled=%d waiting=%v missing=%v err=%v", report.Enabled, report.Waiting, report.Missing, err)
		}
	}
	checkMissing() // Deliberately skip repair after the signal commit.
	if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"}); err != nil {
		t.Fatal(err)
	}
	scan := reconcile.NewSignalScanWithPort(model)
	if result, err := scan.Scan(ctx, sequence, 1, false); !errors.Is(err, ErrTransportLost) || result.Reenqueued != 1 {
		t.Fatalf("faulted scan result=%+v err=%v", result, err)
	}
	checkMissing()
	if result, err := scan.Scan(ctx, sequence, 1, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("recovery scan result=%+v err=%v", result, err)
	}
	if report, err := CheckSignalWakeupLiveness(model); err != nil || report.Enabled != 1 || len(report.Missing) != 0 {
		t.Fatalf("recovered enabled=%d missing=%v err=%v", report.Enabled, report.Missing, err)
	}
	model.StartTransport.mu.Lock()
	model.runs = nil // Deliberately remove the retained wakeup.
	model.StartTransport.mu.Unlock()
	checkMissing()
}
