package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/worker"
)

// Verify the actual broker retains domain metadata on the emitted native
// target, rather than relying on the in-memory transport's header copying.
func TestNativeTimerDeliveryPreservesDomainDeadline(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	physical := run.CachedInfo().TimeStamp.Add(300 * time.Millisecond)
	canonical := physical.Add(time.Minute)
	deadline := worker.TimerDeadline{FireAt: canonical, ClockDomain: "utc-quorum-v1", ScheduleAt: physical}
	published, err := worker.ScheduleTimerDeadlineWithPort(ctx, worker.NewTimerSchedulePort(all[1]), true, "test", "domain-native", 42, 7, deadline)
	if err != nil || !published {
		t.Fatalf("publish=%t %v", published, err)
	}
	subject := identity.RunSubject("test", "domain-native", provision.Partitions)
	var message *jetstream.RawStreamMsg
	for {
		message, err = run.GetLastMsgForSubject(ctx, subject)
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if message.Header.Get(identity.TimerClockDomainHeader) != "utc-quorum-v1" || message.Header.Get(identity.TimerDeadlineHeader) != canonical.UTC().Format(time.RFC3339Nano) || message.Header.Get(identity.TimerInvSeqHeader) != "42" || message.Header.Get(identity.TimerStepHeader) != "7" {
		t.Fatalf("actual scheduled target lost deadline identity: %v", message.Header)
	}
	t.Logf("native target preserves domain/deadline and generation/step; hint=%s canonical=%s", physical, canonical)
}
