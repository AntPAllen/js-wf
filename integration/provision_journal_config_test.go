package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"js-wf/provision"

	"github.com/nats-io/nats.go/jetstream"
)

func TestProvisionRejectsJournalPerSubjectCap(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	config := info.Config
	if config.MaxMsgsPerSubject != -1 {
		t.Fatalf("unexpected initial per-subject cap: %d", config.MaxMsgsPerSubject)
	}
	config.MaxMsgsPerSubject = 1
	// An update reply can be lost after commit. Confirm the stored config from
	// both nodes before testing that provisioning rejects it.
	var updateErr, readErr error
	var observed [2]int64
	confirmed := false
	for attempt := 0; attempt < 4 && !confirmed; attempt++ {
		writeCtx, stopWrite := context.WithTimeout(context.Background(), 6*time.Second)
		_, updateErr = all[0].UpdateStream(writeCtx, config)
		stopWrite()
		confirmed = true
		for node := 0; node < 2; node++ {
			readCtx, stopRead := context.WithTimeout(context.Background(), 3*time.Second)
			current, err := all[node].Stream(readCtx, "WF_JRN")
			if err == nil {
				var state *jetstream.StreamInfo
				state, err = current.Info(readCtx)
				if err == nil {
					observed[node] = state.Config.MaxMsgsPerSubject
				}
			}
			stopRead()
			if err != nil {
				readErr = err
			}
			confirmed = confirmed && err == nil && observed[node] == 1
		}
		if !confirmed {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !confirmed {
		t.Fatalf("journal cap update unconfirmed on two nodes: values=%v update_err=%v read_err=%v", observed, updateErr, readErr)
	}
	guardCtx, stopGuard := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopGuard()
	if err := provision.Ensure(guardCtx, all[1], 3); err == nil || !strings.Contains(err.Error(), "stream WF_JRN configuration mismatch") {
		t.Fatalf("expected journal provisioning rejection, got %v", err)
	}
}
