package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"js-wf/provision"
)

func TestProvisionRejectsJournalPerSubjectCap(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	if _, err := all[0].UpdateStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	if err := provision.Ensure(ctx, all[1], 3); err == nil || !strings.Contains(err.Error(), "stream WF_JRN configuration mismatch") {
		t.Fatalf("expected journal provisioning rejection, got %v", err)
	}
}
