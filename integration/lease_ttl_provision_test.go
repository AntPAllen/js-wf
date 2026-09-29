package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"js-wf/provision"
)

func TestProvisionRejectsOldLeaseTTL(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv, err := all[0].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := status.TTL(); got != provision.LeaseTTL {
		t.Fatalf("provisioned lease TTL=%s want=%s", got, provision.LeaseTTL)
	}
	cfg := status.Config()
	cfg.TTL = 30 * time.Second
	if _, err := all[0].UpdateKeyValue(ctx, cfg); err != nil {
		t.Fatalf("set old lease TTL: %v", err)
	}
	if err := provision.Ensure(ctx, all[1], 3); err == nil || !strings.Contains(err.Error(), "bucket WF_LEASE configuration mismatch") {
		t.Fatalf("provision accepted old lease TTL: %v", err)
	}
	cfg.TTL = provision.LeaseTTL
	if _, err := all[0].UpdateKeyValue(ctx, cfg); err != nil {
		t.Fatalf("restore lease TTL: %v", err)
	}
	if err := provision.Ensure(ctx, all[2], 3); err != nil {
		t.Fatalf("provision after explicit TTL update: %v", err)
	}
}
