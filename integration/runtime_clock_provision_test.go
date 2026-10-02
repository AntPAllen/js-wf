package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"js-wf/runtimeclock"
	"js-wf/testcluster"
)

func TestRuntimeClockProvisionsExactIndependentProbesAndSurvivesLoss(t *testing.T) {
	tags := map[int][]string{0: {"clock-a"}, 1: {"clock-b"}, 2: {"clock-c"}}
	c, err := testcluster.StartWithServerTags(t.TempDir(), 3, tags)
	if err != nil {
		t.Fatal(err)
	}
	all, cluster := setupCluster(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := runtimeclock.DefaultConfig()
	cfg.Refresh = "1ns"
	for i, server := range cluster.Servers {
		cfg.Probes = append(cfg.Probes, runtimeclock.Probe{Name: fmt.Sprintf("WF_CLOCK_%d", i), Server: server.Name(), Identity: server.ID(), Tag: tags[i][0]})
	}
	if err := runtimeclock.EnsureProbes(ctx, all[2], cfg); err != nil {
		t.Fatal(err)
	}
	if err := runtimeclock.EnsureProbes(ctx, all[1], cfg); err != nil {
		t.Fatalf("idempotent provisioning: %v", err)
	}
	clock, err := runtimeclock.NewClock(all[2], cfg)
	if err != nil {
		t.Fatal(err)
	}
	lower, upper, err := clock.Bounds(ctx)
	if err != nil || upper.Before(lower) {
		t.Fatalf("clock=%s..%s err=%v", lower, upper, err)
	}
	for _, p := range cfg.Probes {
		s, err := all[2].Stream(ctx, p.Name)
		if err != nil {
			t.Fatal(err)
		}
		info := s.CachedInfo()
		if info.Cluster.Leader != p.Server || info.Config.MaxBytes != 1 || info.Config.Metadata["workflow_clock_domain"] != runtimeclock.DeadlineDomain {
			t.Fatalf("actual clock probe=%+v", info)
		}
	}
	// A conflicting retained config must be rejected without being rewritten.
	s, err := all[2].Stream(ctx, cfg.Probes[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	changed := s.CachedInfo().Config
	changed.MaxBytes = 2
	if _, err := all[2].UpdateStream(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if err := runtimeclock.EnsureProbes(ctx, all[2], cfg); err == nil || !strings.Contains(err.Error(), "configuration mismatch") {
		t.Fatalf("adopted bad clock probe: %v", err)
	}
	info, err := s.Info(ctx)
	if err != nil || info.Config.MaxBytes != 2 {
		t.Fatal("provisioner rewrote the conflict")
	}
	changed.MaxBytes = 1
	if _, err := all[2].UpdateStream(ctx, changed); err != nil {
		t.Fatal(err)
	}
	cluster.KillNode(0)
	// Reading refresh is forced by the1ns cadence: healthy survivors must
	// establish new agreement rather than use the pre-outage cached reading.
	lower, upper, err = clock.Bounds(ctx)
	if err != nil || upper.Before(lower) {
		t.Fatalf("surviving clock agreement=%s..%s err=%v", lower, upper, err)
	}
	t.Logf("exact probes, unchanged conflict and fresh two-source agreement after node loss; uncertainty=%s", upper.Sub(lower))
}
