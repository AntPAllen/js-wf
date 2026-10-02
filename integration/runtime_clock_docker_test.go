//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/runtimeclock"
	"js-wf/testcluster"
)

func TestFiveContainerIndependentClockWithSkewAndProbeRestart(t *testing.T) {
	if os.Getenv("WF_TIER3_CLOCK_PROBES") != "1" {
		t.Skip("set WF_TIER3_CLOCK_PROBES=1 for native Docker clock topology")
	}
	for _, offset := range []time.Duration{-time.Minute, time.Minute} {
		t.Run(offset.String(), func(t *testing.T) {
			t.Setenv("WF_TIER3_SERVER_SKEW", "4:"+offset.String())
			root := t.TempDir()
			if base := os.Getenv("TIER3_CLOCK_PROBE_ARTIFACT_ROOT"); base != "" {
				root = filepath.Join(base, fmt.Sprintf("offset-%d", int(offset.Seconds())))
			}
			tags := map[int][]string{}
			for i := 0; i < 5; i++ {
				tags[i] = []string{fmt.Sprintf("clock-%d", i)}
			}
			cluster, err := testcluster.StartDockerClusterWithStoresAndTags(root, 5, nil, tags)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			defer func() {
				for i := 0; i < 5; i++ {
					logs, err := cluster.Logs(i)
					if err != nil {
						t.Error(err)
						continue
					}
					if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("server-%d.log", i)), []byte(logs), 0644); err != nil {
						t.Error(err)
					}
				}
			}()
			urls := make([]string, 5)
			for i := range urls {
				urls[i] = cluster.ClientURL(i)
			}
			nc, err := nats.Connect(strings.Join(urls, ","), nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond), nats.IgnoreDiscoveredServers())
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			cfg := runtimeclock.DefaultConfig()
			cfg.Refresh = "1ns"
			trusted := map[string]string{}
			for i := 0; i < 5; i++ {
				identity := fmt.Sprintf("docker-physical-%d", i)
				cfg.Probes = append(cfg.Probes, runtimeclock.Probe{Name: fmt.Sprintf("WF_CLOCK_%d", i), Server: cluster.NodeName(i), Identity: identity, Tag: tags[i][0]})
				trusted[cluster.NodeName(i)] = identity
			}
			ready, stop := context.WithTimeout(ctx, 45*time.Second)
			for ready.Err() == nil {
				err = runtimeclock.EnsureProbes(ready, js, cfg)
				if err == nil {
					break
				}
				select {
				case <-ready.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
			stop()
			if err != nil {
				t.Fatal(err)
			}
			source, err := runtimeclock.NewStreamSource(js, trusted)
			if err != nil {
				t.Fatal(err)
			}
			physical, stamp, err := source.ReadClock(ctx, "WF_CLOCK_4")
			if err != nil || physical != "docker-physical-4" || time.Since(stamp.Add(-offset)) > 2*time.Second || stamp.Add(-offset).After(time.Now().Add(2*time.Second)) {
				t.Fatalf("shifted probe identity=%s time=%s offset=%s err=%v", physical, stamp, offset, err)
			}
			clock, err := runtimeclock.NewClock(js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			type boundsProof struct {
				Stage                       string
				Before, Lower, Upper, After time.Time
			}
			var proofs []boundsProof
			check := func(stage string) {
				before := time.Now().UTC()
				lower, upper, err := clock.Bounds(ctx)
				after := time.Now().UTC()
				if err != nil || lower.After(after) || upper.Before(before) || upper.Sub(lower) > time.Second {
					t.Fatalf("%s canonical bounds=%s..%s observer=%s..%s err=%v", stage, lower, upper, before, after, err)
				}
				proofs = append(proofs, boundsProof{stage, before, lower, upper, after})
			}
			check("all-five-one-skewed")
			if err := cluster.KillNode(0); err != nil {
				t.Fatal(err)
			}
			check("one-healthy-probe-unavailable")
			if err := cluster.RestartNode(0); err != nil {
				t.Fatal(err)
			}
			// Restart may recreate volatile memory contents; exact provisioning must
			// still find the declared placement, never relocate to an alias.
			restartReady, stopRestartReady := context.WithTimeout(ctx, 30*time.Second)
			for restartReady.Err() == nil {
				err = runtimeclock.EnsureProbes(restartReady, js, cfg)
				if err == nil {
					break
				}
				if !matrixTransientTransport(err) {
					break
				}
				t.Logf("probe restart readiness: %v", err)
				select {
				case <-restartReady.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
			stopRestartReady()
			if err != nil {
				t.Fatal(err)
			}
			physical, _, err = source.ReadClock(ctx, "WF_CLOCK_0")
			if err != nil || physical != "docker-physical-0" {
				t.Fatalf("restarted probe identity=%s err=%v", physical, err)
			}
			check("probe-restarted-same-physical-placement")
			data, _ := json.MarshalIndent(struct {
				Config runtimeclock.Config
				Offset time.Duration
				Proofs []boundsProof
			}{cfg, offset, proofs}, "", "  ")
			if err := os.WriteFile(filepath.Join(root, "clock-proof.json"), data, 0644); err != nil {
				t.Fatal(err)
			}
			t.Logf("five independent probes, actual%s skew, canonical bounds through healthy-node loss and restart", offset)
		})
	}
}
