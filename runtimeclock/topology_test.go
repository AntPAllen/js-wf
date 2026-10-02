package runtimeclock

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func validClockConfig() Config {
	c := DefaultConfig()
	c.Probes = []Probe{{"WF_CLOCK_A", "server-a", "physical-a", "clock-a"}, {"WF_CLOCK_B", "server-b", "physical-b", "clock-b"}, {"WF_CLOCK_C", "server-c", "physical-c", "clock-c"}}
	return c
}

func TestClockTopologyRejectsAliasesAndInvalidBounds(t *testing.T) {
	for _, mode := range []string{"valid", "alias_server", "alias_identity", "alias_tag", "alias_stream", "foreign_stream", "invalid_stream", "empty", "too_few", "too_many_skewed", "budget", "healthy", "age", "refresh"} {
		t.Run(mode, func(t *testing.T) {
			c := validClockConfig()
			switch mode {
			case "alias_server":
				c.Probes[1].Server = c.Probes[0].Server
			case "alias_identity":
				c.Probes[1].Identity = c.Probes[0].Identity
			case "alias_tag":
				c.Probes[1].Tag = c.Probes[0].Tag
			case "alias_stream":
				c.Probes[1].Name = c.Probes[0].Name
			case "foreign_stream":
				c.Probes[0].Name = "WF_RUN"
			case "invalid_stream":
				c.Probes[0].Name = "WF_CLOCK_*"
			case "empty":
				c.Probes[0].Identity = ""
			case "too_few":
				c.Probes = c.Probes[:2]
			case "too_many_skewed":
				c.MaxSkewed = 2
			case "budget":
				c.SampleBudget = "2s"
			case "healthy":
				c.HealthyError = "-1ms"
			case "age":
				c.ReadingAge = "100ms"
			case "refresh":
				c.Refresh = "1s"
			}
			err := c.Validate()
			if (err == nil) != (mode == "valid") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
		})
	}
	b, _ := json.Marshal(validClockConfig())
	if _, err := ReadConfig(strings.NewReader(string(b))); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{string(b) + " {}", `{"probes":[],"unrecognized":true}`, strings.Repeat(" ", 64*1024+1), `null`} {
		if _, err := ReadConfig(strings.NewReader(data)); err == nil {
			t.Fatal("invalid topology file accepted")
		}
	}
}

func TestSharedClockCacheAdvancesAndFailsClosedOnRefresh(t *testing.T) {
	var calls atomic.Int32
	var failed atomic.Bool
	source := clockSourceFunc(func(ctx context.Context, probe string) (string, time.Time, error) {
		calls.Add(1)
		if failed.Load() {
			return "", time.Time{}, context.DeadlineExceeded
		}
		return probe, time.Now(), nil
	})
	sampler, err := NewSampler(source, []string{"a", "b", "c"}, 1, time.Second, time.Millisecond, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c := newClock(sampler, time.Second)
	lower, upper, err := c.Bounds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstCalls := calls.Load()
	newLower, newUpper, err := c.Bounds(context.Background())
	if err != nil || newLower.Before(lower) || newUpper.Before(upper) || calls.Load() != firstCalls {
		t.Fatalf("cache did not advance monotonically: %v", err)
	}
	if _, err := c.Lower(context.Background(), "foreign"); err == nil {
		t.Fatal("unknown domain accepted")
	}
	// Force refresh by advancing only the refresh marker, not the reading's
	// monotonic anchor; an otherwise live old reading must not mask failure.
	c.collected = c.collected.Add(-2 * time.Second)
	failed.Store(true)
	if _, _, err := c.Bounds(context.Background()); !errors.Is(err, ErrNoAgreement) {
		t.Fatalf("refresh masked loss=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.Bounds(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	failed.Store(false)
	if _, _, err := c.Bounds(context.Background()); err != nil {
		t.Fatalf("refresh did not recover: %v", err)
	}
}

func TestSharedClockGateWaitHonorsContext(t *testing.T) {
	c := newClock(nil, time.Second)
	<-c.gate
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, _, err := c.Bounds(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
