package runtimeclock

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Clock shares bounded monotonic readings between worker and repair goroutines.
// Waiting for its sampling gate honors caller cancellation. Refresh failures
// propagate; they do not select a single server or silently extend reading age.
type Clock struct {
	sampler   *Sampler
	gate      chan struct{}
	reading   Reading
	collected time.Time
	refresh   time.Duration
}

// NewClock does not provision or mutate streams. Probe loss during a node outage
// is tolerated by sampling available independent peers after initial bootstrap.
func NewClock(js jetstream.JetStream, c Config) (*Clock, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	trusted := map[string]string{}
	var probes []string
	for _, p := range c.Probes {
		trusted[p.Server] = p.Identity
		probes = append(probes, p.Name)
	}
	source, err := NewStreamSource(js, trusted)
	if err != nil {
		return nil, err
	}
	budget, healthy, age, refresh, _ := c.limits()
	sampler, err := NewSampler(source, probes, c.MaxSkewed, budget, healthy, age)
	if err != nil {
		return nil, err
	}
	return newClock(sampler, refresh), nil
}

func newClock(sampler *Sampler, refresh time.Duration) *Clock {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &Clock{sampler: sampler, gate: gate, refresh: refresh}
}

func (c *Clock) Bounds(ctx context.Context) (time.Time, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, time.Time{}, err
	}
	select {
	case <-ctx.Done():
		return time.Time{}, time.Time{}, ctx.Err()
	case <-c.gate:
	}
	defer func() { c.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return time.Time{}, time.Time{}, err
	}
	interval, err := c.reading.Bounds()
	if err != nil || time.Since(c.collected) >= c.refresh {
		reading, err := c.sampler.Sample(ctx)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		c.reading = reading
		c.collected = time.Now()
		interval, err = reading.Bounds()
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
	}
	return interval.Lower, interval.Upper, nil
}

func (c *Clock) Lower(ctx context.Context, domain string) (time.Time, error) {
	if domain != DeadlineDomain {
		return time.Time{}, fmt.Errorf("unsupported clock domain %q", domain)
	}
	lower, _, err := c.Bounds(ctx)
	return lower, err
}
