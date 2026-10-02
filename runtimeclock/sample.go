package runtimeclock

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Source returns the physical server identity and its current clock. Implementors
// must use an authenticated transport, not identify the server by the endpoint
// receiving the request: a clustered request can be forwarded to another peer.
type Source interface {
	// ReadClock must honor context cancellation and return promptly.
	ReadClock(context.Context, string) (server string, now time.Time, err error)
}

type Sampler struct {
	source                             Source
	probes                             []string
	maxSkewed                          int
	maxRoundTrip, healthyError, maxAge time.Duration
}

// NewSampler copies its probe configuration. Each sample performs at most five
// concurrent RPCs; it never caches a server timestamp or retries within an RPC.
// maxAge limits use of the resulting monotonic reading after collection starts.
func NewSampler(source Source, probes []string, maxSkewed int, maxRoundTrip, healthyError, maxAge time.Duration) (*Sampler, error) {
	const maxDuration time.Duration = 1<<63 - 1
	if source == nil || len(probes) == 0 || len(probes) > 5 || maxSkewed < 0 || maxSkewed > 2 || maxRoundTrip <= 0 || healthyError < 0 || healthyError > (maxDuration-maxRoundTrip)/2 || maxAge <= maxRoundTrip {
		return nil, fmt.Errorf("invalid clock sampler configuration")
	}
	seen := map[string]bool{}
	for _, probe := range probes {
		if probe == "" || seen[probe] {
			return nil, fmt.Errorf("empty or duplicate clock probe %q", probe)
		}
		seen[probe] = true
	}
	return &Sampler{source: source, probes: append([]string(nil), probes...), maxSkewed: maxSkewed, maxRoundTrip: maxRoundTrip, healthyError: healthyError, maxAge: maxAge}, nil
}

// Reading retains a monotonic anchor. Its wall component is never used to
// estimate UTC. A reading cannot be serialized or reused across process starts.
type Reading struct {
	interval Interval
	anchor   time.Time
	maxAge   time.Duration
}

// Bounds advances both UTC bounds by monotonic elapsed time. A caller must use
// the upper bound to create a duration deadline and the lower bound to prove due.
func (r Reading) Bounds() (Interval, error) { return r.bounds(time.Since(r.anchor)) }

func (r Reading) bounds(elapsed time.Duration) (Interval, error) {
	if r.anchor.IsZero() || elapsed < 0 || elapsed > r.maxAge {
		return Interval{}, fmt.Errorf("clock reading absent or expired")
	}
	return Interval{Lower: r.interval.Lower.Add(elapsed), Upper: r.interval.Upper.Add(elapsed), Sources: append([]string(nil), r.interval.Sources...)}, nil
}

// Sample brackets each RPC against one monotonic anchor and waits for the
// bounded collection. Unavailable sources are omitted; malformed successful
// replies fail closed. Migrated probes on one peer count only once. The shortest
// bracket wins, with deterministic ties, so duplicate leaders cannot vote twice.
func (s *Sampler) Sample(ctx context.Context) (Reading, error) {
	parent := ctx
	if err := parent.Err(); err != nil {
		return Reading{}, err
	}
	anchor := time.Now()
	ctx, cancel := context.WithTimeout(ctx, s.maxRoundTrip)
	defer cancel()
	type response struct {
		observation Observation
		err         error
	}
	replies := make(chan response, len(s.probes))
	for _, probe := range s.probes {
		go func(probe string) {
			started := time.Since(anchor)
			server, now, err := s.source.ReadClock(ctx, probe)
			replies <- response{Observation{server, now, started, time.Since(anchor)}, err}
		}(probe)
	}
	var observations []Observation
	for range s.probes {
		select {
		case reply := <-replies:
			if reply.err != nil {
				continue
			}
			o := reply.observation
			if o.Server == "" || o.Time.IsZero() || o.Finished < o.Started || o.Finished-o.Started > s.maxRoundTrip {
				return Reading{}, fmt.Errorf("malformed successful clock response")
			}
			observations = append(observations, o)
		case <-ctx.Done():
			if err := parent.Err(); err != nil {
				return Reading{}, err
			}
			// Collection budget expiry is a source loss, not agreement. Drain
			// replies already received; don't wait for a non-cooperative adapter.
			for {
				select {
				case reply := <-replies:
					if reply.err == nil {
						observations = append(observations, reply.observation)
					}
				default:
					return s.reading(anchor, observations)
				}
			}
		}
	}
	if err := parent.Err(); err != nil {
		return Reading{}, err
	}
	return s.reading(anchor, observations)
}

func (s *Sampler) reading(anchor time.Time, observations []Observation) (Reading, error) {
	sort.Slice(observations, func(i, j int) bool {
		a, b := observations[i], observations[j]
		if a.Server != b.Server {
			return a.Server < b.Server
		}
		if a.Finished-a.Started != b.Finished-b.Started {
			return a.Finished-a.Started < b.Finished-b.Started
		}
		if !a.Time.Equal(b.Time) {
			return a.Time.Before(b.Time)
		}
		return a.Started < b.Started
	})
	var unique []Observation
	for _, o := range observations {
		// Validate before deduplication: malformed aliases cannot be hidden.
		if o.Server == "" || o.Time.IsZero() || o.Started < 0 || o.Finished < o.Started || o.Finished-o.Started > s.maxRoundTrip {
			return Reading{}, fmt.Errorf("invalid clock observation")
		}
		if len(unique) == 0 || unique[len(unique)-1].Server != o.Server {
			unique = append(unique, o)
		}
	}
	interval, err := Estimate(unique, s.maxSkewed, s.maxRoundTrip, s.healthyError)
	if err != nil {
		return Reading{}, err
	}
	reading := Reading{interval, anchor, s.maxAge}
	if _, err := reading.Bounds(); err != nil {
		return Reading{}, err
	}
	return reading, nil
}
