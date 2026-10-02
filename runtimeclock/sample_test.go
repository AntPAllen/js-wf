package runtimeclock

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type clockSourceFunc func(context.Context, string) (string, time.Time, error)

func (f clockSourceFunc) ReadClock(ctx context.Context, p string) (string, time.Time, error) {
	return f(ctx, p)
}

func TestSamplerIndependentSourcesAndLoss(t *testing.T) {
	for _, mode := range []string{"ahead", "behind", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			blockedDone := make(chan struct{})
			f := clockSourceFunc(func(ctx context.Context, p string) (string, time.Time, error) {
				calls.Add(1)
				if p == "c" {
					switch mode {
					case "ahead":
						return p, time.Now().Add(time.Minute), nil
					case "behind":
						return p, time.Now().Add(-time.Minute), nil
					default:
						defer close(blockedDone)
						<-ctx.Done()
						return "", time.Time{}, ctx.Err()
					}
				}
				return p, time.Now(), nil
			})
			s, err := NewSampler(f, []string{"a", "b", "c"}, 1, 50*time.Millisecond, 20*time.Millisecond, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.Sample(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			bounds, err := r.Bounds()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(bounds.Sources, []string{"a", "b"}) || bounds.Upper.Sub(bounds.Lower) > time.Second {
				t.Fatalf("skew or lost source influenced reading: %+v", bounds)
			}
			if calls.Load() != 3 {
				t.Fatalf("calls=%d", calls.Load())
			}
			if mode == "unavailable" {
				select {
				case <-blockedDone:
				case <-time.After(time.Second):
					t.Fatal("source did not cancel")
				}
			}
		})
	}
}

func TestSamplerMigrationCountsPhysicalLeaderOnce(t *testing.T) {
	s, _ := NewSampler(clockSourceFunc(nil), []string{"a", "b", "c"}, 1, time.Second, time.Millisecond, 3*time.Second)
	anchor := time.Now()
	observations := []Observation{
		{Server: "one", Time: anchor.UTC(), Finished: 10 * time.Millisecond},
		{Server: "one", Time: anchor.UTC(), Finished: time.Millisecond},
		{Server: "two", Time: anchor.UTC(), Finished: time.Millisecond},
	}
	r, err := s.reading(anchor, observations)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.interval.Sources, []string{"one", "two"}) || r.interval.Upper.Sub(r.interval.Lower) > 15*time.Millisecond {
		t.Fatalf("did not choose short independent observations: %+v", r.interval)
	}
	observations[2].Server = "one"
	if _, err := s.reading(anchor, observations); !errors.Is(err, ErrNoAgreement) {
		t.Fatalf("aliases voted twice: %v", err)
	}
	observations[0].Time = time.Time{}
	if _, err := s.reading(anchor, observations); err == nil {
		t.Fatal("malformed duplicate hidden")
	}
}

func TestReadingMonotonicAgeAndOwnership(t *testing.T) {
	anchor := time.Now()
	r := Reading{Interval{anchor.UTC(), anchor.UTC().Add(time.Millisecond), []string{"a", "b"}}, anchor, time.Second}
	b, err := r.bounds(500 * time.Millisecond)
	if err != nil || !b.Lower.Equal(r.interval.Lower.Add(500*time.Millisecond)) || !b.Upper.Equal(r.interval.Upper.Add(500*time.Millisecond)) {
		t.Fatalf("advance: %+v %v", b, err)
	}
	b.Sources[0] = "changed"
	if r.interval.Sources[0] != "a" {
		t.Fatal("reading sources aliased")
	}
	for _, elapsed := range []time.Duration{-1, time.Second + 1} {
		if _, err := r.bounds(elapsed); err == nil {
			t.Fatal("invalid age accepted")
		}
	}
	if _, err := (Reading{}).Bounds(); err == nil {
		t.Fatal("empty reading accepted")
	}
}

func TestSamplerCancellationAndConfiguration(t *testing.T) {
	var calls atomic.Int32
	f := clockSourceFunc(func(ctx context.Context, p string) (string, time.Time, error) {
		calls.Add(1)
		<-ctx.Done()
		return "", time.Time{}, ctx.Err()
	})
	s, _ := NewSampler(f, []string{"a", "b"}, 1, time.Second, 0, 2*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Sample(ctx); !errors.Is(err, context.Canceled) || calls.Load() != 0 {
		t.Fatalf("canceled sampling: %v calls=%d", err, calls.Load())
	}
	for _, probes := range [][]string{nil, {""}, {"a", "a"}, {"a", "b", "c", "d", "e", "f"}} {
		if _, err := NewSampler(f, probes, 1, time.Second, 0, 2*time.Second); err == nil {
			t.Fatal("invalid probes accepted")
		}
	}
	if _, err := NewSampler(f, []string{"a"}, 1, time.Second, 0, time.Second); err == nil {
		t.Fatal("reading expires before collection")
	}
}

func TestStreamClockUsesTrustedResponseLeader(t *testing.T) {
	s := &StreamSource{leaders: map[string]string{"server-a": "physical-a", "server-a-alias": "physical-a", "server-b": "physical-b"}}
	stamp := time.Now().UTC()
	info := jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: "probe", Replicas: 1}, Cluster: &jetstream.ClusterInfo{Leader: "server-b"}, TimeStamp: stamp}
	id, now, err := s.clockFromInfo("probe", &info)
	if err != nil || id != "physical-b" || !now.Equal(stamp) {
		t.Fatalf("leader provenance: %q %v %v", id, now, err)
	}
	for _, mode := range []string{"unknown", "missing_cluster", "zero_time", "wrong_stream", "empty_leader", "replicated", "raft", "migrating", "peers"} {
		t.Run(mode, func(t *testing.T) {
			copy := info
			cluster := *info.Cluster
			copy.Cluster = &cluster
			switch mode {
			case "unknown":
				copy.Cluster.Leader = "foreign"
			case "missing_cluster":
				copy.Cluster = nil
			case "zero_time":
				copy.TimeStamp = time.Time{}
			case "wrong_stream":
				copy.Config.Name = "other"
			case "empty_leader":
				copy.Cluster.Leader = ""
			case "replicated":
				copy.Config.Replicas = 3
			case "raft":
				copy.Cluster.RaftGroup = "group"
			case "migrating":
				copy.Cluster.Desired = &jetstream.DesiredClusterInfo{}
			case "peers":
				copy.Cluster.Replicas = []*jetstream.PeerInfo{{Name: "peer"}}
			}
			if _, _, err := s.clockFromInfo("probe", &copy); err == nil {
				t.Fatal("untrusted or absent provenance accepted")
			}
		})
	}
	if _, _, err := s.clockFromInfo("probe", nil); err == nil {
		t.Fatal("nil response accepted")
	}
}
