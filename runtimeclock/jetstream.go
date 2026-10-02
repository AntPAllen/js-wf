package runtimeclock

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// StreamSource reads current single-replica stream-info responses. trustedLeaders maps unique
// configured cluster server names to physical identities. It must come from
// operator-controlled topology, not an unverified discovery response. The NATS
// connection must authenticate the trusted cluster and protect reply subjects.
// Single-replica stream owners, not the worker's connected peer, identify clock
// sources. Replicated/migrating streams are rejected: their current leader can
// change between response admission and timestamp construction.
type StreamSource struct {
	js      jetstream.JetStream
	leaders map[string]string
}

func NewStreamSource(js jetstream.JetStream, trustedLeaders map[string]string) (*StreamSource, error) {
	if js == nil || len(trustedLeaders) == 0 {
		return nil, fmt.Errorf("clock source requires trusted cluster topology")
	}
	copy := make(map[string]string, len(trustedLeaders))
	for name, id := range trustedLeaders {
		if name == "" || id == "" {
			return nil, fmt.Errorf("empty trusted server name or identity")
		}
		copy[name] = id
	}
	return &StreamSource{js, copy}, nil
}

func (s *StreamSource) ReadClock(ctx context.Context, probe string) (string, time.Time, error) {
	// Stream performs one live STREAM.INFO request. CachedInfo here is that
	// just-returned response, not a stream handle retained between samples.
	stream, err := s.js.Stream(ctx, probe)
	if err != nil {
		return "", time.Time{}, err
	}
	return s.clockFromInfo(probe, stream.CachedInfo())
}

func (s *StreamSource) clockFromInfo(probe string, info *jetstream.StreamInfo) (string, time.Time, error) {
	if info == nil || info.Config.Name != probe || info.Config.Replicas != 1 || info.Cluster == nil || info.TimeStamp.IsZero() {
		return "", time.Time{}, fmt.Errorf("missing clustered clock provenance for %q", probe)
	}
	if info.Cluster.RaftGroup != "" || info.Cluster.Desired != nil || len(info.Cluster.Replicas) != 0 {
		return "", time.Time{}, fmt.Errorf("replicated or migrating clock probe %q", probe)
	}
	id := s.leaders[info.Cluster.Leader]
	if id == "" {
		return "", time.Time{}, fmt.Errorf("untrusted clock leader %q", info.Cluster.Leader)
	}
	return id, info.TimeStamp.UTC(), nil
}
