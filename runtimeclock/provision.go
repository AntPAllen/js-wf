package runtimeclock

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// EnsureProbes creates bounded R1 memory clock probes with unique placement tags.
// Existing incompatible streams are rejected, never adopted or reconfigured.
// Bootstrap while the declared servers are available, before starting writers.
func EnsureProbes(ctx context.Context, js jetstream.JetStream, c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	for _, p := range c.Probes {
		bound, cancel := context.WithTimeout(ctx, 3*time.Second)
		want := jetstream.StreamConfig{Name: p.Name, Subjects: []string{"wf.clock." + p.Name}, Storage: jetstream.MemoryStorage, Replicas: 1, MaxMsgs: 1, MaxBytes: 1, MaxMsgSize: 1, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld, Placement: &jetstream.Placement{Tags: []string{p.Tag}}, Metadata: map[string]string{"workflow_clock_domain": DeadlineDomain, "workflow_clock_identity": p.Identity}}
		stream, err := js.Stream(bound, p.Name)
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			stream, err = js.CreateStream(bound, want)
			if err != nil {
				stream, err = js.Stream(bound, p.Name)
			} // Verify a concurrent creator.
		}
		if err == nil {
			err = matchProbe(want, stream.CachedInfo(), p.Server)
		}
		cancel()
		if err != nil {
			return fmt.Errorf("clock probe %s: %w", p.Name, err)
		}
	}
	return nil
}

func matchProbe(want jetstream.StreamConfig, info *jetstream.StreamInfo, server string) error {
	if info == nil {
		return fmt.Errorf("missing clock probe info")
	}
	got := info.Config
	if got.Name != want.Name || !reflect.DeepEqual(got.Subjects, want.Subjects) || got.Storage != want.Storage || got.Replicas != 1 || got.MaxMsgs != 1 || got.MaxBytes != 1 || got.MaxMsgSize != 1 || got.Retention != want.Retention || got.Discard != want.Discard || got.MaxAge != 0 || got.NoAck || got.AllowMsgSchedules || got.AllowRollup || got.Mirror != nil || len(got.Sources) != 0 || got.Placement == nil || !reflect.DeepEqual(got.Placement, want.Placement) {
		return fmt.Errorf("clock probe configuration mismatch")
	}
	for key, value := range want.Metadata {
		if got.Metadata[key] != value {
			return fmt.Errorf("clock probe metadata mismatch")
		}
	}
	if info.Cluster == nil || info.Cluster.Leader != server || info.Cluster.RaftGroup != "" || info.Cluster.Desired != nil || len(info.Cluster.Replicas) != 0 {
		return fmt.Errorf("clock probe physical placement mismatch")
	}
	return nil
}
