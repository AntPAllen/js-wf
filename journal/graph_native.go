package journal

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
)

// NativeGraphConfig names explicitly provisioned, isolated graph stores.
// All writers/readers for a workflow must select the same configuration.
// Opening does not import legacy data or enable production online collection.
type NativeGraphConfig struct {
	AuthorityStream string
	AuthorityPrefix string
	ObjectBucket    string
	// ExpectedReplicas optionally requires both stores to have this replica count.
	// Zero accepts the adapter's structurally safe positive replica counts.
	ExpectedReplicas int
	Now              func() time.Time
	PinTTL           time.Duration
	IntentTTL        time.Duration
	Encoding         Encoding
	PayloadReadLimit int
	CanonicalStarts  bool
	CanonicalSignals bool
}

var nativeGraphName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var nativeGraphPrefix = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)

func (cfg NativeGraphConfig) validate() error {
	if !nativeGraphName.MatchString(cfg.AuthorityStream) || !nativeGraphPrefix.MatchString(cfg.AuthorityPrefix) || !nativeGraphName.MatchString(cfg.ObjectBucket) || len(cfg.ObjectBucket) > 32 || cfg.AuthorityStream == "OBJ_"+cfg.ObjectBucket {
		return fmt.Errorf("invalid native graph namespace")
	}
	if cfg.ExpectedReplicas < 0 || cfg.ExpectedReplicas > 5 || cfg.PinTTL < 0 || cfg.IntentTTL < 0 || cfg.PayloadReadLimit < 0 || int64(cfg.PayloadReadLimit) == math.MaxInt64 || validateEncoding(cfg.Encoding) != nil || cfg.CanonicalSignals && !cfg.CanonicalStarts {
		return fmt.Errorf("invalid native graph journal configuration")
	}
	return nil
}

// NativeGraphStreamConfigs returns configurations for NEW isolated stores.
// Provision them explicitly with CreateStream, never by updating an existing
// authority or legacy object bucket. OpenNativeGraphStore validates existing
// configurations and rejects unsafe stores without changing them.
func NativeGraphStreamConfigs(cfg NativeGraphConfig, replicas int) ([]jetstream.StreamConfig, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if replicas < 1 || replicas > 5 || cfg.ExpectedReplicas != 0 && cfg.ExpectedReplicas != replicas {
		return nil, fmt.Errorf("graph replicas must be between 1 and 5")
	}
	return []jetstream.StreamConfig{
		graphpublication.AuthorityStreamConfig(cfg.AuthorityStream, cfg.AuthorityPrefix, replicas),
		graphpublication.NativeObjectStreamConfig(cfg.ObjectBucket, replicas),
	}, nil
}

// OpenNativeGraphStore opens an existing native graph journal using only public
// SDK types. Admission reads stream configuration; it never creates or changes
// stores. Zero TTLs and payload budget select NewGraphStore's defaults.
func OpenNativeGraphStore(ctx context.Context, js jetstream.JetStream, cfg NativeGraphConfig) (*GraphStore, error) {
	if js == nil {
		return nil, fmt.Errorf("nil JetStream for native graph journal")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.ExpectedReplicas != 0 {
		for _, name := range []string{cfg.AuthorityStream, "OBJ_" + cfg.ObjectBucket} {
			stream, err := js.Stream(ctx, name)
			if err != nil {
				return nil, err
			}
			info, err := stream.Info(ctx)
			if err != nil {
				return nil, err
			}
			if info.Config.Replicas != cfg.ExpectedReplicas {
				return nil, fmt.Errorf("native graph stream %s has %d replicas, expected %d", name, info.Config.Replicas, cfg.ExpectedReplicas)
			}
		}
	}
	authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
	if err != nil {
		return nil, err
	}
	port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
	if err != nil {
		return nil, err
	}
	return NewGraphStore(GraphConfig{Protocol: graphpublication.Protocol{Port: port}, Now: cfg.Now, PinTTL: cfg.PinTTL, IntentTTL: cfg.IntentTTL, Encoding: cfg.Encoding, PayloadReadLimit: cfg.PayloadReadLimit, CanonicalStarts: cfg.CanonicalStarts, CanonicalSignals: cfg.CanonicalSignals})
}
