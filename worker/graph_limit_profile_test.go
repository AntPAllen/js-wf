package worker

import (
	"context"
	"sync"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

// graphLimitProfilePort observes completed native calls without changing their
// arguments, results, errors or retry policy. Totals include concurrent worker
// polling in the measured window. Summed durations can exceed wall time and do
// not identify individual server RPCs or establish a server-side cause.
type graphLimitProfilePort struct {
	*graphpublication.NativePort
	mu     sync.Mutex
	totals map[string]graphLimitPortTiming
}

type graphLimitPortTiming struct {
	Calls   uint64 `json:"calls"`
	Errors  uint64 `json:"errors"`
	TotalNS int64  `json:"total_ns"`
}

func graphLimitMeasure[T any](p *graphLimitProfilePort, name string, call func() (T, error)) (T, error) {
	started := time.Now()
	value, err := call()
	elapsed := time.Since(started).Nanoseconds()
	p.mu.Lock()
	timing := p.totals[name]
	timing.Calls++
	timing.TotalNS += elapsed
	if err != nil {
		timing.Errors++
	}
	p.totals[name] = timing
	p.mu.Unlock()
	return value, err
}

func (p *graphLimitProfilePort) snapshot() map[string]graphLimitPortTiming {
	p.mu.Lock()
	defer p.mu.Unlock()
	copy := make(map[string]graphLimitPortTiming, len(p.totals))
	for name, timing := range p.totals {
		copy[name] = timing
	}
	return copy
}

func (p *graphLimitProfilePort) delta(before map[string]graphLimitPortTiming) map[string]graphLimitPortTiming {
	after := p.snapshot()
	for name, timing := range after {
		old := before[name]
		after[name] = graphLimitPortTiming{Calls: timing.Calls - old.Calls, Errors: timing.Errors - old.Errors, TotalNS: timing.TotalNS - old.TotalNS}
	}
	return after
}

func (p *graphLimitProfilePort) ReadRoot(ctx context.Context, destination string) (graphpublication.Root, error) {
	return graphLimitMeasure(p, "ReadRoot", func() (graphpublication.Root, error) { return p.NativePort.ReadRoot(ctx, destination) })
}

func (p *graphLimitProfilePort) CASRoot(ctx context.Context, destination string, head uint64, next graphpublication.Root) (graphpublication.Root, error) {
	return graphLimitMeasure(p, "CASRoot", func() (graphpublication.Root, error) { return p.NativePort.CASRoot(ctx, destination, head, next) })
}

func (p *graphLimitProfilePort) ReadBlob(ctx context.Context, key string) (graphpublication.Record, error) {
	return graphLimitMeasure(p, "ReadBlob", func() (graphpublication.Record, error) { return p.NativePort.ReadBlob(ctx, key) })
}

func (p *graphLimitProfilePort) CASBlob(ctx context.Context, key string, revision uint64, next graphpublication.Fence) (graphpublication.Record, error) {
	return graphLimitMeasure(p, "CASBlob", func() (graphpublication.Record, error) { return p.NativePort.CASBlob(ctx, key, revision, next) })
}

func (p *graphLimitProfilePort) Put(ctx context.Context, name string, data []byte) error {
	_, err := graphLimitMeasure(p, "Put", func() (struct{}, error) { return struct{}{}, p.NativePort.Put(ctx, name, data) })
	return err
}

func (p *graphLimitProfilePort) Get(ctx context.Context, link retainedgraph.Link, maxBytes int) ([]byte, error) {
	return graphLimitMeasure(p, "Get", func() ([]byte, error) { return p.NativePort.Get(ctx, link, maxBytes) })
}

var _ graphpublication.Port = (*graphLimitProfilePort)(nil)

// Only explicitly indexed adapters expose complete owner discovery. The legacy
// profiler must keep its original interface set.
type graphLimitIndexedProfilePort struct {
	*graphLimitProfilePort
	indexed *graphpublication.OwnerIndexedNativePort
}

func (p *graphLimitIndexedProfilePort) BlobKeysForOwner(ctx context.Context, owner string) ([]string, error) {
	return graphLimitMeasure(p.graphLimitProfilePort, "BlobKeysForOwner", func() ([]string, error) { return p.indexed.BlobKeysForOwner(ctx, owner) })
}
func (p *graphLimitIndexedProfilePort) ValidateOwnerScope(ctx context.Context, owner, key string) error {
	_, err := graphLimitMeasure(p.graphLimitProfilePort, "ValidateOwnerScope", func() (struct{}, error) { return struct{}{}, p.indexed.ValidateOwnerScope(ctx, owner, key) })
	return err
}
func (p *graphLimitIndexedProfilePort) BeginOwnerScopeScan(ctx context.Context, owner string) (graphpublication.OwnerScopeScan, error) {
	return graphLimitMeasure(p.graphLimitProfilePort, "BeginOwnerScopeScan", func() (graphpublication.OwnerScopeScan, error) { return p.indexed.BeginOwnerScopeScan(ctx, owner) })
}

func openGraphLimitProfile(ctx context.Context, authority *graphpublication.NativeAuthority, bucket string, indexed bool) (*graphLimitProfilePort, graphpublication.Port, error) {
	if indexed {
		port, err := graphpublication.OpenOwnerIndexedNativePort(ctx, authority, bucket)
		if err != nil {
			return nil, nil, err
		}
		profile := &graphLimitProfilePort{NativePort: port.NativePort, totals: make(map[string]graphLimitPortTiming)}
		return profile, &graphLimitIndexedProfilePort{graphLimitProfilePort: profile, indexed: port}, nil
	}
	port, err := graphpublication.OpenNativePort(ctx, authority, bucket)
	if err != nil {
		return nil, nil, err
	}
	profile := &graphLimitProfilePort{NativePort: port, totals: make(map[string]graphLimitPortTiming)}
	return profile, profile, nil
}

var _ graphpublication.OwnerScopeScanPort = (*graphLimitIndexedProfilePort)(nil)
