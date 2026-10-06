//go:build linux

package integration_test

import (
	"context"
	"sync/atomic"

	"js-wf/internal/handlecache"

	"github.com/nats-io/nats.go/jetstream"
)

// Share successful client handles during one final audit. No records, stream
// state, snapshots or timestamps are cached: operations on these handles still
// go to JetStream. Failed lookups remain retryable and waiters can cancel.
type matrixLatencyMetadataJS struct {
	jetstream.JetStream
	journal        handlecache.Cache[jetstream.Stream]
	signals        handlecache.Cache[jetstream.Stream]
	state          handlecache.Cache[jetstream.KeyValue]
	objects        handlecache.Cache[jetstream.ObjectStore]
	journalLookups atomic.Uint64
	signalLookups  atomic.Uint64
	stateLookups   atomic.Uint64
	objectLookups  atomic.Uint64
}

func (j *matrixLatencyMetadataJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	var cache *handlecache.Cache[jetstream.Stream]
	var counter *atomic.Uint64
	switch name {
	case "WF_JRN":
		cache, counter = &j.journal, &j.journalLookups
	case "WF_SIG":
		cache, counter = &j.signals, &j.signalLookups
	default:
		return j.JetStream.Stream(ctx, name)
	}
	return cache.Get(ctx, func(request context.Context) (jetstream.Stream, error) {
		counter.Add(1)
		return j.JetStream.Stream(request, name)
	})
}

func (j *matrixLatencyMetadataJS) KeyValue(ctx context.Context, name string) (jetstream.KeyValue, error) {
	if name != "WF_STATE" {
		return j.JetStream.KeyValue(ctx, name)
	}
	return j.state.Get(ctx, func(request context.Context) (jetstream.KeyValue, error) {
		j.stateLookups.Add(1)
		return j.JetStream.KeyValue(request, name)
	})
}

func (j *matrixLatencyMetadataJS) ObjectStore(ctx context.Context, name string) (jetstream.ObjectStore, error) {
	if name != "WF_BLOB" {
		return j.JetStream.ObjectStore(ctx, name)
	}
	return j.objects.Get(ctx, func(request context.Context) (jetstream.ObjectStore, error) {
		j.objectLookups.Add(1)
		return j.JetStream.ObjectStore(request, name)
	})
}

func (j *matrixLatencyMetadataJS) lookupCounts() map[string]uint64 {
	return map[string]uint64{"journal": j.journalLookups.Load(), "signals": j.signalLookups.Load(), "state": j.stateLookups.Load(), "objects": j.objectLookups.Load()}
}
