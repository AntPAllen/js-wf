//go:build linux

package integrity

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Reuse the already race-qualified real WatchAll cancellation fixture. The
// full copied checker supplies all state entries and its native initial barrier.
// No relay publishes the held request, and no synthetic state is supplied.
type copiedCreationState struct {
	*nativeCreationState
	joined chan struct{}
}

func (s *copiedCreationState) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	first := s.calls == 0
	if first {
		defer close(s.joined)
	}
	watch, err := s.nativeCreationState.WatchAll(ctx, opts...)
	if first {
		s.proof["transport_joined"] = time.Now().UTC()
	}
	return watch, err
}

type copiedCreationJS struct {
	jetstream.JetStream
	state *copiedCreationState
}

func (s copiedCreationJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	if bucket != "WF_STATE" {
		return nil, fmt.Errorf("creation fault unexpectedly requested bucket %q", bucket)
	}
	deadline, ok := ctx.Deadline()
	if !ok || !s.state.parent.IsZero() {
		return nil, fmt.Errorf("creation fault requires one full-budget state admission")
	}
	s.state.parent = deadline
	s.state.proof["deadline"] = deadline.UTC()
	s.state.proof["parent_budget_ns"] = int64(20 * time.Second)
	return s.state, nil
}

func prepareCopiedCreation(t *testing.T, ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, root string) *copiedCreationState {
	t.Helper()
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.ClientURL(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	if err := proxy.EnableTrafficFileTrace(filepath.Join(root, "creation-wire.jsonl"), 16<<20); err != nil {
		t.Fatal(err)
	}
	if err := proxy.HoldFirstConsumerCreate("$JS.API.", "KV_WF_STATE"); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(proxy.URL(), nats.IgnoreDiscoveredServers(), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	firstJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstJS.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	proof := map[string]any{
		"scope":     "Full original checkpoint4160 R5 copied creation stall plus cold cursor-owner recovery; no concurrent24h/historical cause qualification",
		"server_id": nc.ConnectedServerId(), "server_name": nc.ConnectedServerName(), "upstream_url": cluster.ClientURL(0),
	}
	return &copiedCreationState{nativeCreationState: &nativeCreationState{KeyValue: state, t: t, first: first, nc: nc, proxy: proxy, proof: proof}, joined: make(chan struct{})}
}
