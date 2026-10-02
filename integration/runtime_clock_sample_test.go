package integration_test

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"js-wf/runtimeclock"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

// A client attached to node two samples three distinct physical leaders. The
// source identity must come from each forwarded response, not that attachment.
func TestRuntimeClockSamplesForwardedIndependentLeaders(t *testing.T) {
	tags := map[int][]string{0: {"clock-a"}, 1: {"clock-b"}, 2: {"clock-c"}}
	c, err := testcluster.StartWithServerTags(t.TempDir(), 3, tags)
	if err != nil {
		t.Fatal(err)
	}
	all, cluster := setupCluster(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	trusted := map[string]string{}
	var expected []string
	for _, server := range cluster.Servers {
		trusted[server.Name()] = server.ID()
		expected = append(expected, server.ID())
	}
	sort.Strings(expected)
	probes := []string{"CLOCK_PROBE_A", "CLOCK_PROBE_B", "CLOCK_PROBE_C"}
	for i, name := range probes {
		stream, err := all[2].CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{"clock.probe." + name}, Replicas: 1, Storage: jetstream.MemoryStorage, Placement: &jetstream.Placement{Tags: tags[i]}})
		if err != nil {
			t.Fatal(err)
		}
		info := stream.CachedInfo()
		if info.Cluster == nil || info.Cluster.Leader != cluster.Servers[i].Name() {
			t.Fatalf("probe placement: %+v", info)
		}

	}
	source, err := runtimeclock.NewStreamSource(all[2], trusted)
	if err != nil {
		t.Fatal(err)
	}
	// Mutation of the supplied topology cannot alter the source after construction.
	for name := range trusted {
		delete(trusted, name)
	}
	sampler, err := runtimeclock.NewSampler(source, probes, 1, time.Second, 20*time.Millisecond, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	reading, err := sampler.Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bounds, err := reading.Bounds()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bounds.Sources, expected) {
		t.Fatalf("forwarded identities=%v want=%v", bounds.Sources, expected)
	}
	if bounds.Upper.Sub(bounds.Lower) > 3*time.Second {
		t.Fatalf("unbounded clock interval: %+v", bounds)
	}
	// Two actual streams on one physical server must not become a quorum.
	alias := "CLOCK_PROBE_ALIAS"
	if _, err := all[2].CreateStream(ctx, jetstream.StreamConfig{Name: alias, Subjects: []string{"clock.probe.alias"}, Replicas: 1, Storage: jetstream.MemoryStorage, Placement: &jetstream.Placement{Tags: tags[0]}}); err != nil {
		t.Fatal(err)
	}
	aliases, _ := runtimeclock.NewSampler(source, []string{probes[0], alias}, 1, time.Second, 20*time.Millisecond, 3*time.Second)
	if _, err := aliases.Sample(ctx); err == nil {
		t.Fatal("one server accepted as independent clock agreement")
	}
	if _, _, err := source.ReadClock(ctx, "WF_RUN"); err == nil {
		t.Fatal("replicated stream admitted as clock source")
	}
	t.Logf("independent sources=%v uncertainty=%s", bounds.Sources, bounds.Upper.Sub(bounds.Lower))
}
