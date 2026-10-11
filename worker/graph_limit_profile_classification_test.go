package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/testcluster"
)

func TestGraphLimitProfileErrorClassificationAndSnapshots(t *testing.T) {
	p := &graphLimitProfilePort{totals: map[string]graphLimitPortTiming{}}
	cases := []struct {
		kind string
		err  error
	}{
		{"conflict", graphpublication.ErrConflict},
		{"wrapped_conflict", fmt.Errorf("uncertain publication: %w", graphpublication.ErrConflict)},
		{"cancelled", context.Canceled}, {"deadline", context.DeadlineExceeded},
		{"timeout", nats.ErrTimeout}, {"revoked", graphpublication.ErrRevoked},
		{"api", &jetstream.APIError{Code: 503, Description: "test API failure"}},
		{"other", errors.New("unclassified test failure")},
	}
	for _, c := range cases {
		value, err := graphLimitMeasure(p, "CASRoot", func() (int, error) { return 42, c.err })
		if value != 42 || err != c.err {
			t.Fatal("measurement changed result", value, err)
		}
	}
	before := p.snapshot()
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			graphLimitMeasure(p, "CASRoot", func() (int, error) { return 0, graphpublication.ErrConflict })
		}()
	}
	workers.Wait()
	graphLimitMeasure(p, "CASRoot", func() (int, error) { return 0, nil })
	for _, c := range cases {
		if before["CASRoot"].ErrorKinds[c.kind] != 1 {
			t.Fatal("snapshot mutated", before)
		}
	}
	delta := p.delta(before)["CASRoot"]
	if delta.Calls != 33 || delta.Errors != 32 || len(delta.ErrorKinds) != 1 || delta.ErrorKinds["conflict"] != 32 {
		t.Fatal(delta)
	}
	// Mutating an external snapshot must not change the collector's counters.
	before["CASRoot"].ErrorKinds["conflict"] = 999
	if p.snapshot()["CASRoot"].ErrorKinds["conflict"] != 33 {
		t.Fatal("snapshot aliases collector")
	}
}

func TestGraphLimitProfileNativeCASClassification(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, graphpublication.AuthorityStreamConfig("PROFILE_AUTH", "wf.profile", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(ctx, graphpublication.NativeObjectStreamConfig("PROFILE_OBJECTS", 1)); err != nil {
		t.Fatal(err)
	}
	authority, err := graphpublication.OpenNativeAuthority(ctx, js, "PROFILE_AUTH", "wf.profile")
	if err != nil {
		t.Fatal(err)
	}
	profile, port, err := openGraphLimitProfile(ctx, authority, "PROFILE_OBJECTS", false)
	if err != nil {
		t.Fatal(err)
	}
	before := profile.snapshot()
	if _, err := port.CASRoot(ctx, "destination", 0, graphpublication.EmptyRoot()); err != nil {
		t.Fatal(err)
	}
	if _, err := port.CASRoot(ctx, "destination", 0, graphpublication.EmptyRoot()); err != graphpublication.ErrConflict {
		t.Fatal("stale native root did not reject exactly", err)
	}
	delta := profile.delta(before)["CASRoot"]
	if delta.Calls != 2 || delta.Errors != 1 || delta.ErrorKinds["conflict"] != 1 || len(delta.ErrorKinds) != 1 {
		t.Fatal(delta)
	}
	t.Logf("NATIVE_PROFILE_CLASSIFICATION calls=%d errors=%d conflict=%d wrapped_conflict=%d", delta.Calls, delta.Errors, delta.ErrorKinds["conflict"], delta.ErrorKinds["wrapped_conflict"])
}
