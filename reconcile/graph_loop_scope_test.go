package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
)

func TestGraphRepairScopedAdmissionBeforeStorage(t *testing.T) {
	valid := readerAdmissionComplete{readerAdmissionScan{readerAdmissionScope{scope: strings.Repeat("a", 64)}}}
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: graphpublication.Protocol{Port: valid}, CanonicalStarts: true, CanonicalSignals: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id       string
		interval time.Duration
		budget   int
	}{
		{"", time.Second, 1}, {"worker", 0, 1}, {"worker", 11 * time.Second, 1}, {"worker", time.Second, 0},
	} {
		js := &readerAdmissionJS{}
		if err := RunRepairLoopWithGraphJournal(context.Background(), js, tc.id, "graph-start", tc.interval, tc.budget, store, nil, nil, nil); err == nil || js.calls != 0 {
			t.Fatal(err, js.calls)
		}
	}
	if err := RunRepairLoopWithGraphJournal(context.Background(), nil, "worker", "graph-start", time.Second, 1, store, nil, nil, nil); err == nil {
		t.Fatal("nil JetStream accepted")
	}
}

func TestGraphRepairScopedCheckpointBoundary(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"start", "graph-start", "graph-signal", "graph-terminal", "graph-terminal-audit", "graph-continuation", "signal", "timer", "fallback-timer", "suspended"} {
		t.Run(kind, func(t *testing.T) {
			kv := &readerCheckpointKV{}
			p := graphRepairLoopPort{jetStreamLoopPort: &jetStreamLoopPort{state: kv}, scope: strings.Repeat("a", 64)}
			if next, rev, err := p.LoadCursor(ctx, kind); err != nil || next != 1 || rev != 0 {
				t.Fatal(next, rev, err)
			}
			kv.lost = true
			if _, err := p.SaveCursor(ctx, kind, 31, 0); !errors.Is(err, nats.ErrTimeout) {
				t.Fatal(err)
			}
			next, rev, err := p.LoadCursor(ctx, kind)
			if err != nil || next != 31 || rev != 1 {
				t.Fatal(next, rev, err)
			}
			other := p
			other.scope = strings.Repeat("b", 64)
			if _, _, err := other.LoadCursor(ctx, kind); err == nil {
				t.Fatal("copied scope accepted")
			}
			otherKind := "graph-terminal"
			if kind == otherKind {
				otherKind = "graph-start"
			}
			if _, _, err := p.LoadCursor(ctx, otherKind); err == nil {
				t.Fatal("copied kind accepted")
			}
			if _, err := p.SaveCursor(ctx, kind, 32, 0); !errors.Is(err, ErrCursorStale) {
				t.Fatal(err)
			}
			if _, err := p.SaveCursor(ctx, kind, 32, rev); err != nil {
				t.Fatal(err)
			}
			for _, raw := range []string{"31", "null", "{}", `{"Version":1,"Scope":"bad","Kind":"graph-start","Next":31}`} {
				kv.data = []byte(raw)
				if _, _, err := p.LoadCursor(ctx, kind); err == nil {
					t.Fatal("invalid checkpoint accepted", raw)
				}
			}
		})
	}
	if _, err := (graphRepairLoopPort{scope: strings.Repeat("A", 64)}).key("graph-start"); err == nil {
		t.Fatal("invalid scope accepted")
	}
	if _, err := (graphRepairLoopPort{scope: strings.Repeat("a", 64)}).key("unknown"); err == nil {
		t.Fatal("invalid kind accepted")
	}
}

func TestNativeGraphRepairNamespaceIsolation(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprint("R", replicas), func(t *testing.T) {
			cluster, err := testcluster.Start(t.TempDir(), replicas)
			if err != nil {
				t.Fatal(err)
			}
			defer cluster.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			if replicas > 1 {
				for {
					ready := false
					for _, s := range cluster.Servers {
						ready = ready || s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == replicas
					}
					if ready {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			js, err := jetstream.New(cluster.Clients[0])
			if err != nil {
				t.Fatal(err)
			}
			if err = provision.Ensure(ctx, js, replicas); err != nil {
				t.Fatal(err)
			}
			backend := &jetStreamLoopPort{js: js}
			if err = backend.Prepare(ctx); err != nil {
				t.Fatal(err)
			}
			kind := "graph-start"
			legacy, err := backend.Acquire(ctx, kind, "legacy-owner")
			if err != nil {
				t.Fatal(err)
			}
			defer legacy.Release(ctx)
			if _, err = backend.SaveCursor(ctx, kind, 999, 0); err != nil {
				t.Fatal(err)
			}
			// The old account-wide key collides independent of graph identity.
			if _, err = backend.Acquire(ctx, kind, "second-legacy-owner"); !errors.Is(err, lease.ErrHeld) {
				t.Fatal(err)
			}
			a := graphRepairLoopPort{jetStreamLoopPort: backend, scope: strings.Repeat("a", 64)}
			b := graphRepairLoopPort{jetStreamLoopPort: backend, scope: strings.Repeat("b", 64)}
			ownerA, err := a.Acquire(ctx, kind, "owner-a")
			if err != nil {
				t.Fatal(err)
			}
			defer ownerA.Release(ctx)
			ownerB, err := b.Acquire(ctx, kind, "owner-b")
			if err != nil {
				t.Fatal(err)
			}
			defer ownerB.Release(ctx)
			for _, p := range []graphRepairLoopPort{a, b} {
				if next, rev, err := p.LoadCursor(ctx, kind); err != nil || next != 1 || rev != 0 {
					t.Fatal("unscoped cursor imported", next, rev, err)
				}
			}
			key, _ := a.key(kind)
			a.jetStreamLoopPort = &jetStreamLoopPort{state: hiddenCursorAckKV{KeyValue: backend.state, key: "scan." + key, operation: "create"}}
			if _, err = a.SaveCursor(ctx, kind, 11, 0); !errors.Is(err, nats.ErrTimeout) {
				t.Fatal(err)
			}
			// Fresh adapters recover a committed lost-ack cursor independently.
			a.jetStreamLoopPort = &jetStreamLoopPort{js: js}
			if err = a.Prepare(ctx); err != nil {
				t.Fatal(err)
			}
			next, rev, err := a.LoadCursor(ctx, kind)
			if err != nil || next != 11 || rev == 0 {
				t.Fatal(next, rev, err)
			}
			if _, err = b.SaveCursor(ctx, kind, 77, 0); err != nil {
				t.Fatal(err)
			}
			if n, _, err := b.LoadCursor(ctx, kind); err != nil || n != 77 {
				t.Fatal(n, err)
			}
			if _, err = a.SaveCursor(ctx, kind, 12, 0); !errors.Is(err, ErrCursorStale) {
				t.Fatal(err)
			}
			if _, err = a.SaveCursor(ctx, kind, 12, rev); err != nil {
				t.Fatal(err)
			}
			if n, _, err := b.LoadCursor(ctx, kind); err != nil || n != 77 {
				t.Fatal(n, err)
			}
		})
	}
}
