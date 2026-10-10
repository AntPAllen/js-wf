package worker_test

import (
	"context"
	"errors"
	"testing"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/worker"
)

type uncertainCompactionRelease struct {
	*sim.GraphPublicationTransport
	mode            string
	armed, loseRead bool
	releases, reads int
}

func (p *uncertainCompactionRelease) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	p.reads++
	if p.loseRead {
		p.loseRead = false
		return graphpublication.Root{}, sim.ErrTransportLost
	}
	return p.GraphPublicationTransport.ReadRoot(ctx, key)
}
func (p *uncertainCompactionRelease) CASRoot(ctx context.Context, key string, head uint64, next graphpublication.Root) (graphpublication.Root, error) {
	if p.armed && len(next.Readers) == 0 {
		p.armed = false
		p.releases++
		p.loseRead = true
		if p.mode == "lost" {
			if _, e := p.GraphPublicationTransport.CASRoot(ctx, key, head, next); e != nil {
				return graphpublication.Root{}, e
			}
		}
		return graphpublication.Root{}, sim.ErrTransportLost
	}
	if len(next.Readers) == 0 {
		p.releases++
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, next)
}
func TestGraphDeliveryCompactionReleaseUncertaintyIsSticky(t *testing.T) {
	for _, mode := range []string{"drop", "lost", "cancel-before"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			model := sim.NewGraphPublicationTransport(sim.NewScheduler(97))
			port := &uncertainCompactionRelease{GraphPublicationTransport: model, mode: mode}
			protocol := model.Protocol()
			protocol.Port = port
			store, e := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = store.Begin(ctx, "flow", "release", 1); e != nil {
				t.Fatal(e)
			}
			view, e := store.Open(ctx, "flow", "release", 1)
			if e != nil {
				t.Fatal(e)
			}
			release, closeDelivery, attempted := worker.GraphCompactionDeliveryReleaseForTest(view)
			port.releases = 0
			port.armed = true
			if mode == "cancel-before" {
				cancelled, stop := context.WithCancel(ctx)
				stop()
				if e = release(cancelled); !errors.Is(e, context.Canceled) || attempted() {
					t.Fatal(e, attempted())
				}
				port.armed = false
				if e = closeDelivery(ctx); e != nil || port.releases != 1 {
					t.Fatal("cleanup suppressed before mutation", e, port.releases)
				}
				return
			}
			e = release(ctx)
			if !errors.Is(e, sim.ErrTransportLost) || port.releases != 1 {
				t.Fatal("release fault missed", e, port.releases)
			}
			reads := port.reads
			if repeated := closeDelivery(ctx); repeated != e || port.releases != 1 || port.reads != reads {
				t.Fatal("cleanup retried uncertain release", e, repeated, port.releases, port.reads, reads)
			}
			keys, e := model.RootKeys(ctx)
			if e != nil || len(keys) != 1 {
				t.Fatal(keys, e)
			}
			root, e := model.ReadRoot(ctx, keys[0])
			if e != nil {
				t.Fatal(e)
			}
			want := 1
			if mode == "lost" {
				want = 0
			}
			if len(root.Readers) != want {
				t.Fatal("fault outcome changed", root.Readers, want)
			}
			t.Logf("COMPACTION_RELEASE mode=%s attempts=1 hidden_reads=0 retained_readers=%d", mode, want)
		})
	}
}
