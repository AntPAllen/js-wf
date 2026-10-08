package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type signalQueueCommitCut struct {
	*sim.GraphPublicationTransport
	mode  string
	fired bool
}

func (p *signalQueueCommitCut) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if !p.fired && p.mode != "" {
		for _, stream := range root.Streams {
			if stream.Name == "signal-queue" && stream.Graph.Count == 1 {
				p.fired = true
				fault := sim.DropBeforeCommit
				if p.mode == "queue_lost_readback" {
					fault = sim.LoseAckAfterCommit
					p.PauseBefore("cas_root", func() error { return p.QueueFault("read_root", sim.DropBeforeCommit) })
				}
				if err := p.QueueFault("cas_root", fault); err != nil {
					return root, err
				}
			}
		}
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}
func TestGraphCanonicalSignalPublicationRecoveryCuts(t *testing.T) {
	for _, mode := range []string{"healthy", "source_drop", "source_lost_ack", "queue_drop", "queue_lost_readback", "enqueue_drop", "enqueue_lost_ack"} {
		for seed := int64(1); seed <= 16; seed++ {
			t.Run(fmt.Sprintf("%s/%d", mode, seed), func(t *testing.T) {
				ctx := context.Background()
				sched := sim.NewScheduler(seed)
				model := sim.NewGraphPublicationTransport(sched)
				cut := &signalQueueCommitCut{GraphPublicationTransport: model}
				if mode == "queue_drop" || mode == "queue_lost_readback" {
					cut.mode = mode
				}
				protocol := model.Protocol()
				protocol.Port = cut
				now := time.Unix(1000, 0)
				store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return now }, PinTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				source := sim.NewSignalTransport(sched)
				c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(store)
				if err != nil {
					t.Fatal(err)
				}
				h, err := c.Start(ctx, "flow", "id", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "source_drop":
					err = source.QueueSignalFault(sim.SignalDropBeforeCommit)
				case "source_lost_ack":
					err = source.QueueSignalFault(sim.SignalLoseAckAfterCommit)
				case "enqueue_drop":
					err = source.QueueFault(sim.StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
				case "enqueue_lost_ack":
					err = source.QueueFault(sim.StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
				}
				if err != nil {
					t.Fatal(err)
				}
				seq, err := c.Signal(ctx, h.Type, h.ID, "signal", []byte("owned input"), "key")
				wantUnknown := mode == "source_drop" || mode == "queue_drop"
				wantEnqueue := mode == "enqueue_drop" || mode == "enqueue_lost_ack"
				if wantUnknown && !errors.Is(err, client.ErrSignalUnknown) || wantEnqueue && !errors.Is(err, client.ErrEnqueueUnknown) || !wantUnknown && !wantEnqueue && err != nil {
					t.Fatal(seq, err)
				}
				r := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: "key"}
				input, _, found, e := store.ReadSignalInput(ctx, r)
				if e != nil || !found {
					t.Fatal("reservation not recoverable", found, e)
				}
				recovered, e := c.RecoverSignal(ctx, r, input.Token)
				if e != nil || recovered == 0 || seq != 0 && seq != recovered {
					t.Fatal(seq, recovered, e)
				}
				b, body, found, e := store.ReadSignalBinding(ctx, r)
				if e != nil || !found || b.Sequence != recovered || b.Input != input || string(body) != "owned input" {
					t.Fatal(b, found, e)
				}
				if len(source.SignalFor(h.Type, h.ID, "signal")) != 1 || len(source.Runs()) != 2 {
					t.Fatal("duplicate operation or missing delivery", len(source.SignalFor(h.Type, h.ID, "signal")), len(source.Runs()))
				}
				if _, e = c.Signal(ctx, h.Type, h.ID, "signal", []byte("changed"), "key"); !errors.Is(e, client.ErrSignalMismatch) {
					t.Fatal(e)
				}
				source.PurgeSignal(recovered)
				if e = sched.AdvanceMillis((3 * time.Minute).Milliseconds()); e != nil {
					t.Fatal(e)
				}
				duplicate, e := c.Signal(ctx, h.Type, h.ID, "signal", []byte("owned input"), "key")
				if e != nil || duplicate != recovered || len(source.SignalFor(h.Type, h.ID, "signal")) != 0 {
					t.Fatal("purged source/expired dedup changed binding", duplicate, e)
				}
				if _, e = c.RecoverSignal(ctx, r, "foreign"); !errors.Is(e, client.ErrStaleGeneration) {
					t.Fatal(e)
				}
				for _, event := range sched.Trace().Transport {
					if event.Operation == "put_signal_blob" {
						t.Fatal("legacy blob written")
					}
				}
				if cut.mode != "" && !cut.fired {
					t.Fatal("queue cut not reached")
				}
				if e = model.CheckReferences(); e != nil {
					t.Fatal(e)
				}
			})
		}
	}
}
