package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type blockedContinuationArchive struct {
	*sim.GraphPublicationTransport
	blocked bool
	mode    string
	cut     bool
}

type continuationNodeReader struct{ *sim.GraphPublicationTransport }

func (continuationNodeReader) Put(context.Context, string, []byte) (blobpublication.Reference, error) {
	return blobpublication.Reference{}, fmt.Errorf("read-only traversal")
}

func (p *blockedContinuationArchive) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	var cursor struct {
		RetainedFrom uint64          `json:"retained_from"`
		Checkpoint   json.RawMessage `json:"checkpoint"`
		Kind         journal.Kind    `json:"kind"`
	}
	if err := json.Unmarshal(root.Application, &cursor); err != nil {
		return graphpublication.Root{}, err
	}
	match := p.mode == "archive-cut" && cursor.RetainedFrom != 0 ||
		p.mode == "pointer-cut" && len(cursor.Checkpoint) != 0 ||
		p.mode == "suspension-cut" && cursor.Kind == journal.Suspended
	if p.blocked && match {
		p.cut = true
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

func TestGraphContinuationRepairsArchiveBeforeStage(t *testing.T) {
	for _, archive := range []bool{false, true} {
		for _, name := range []string{"healthy", "pointer-cut", "suspension-cut", "archive-cut"} {
			if !archive && name == "archive-cut" {
				continue
			}
			blocked := name != "healthy"
			t.Run(fmt.Sprintf("archive=%t/%s", archive, name), func(t *testing.T) {
				ctx := context.Background()
				schedule := sim.NewScheduler(23)
				model := sim.NewGraphPublicationTransport(schedule)
				port := &blockedContinuationArchive{GraphPublicationTransport: model, blocked: blocked, mode: name}
				protocol := model.Protocol()
				protocol.Port = port
				now := time.Unix(1000, 0)
				store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: archive, Now: func() time.Time { return now }})
				if err != nil {
					t.Fatal(err)
				}
				transport := sim.NewWorkerTransport(schedule, 3*time.Second)
				c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
				if err != nil {
					t.Fatal(err)
				}
				h, err := c.Start(ctx, "repair", "archive", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				leases := lease.NewWithKVPort(sim.NewKVTransport(schedule, 30*time.Second))
				initial, stagesEntered := 0, 0
				originalReceipts := map[string]bool{}
				handlers := map[string]worker.Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					initial++
					if err := c.SetState("value", 42); err != nil {
						return nil, err
					}
					return nil, wf.Continue(c, "next", 42)
				}}
				stages := map[string]worker.ContinuationHandler{"next": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
					stagesEntered++
					if string(locals) != "42" {
						t.Error("locals changed", string(locals))
					}
					return json.RawMessage(`42`), nil
				}}
				execute := func() error {
					legacy := sim.NewJournalTransport(schedule)
					w, err := worker.NewWithPorts("repair-worker", handlers, worker.ModeledWorkerPorts{Journal: journal.NewWithPorts(legacy, legacy), Leases: leases, Outcome: sim.NewKVTransport(schedule, 0), Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)}, worker.WithGraphJournal(store))
					if err != nil {
						t.Fatal(err)
					}
					defer w.Close()
					owner, err := leases.Acquire(ctx, h.Type, h.ID, "repair-worker")
					if err != nil {
						t.Fatal(err)
					}
					err = worker.ExecuteGraphContinuationForTest(ctx, w, h.Type, h.ID, owner, stages)
					if releaseErr := owner.Release(ctx); releaseErr != nil {
						t.Fatal(releaseErr)
					}
					return err
				}
				err = execute()
				if blocked {
					if err == nil || !port.cut {
						t.Fatal("handoff fault not exercised", err)
					}
					status, err := store.InspectStart(ctx, h.Type, h.ID)
					wantKind := journal.StepCompleted
					if name == "archive-cut" {
						wantKind = journal.Suspended
					}
					if err != nil || (status.Checkpoint != nil) != (name != "pointer-cut") || status.Kind != wantKind {
						t.Fatal("wrong crash boundary", status, err)
					}
					if name == "archive-cut" {
						keys, e := model.RootKeys(ctx)
						if e != nil || len(keys) != 1 {
							t.Fatal(keys, e)
						}
						root, e := model.ReadRoot(ctx, keys[0])
						if e != nil {
							t.Fatal(e)
						}
						if e = retainedgraph.Walk(ctx, continuationNodeReader{model}, root.Graph, func(link retainedgraph.Link, _ bool) error {
							originalReceipts[link.Reference.Object] = true
							return nil
						}); e != nil {
							t.Fatal(e)
						}
						if len(originalReceipts) == 0 {
							t.Fatal("missing original receipts")
						}
					}
					err = execute()
					if stagesEntered != 0 {
						t.Fatal("stage entered before handoff repair", stagesEntered, err)
					}
					if err == nil {
						t.Fatal("blocked handoff accepted")
					}
					port.blocked = false
					if err = execute(); err != nil {
						t.Fatal("repair failed", err)
					}
					if stagesEntered != 0 {
						t.Fatal("repair ran stage using original receipts")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if len(originalReceipts) > 0 {
					// Controlled collection is a test operation; production GC
					// remains off. Prove fresh resume needs no original receipt.
					now = now.Add(2 * time.Minute)
					if err = schedule.AdvanceMillis(120000); err != nil {
						t.Fatal(err)
					}
					if _, err = protocol.SweepWithReaders(ctx, now); err != nil {
						t.Fatal(err)
					}
					objects, err := model.Objects(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, object := range objects {
						if originalReceipts[object.Reference.Object] {
							t.Fatal("original receipt not physically reclaimed", object)
						}
					}
					t.Logf("HANDOFF_RECOVERY original_receipts_physically_reclaimed=%d before_stage=true", len(originalReceipts))
				}
				if err = execute(); err != nil {
					t.Fatal(err)
				}
				if initial != 1 || stagesEntered != 1 {
					t.Fatal("wrong handler counts", initial, stagesEntered)
				}
				status, err := store.InspectStart(ctx, h.Type, h.ID)
				if err != nil || status.Kind != journal.Completed {
					t.Fatal(status, err)
				}
				if err = model.CheckReferences(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
