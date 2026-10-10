package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

func TestGraphContinuationMaintenanceLeaseAndCancellation(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"normal", "owner-loss-pointer", "owner-loss-confirm", "owner-loss-stage", "owner-loss-verify", "owner-loss-nodes", "cancel-pointer", "cancel-confirm", "cancel-stage", "cancel-verify", "cancel-nodes"} {
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				base := context.Background()
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				schedule := sim.NewScheduler(23)
				model := sim.NewGraphPublicationTransport(schedule)
				store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, Now: func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }})
				if err != nil {
					t.Fatal(err)
				}
				transport := sim.NewWorkerTransport(schedule, 3*time.Second)
				c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
				if err != nil {
					t.Fatal(err)
				}
				h, err := c.Start(ctx, "maintenance", "batches", []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				leases := lease.NewWithKVPort(sim.NewKVTransport(schedule, 30*time.Second))
				owner, err := leases.Acquire(ctx, h.Type, h.ID, "original")
				if err != nil {
					t.Fatal(err)
				}
				initial, entered := 0, 0
				handlers := map[string]worker.Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					initial++
					for i := 0; i < 4; i++ {
						if err := c.SetState("padding", i); err != nil {
							return nil, err
						}
					}
					return nil, wf.Continue(c, "next", 42)
				}}
				stages := map[string]worker.ContinuationHandler{"next": func(_ *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
					entered++
					if string(locals) != "42" {
						return nil, fmt.Errorf("changed locals")
					}
					return json.RawMessage(`42`), nil
				}}
				var replacement *lease.Lease
				triggered := false
				pointerBatches, confirmBatches, stageBatches, verifyBatches := 0, 0, 0, 0
				lastPointerIndex := uint64(0)
				observe := func(event worker.OperationEvent) {
					if event.Error != "" {
						return
					}
					phase := ""
					switch event.Operation {
					case "continuation_checkpoint_verify_batch":
						pointerBatches++
						phase = "pointer"
						if event.JournalIndex-lastPointerIndex > 2 {
							t.Error("pointer batch exceeded budget", lastPointerIndex, event.JournalIndex)
						}
						lastPointerIndex = event.JournalIndex
					case "continuation_archive_confirm_batch":
						confirmBatches++
						phase = "confirm"
					case "continuation_archive_stage_batch":
						stageBatches++
						phase = "stage"
					case "continuation_archive_verify_batch":
						verifyBatches++
						phase = "verify"
						if verifyBatches == 6 {
							phase = "nodes"
						}
					}
					if phase == "" || triggered || !strings.HasSuffix(mode, "-"+phase) {
						return
					}
					triggered = true
					if strings.HasPrefix(mode, "cancel-") {
						cancel()
						return
					}
					if err := schedule.AdvanceMillis(31000); err != nil {
						t.Error(err)
						return
					}
					var err error
					replacement, err = leases.Acquire(base, h.Type, h.ID, "replacement")
					if err != nil {
						t.Error("replacement acquisition", err)
					}
				}
				legacy := sim.NewJournalTransport(schedule)
				w, err := worker.NewWithPorts("maintenance", handlers, worker.ModeledWorkerPorts{Journal: journal.NewWithPorts(legacy, legacy), Leases: leases, Outcome: sim.NewKVTransport(schedule, 0), Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)}, worker.WithGraphJournal(store), worker.WithOperationObserver(observe))
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				err = worker.ExecuteGraphContinuationBatchesForTest(ctx, w, h.Type, h.ID, owner, stages, 2)
				if mode == "normal" {
					if err != nil || initial != 1 || entered != 0 || pointerBatches != 6 || confirmBatches != 1 || stageBatches != 6 || verifyBatches != 10 {
						t.Fatal("healthy maintenance incomplete", err, initial, entered, pointerBatches, confirmBatches, stageBatches, verifyBatches)
					}
				} else {
					want := context.Canceled
					if strings.HasPrefix(mode, "owner-loss-") {
						want = lease.ErrLost
					}
					if !triggered || !errors.Is(err, want) || entered != 0 {
						t.Fatal("lost/canceled maintenance continued", triggered, err, entered)
					}
				}
				keys, e := model.RootKeys(base)
				if e != nil || len(keys) != 1 {
					t.Fatal(keys, e)
				}
				root, e := model.ReadRoot(base, keys[0])
				if e != nil {
					t.Fatal(e)
				}
				var cursor struct {
					Checkpoint   json.RawMessage `json:"checkpoint"`
					RetainedFrom uint64          `json:"retained_from"`
					Kind         journal.Kind    `json:"kind"`
				}
				if e = json.Unmarshal(root.Application, &cursor); e != nil {
					t.Fatal(e)
				}
				if len(root.Readers) != 0 {
					t.Fatal("abandoned readers remained", root.Readers)
				}
				if strings.HasSuffix(mode, "-pointer") {
					if len(cursor.Checkpoint) != 0 || cursor.RetainedFrom != 0 || cursor.Kind != journal.StepCompleted {
						t.Fatal("partial verification published handoff", cursor)
					}
				} else if mode != "normal" {
					if len(cursor.Checkpoint) == 0 || cursor.RetainedFrom != 0 || cursor.Kind != journal.Suspended {
						t.Fatal("abandoned archive published relocation", cursor)
					}
				} else if cursor.RetainedFrom != 9 || cursor.Kind != journal.Suspended {
					t.Fatal("wrong archive boundary", cursor)
				}
				if releaseErr := owner.Release(base); releaseErr != nil && !errors.Is(releaseErr, lease.ErrLost) {
					t.Fatal(releaseErr)
				}
				if replacement != nil {
					if err := replacement.Release(base); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "normal" {
					next, e := leases.Acquire(base, h.Type, h.ID, "next")
					if e != nil {
						t.Fatal(e)
					}
					// The original stage runs once; the next delivery uses relocated references.
					err = worker.ExecuteGraphContinuationForTest(base, w, h.Type, h.ID, next, stages)
					if err != nil || initial != 1 || entered != 1 {
						t.Fatal("relocated resume failed", err, initial, entered)
					}
					if err = next.Release(base); err != nil {
						t.Fatal(err)
					}
				}
				t.Logf("WORKER_MAINTENANCE mode=%s pointer_batches=%d confirm_batches=%d stage_batches=%d verify_batches=%d readers=0 initial_calls=%d stage_calls=%d retained_from=%d", mode, pointerBatches, confirmBatches, stageBatches, verifyBatches, initial, entered, cursor.RetainedFrom)
			})
		}
	}
}
