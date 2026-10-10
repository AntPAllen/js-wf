package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"js-wf/client"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type workerCompactionStorage struct {
	*sim.KVTransport
	key            string
	saves, deletes int
}

func (p *workerCompactionStorage) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	if err := compactionStorageRequestBound(ctx, 15*time.Second); err != nil {
		return nil, 0, err
	}
	e, err := p.KVTransport.Get(ctx, key)
	return e.Value, e.Revision, err
}
func (p *workerCompactionStorage) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	if err := compactionStorageRequestBound(ctx, 3*time.Second); err != nil {
		return 0, err
	}
	p.key = key
	p.saves++
	return p.KVTransport.Create(ctx, key, data)
}
func (p *workerCompactionStorage) Update(ctx context.Context, key string, data []byte, rev uint64) (uint64, error) {
	if err := compactionStorageRequestBound(ctx, 3*time.Second); err != nil {
		return 0, err
	}
	p.saves++
	return p.KVTransport.Update(ctx, key, data, rev)
}
func (p *workerCompactionStorage) Delete(ctx context.Context, key string, rev uint64) error {
	if err := compactionStorageRequestBound(ctx, 3*time.Second); err != nil {
		return err
	}
	p.deletes++
	return p.KVTransport.Delete(ctx, key, rev)
}

func compactionStorageRequestBound(ctx context.Context, max time.Duration) error {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > max {
		return fmt.Errorf("descriptor request exceeds budget %s", max)
	}
	return nil
}

type storedInvocationOverride struct {
	worker.InvocationPort
	corrupt bool
}

func (p storedInvocationOverride) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	msg, err := p.InvocationPort.LastInvocation(ctx, subject)
	if err != nil || !p.corrupt {
		return msg, err
	}
	copy := *msg
	copy.Header = make(map[string][]string, len(msg.Header))
	for key, values := range msg.Header {
		copy.Header[key] = append([]string(nil), values...)
	}
	copy.Header.Set("Wf-Input-SHA256", strings.Repeat("0", 64))
	return &copy, nil
}

func TestGraphContinuationStoredMaintenanceAcrossDeliveries(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"normal", "slow-handoff", "slow-recovery", "cut-stage", "cut-verify", "cut-nodes", "cut-renew", "create-drop", "create-lost", "update-drop", "update-lost", "delete-drop", "delete-lost", "read-lost", "source-change", "expired", "corrupt", "wrong-invocation", "owner-loss-save", "owner-loss-delete", "cancel-save", "cancel-delete"} {
			if (mode == "slow-handoff" || mode == "slow-recovery") && encoding != journal.JSON {
				continue
			}
			t.Run(string(encoding)+"/"+mode, func(t *testing.T) {
				base := context.Background()
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				schedule := sim.NewScheduler(73)
				model := sim.NewGraphPublicationTransport(schedule)
				storage := &workerCompactionStorage{KVTransport: sim.NewKVTransport(schedule, 0)}
				makeStore := func() *journal.GraphStore {
					t.Helper()
					s, e := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, CompactionCheckpoints: storage, Now: func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }})
					if e != nil {
						t.Fatal(e)
					}
					return s
				}
				store := makeStore()
				transport := sim.NewWorkerTransport(schedule, 3*time.Second)
				c, e := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
				if e != nil {
					t.Fatal(e)
				}
				h, e := c.Start(base, "stored", "maintenance", []byte(`7`))
				if e != nil {
					t.Fatal(e)
				}
				leases := lease.NewWithKVPort(sim.NewKVTransport(schedule, 120*time.Second))
				initial, entered := 0, 0
				handlers := map[string]worker.Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
					initial++
					for i := 0; i < 4; i++ {
						if e := c.SetState("padding", i); e != nil {
							return nil, e
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
				fault := func(op string, kind sim.KVFaultKind) {
					t.Helper()
					if e := storage.QueueFault(sim.KVFault{Operation: op, Kind: kind}); e != nil {
						t.Fatal(e)
					}
				}
				switch mode {
				case "create-drop":
					fault("create", sim.KVDropBeforeCommit)
				case "create-lost":
					fault("create", sim.KVLoseAckAfterCommit)
				case "update-drop":
					fault("update", sim.KVDropBeforeCommit)
				case "update-lost":
					fault("update", sim.KVLoseAckAfterCommit)
				case "delete-drop":
					fault("delete", sim.KVDropBeforeCommit)
				case "delete-lost":
					fault("delete", sim.KVLoseAckAfterCommit)
				case "read-lost":
					fault("get", sim.KVGetTransportLost)
				}
				var replacement *lease.Lease
				trigger := false
				stageCounts, verifyCounts, renewCounts := make([]int, 5), make([]int, 5), make([]int, 5)
				delivery := 0
				delayed := make(map[string]bool)
				started := time.Now()
				renewRequested, crossedExpiry := false, false
				observe := func(event worker.OperationEvent) {
					if event.Error != "" {
						return
					}
					if !delayed[event.Operation] && (mode == "slow-handoff" && delivery == 0 && (event.Operation == "continuation_checkpoint_verify_batch" || event.Operation == "continuation_archive_stage_batch" || event.Operation == "continuation_archive_verify_batch") || mode == "slow-recovery" && delivery == 1 && (event.Operation == "continuation_archive_stage_batch" || event.Operation == "continuation_archive_verify_batch" || event.Operation == "continuation_archive_checkpoint_delete")) {
						delayed[event.Operation] = true
						time.Sleep(6 * time.Second)
					}
					switch event.Operation {
					case "continuation_archive_stage_batch":
						stageCounts[delivery]++
						if delivery == 0 && mode == "cut-renew" && !renewRequested {
							renewRequested = true
							if e := schedule.AdvanceMillis(41000); e != nil {
								t.Error(e)
							}
						}
						if delivery == 1 && mode == "cut-renew" && !crossedExpiry {
							crossedExpiry = true
							if e := schedule.AdvanceMillis(20000); e != nil {
								t.Error(e)
							}
						}
					case "continuation_archive_verify_batch":
						verifyCounts[delivery]++
					case "continuation_archive_renew_batch":
						renewCounts[delivery]++
					}
					if delivery != 0 || trigger {
						return
					}
					cut := (mode == "cut-stage" || mode == "slow-recovery") && event.Operation == "continuation_archive_checkpoint_save" && storage.saves == 2 || (mode == "source-change" || mode == "expired" || mode == "corrupt" || mode == "wrong-invocation") && event.Operation == "continuation_archive_checkpoint_save" && storage.saves == 2 || mode == "cut-verify" && verifyCounts[0] == 1 || mode == "cut-nodes" && verifyCounts[0] == 6 || mode == "cut-renew" && renewCounts[0] == 1 || mode == "cancel-save" && event.Operation == "continuation_archive_checkpoint_save" || mode == "cancel-delete" && verifyCounts[0] == 10
					if cut {
						trigger = true
						cancel()
						return
					}
					if mode == "owner-loss-save" && event.Operation == "continuation_archive_confirm_batch" || mode == "owner-loss-delete" && verifyCounts[0] == 10 {
						trigger = true
						if e := schedule.AdvanceMillis(121000); e != nil {
							t.Error(e)
						}
						var e error
						replacement, e = leases.Acquire(base, h.Type, h.ID, "replacement")
						if e != nil {
							t.Error(e)
						}
					}
				}
				execute := func(runCtx context.Context) error {
					t.Helper()
					legacy := sim.NewJournalTransport(schedule)
					w, e := worker.NewWithPorts(fmt.Sprint("stored-", delivery), handlers, worker.ModeledWorkerPorts{Journal: journal.NewWithPorts(legacy, legacy), Leases: leases, Outcome: sim.NewKVTransport(schedule, 0), Invocation: storedInvocationOverride{InvocationPort: transport.SignalTransport, corrupt: mode == "wrong-invocation" && delivery > 0}, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)}, worker.WithGraphJournal(makeStore()), worker.WithOperationObserver(observe))
					if e != nil {
						t.Fatal(e)
					}
					defer w.Close()
					owner, e := leases.Acquire(runCtx, h.Type, h.ID, fmt.Sprint("delivery-", delivery))
					if e != nil {
						t.Fatal(e)
					}
					e = worker.ExecuteGraphContinuationBatchesForTest(runCtx, w, h.Type, h.ID, owner, stages, 2)
					if releaseErr := owner.Release(base); releaseErr != nil && !errors.Is(releaseErr, lease.ErrLost) {
						t.Fatal(releaseErr)
					}
					return e
				}
				rootState := func(want uint64) {
					t.Helper()
					keys, e := model.RootKeys(base)
					if e != nil || len(keys) != 1 {
						t.Fatal(keys, e)
					}
					root, e := model.ReadRoot(base, keys[0])
					if e != nil {
						t.Fatal(e)
					}
					var cur struct {
						RetainedFrom uint64       `json:"retained_from"`
						Kind         journal.Kind `json:"kind"`
					}
					if e = json.Unmarshal(root.Application, &cur); e != nil {
						t.Fatal(e)
					}
					if cur.RetainedFrom != want || len(root.Readers) != 0 {
						t.Fatal("wrong authority/reader state", cur, root.Readers)
					}
					if entered == 0 && cur.Kind != journal.Suspended {
						t.Fatal("handoff changed", cur)
					}
				}
				e = execute(ctx)
				if mode == "normal" || mode == "slow-handoff" {
					if e != nil {
						t.Fatal(e)
					}
				} else if strings.HasPrefix(mode, "cut-") || mode == "slow-recovery" || mode == "source-change" || mode == "expired" || mode == "corrupt" || mode == "wrong-invocation" || mode == "cancel-save" || mode == "cancel-delete" {
					if !trigger || !errors.Is(e, context.Canceled) {
						t.Fatal("cut missed", trigger, e)
					}
				} else if mode == "owner-loss-save" || mode == "owner-loss-delete" {
					if !trigger || !errors.Is(e, lease.ErrLost) {
						t.Fatal(trigger, e)
					}
				} else if mode == "read-lost" {
					if !errors.Is(e, sim.ErrTransportLost) {
						t.Fatal(e)
					}
				} else if !errors.Is(e, journal.ErrUnknown) {
					t.Fatal("mutation outcome hidden", e)
				}
				if strings.HasPrefix(mode, "create-") && storage.saves != 1 || strings.HasPrefix(mode, "update-") && storage.saves != 2 || strings.HasPrefix(mode, "delete-") && storage.deletes != 1 || mode == "owner-loss-save" && storage.saves != 0 || (mode == "owner-loss-delete" || mode == "cancel-delete") && storage.deletes != 0 {
					t.Fatal("mutation continued past uncertainty or ownership loss", mode, storage.saves, storage.deletes)
				}
				if mode == "slow-handoff" && (len(delayed) != 3 || time.Since(started) < 18*time.Second) {
					t.Fatal("whole-handoff boundary not crossed", len(delayed), time.Since(started))
				}
				if initial != 1 || entered != 0 {
					t.Fatal("early stage admission", initial, entered)
				}
				published := mode == "normal" || mode == "slow-handoff" || strings.HasPrefix(mode, "delete-") || mode == "owner-loss-delete" || mode == "cancel-delete"
				if published {
					rootState(9)
				} else {
					rootState(0)
				}
				if replacement != nil {
					if e := replacement.Release(base); e != nil {
						t.Fatal(e)
					}
				}
				if mode == "source-change" {
					v, e := store.OpenExisting(base, h.Type, h.ID, h.InvSeq)
					if e != nil {
						t.Fatal(e)
					}
					if e = v.Close(base); e != nil {
						t.Fatal(e)
					}
				}
				if mode == "expired" {
					if e := schedule.AdvanceMillis(61000); e != nil {
						t.Fatal(e)
					}
				}
				if mode == "corrupt" {
					entry, e := storage.KVTransport.Get(base, storage.key)
					if e != nil {
						t.Fatal(e)
					}
					if _, e = storage.KVTransport.Update(base, storage.key, []byte(`{"schema":"foreign"}`), entry.Revision); e != nil {
						t.Fatal(e)
					}
				}
				delivery = 1
				e = execute(base)
				if mode == "corrupt" || mode == "wrong-invocation" {
					if !errors.Is(e, journal.ErrGap) || entered != 0 || storage.deletes != 0 {
						t.Fatal("corrupt input admitted", e, entered)
					}
					rootState(0)
					return
				}
				if mode == "source-change" || mode == "expired" {
					if !errors.Is(e, journal.ErrStale) || entered != 0 {
						t.Fatal("obsolete input resumed", e, entered)
					}
					rootState(0)
					delivery++
					e = execute(base)
				}
				if e != nil {
					t.Fatal("fresh delivery failed", e)
				}
				if mode == "slow-recovery" && (len(delayed) != 3 || time.Since(started) < 18*time.Second) {
					t.Fatal("recovery boundary not crossed", len(delayed), time.Since(started))
				}
				if !published && entered != 0 {
					t.Fatal("repaired delivery admitted stage", entered)
				}
				rootState(9)
				if mode == "cut-stage" && stageCounts[1] != 5 {
					t.Fatal("saved prefix was discarded", stageCounts)
				}
				if mode == "cut-verify" || mode == "cut-nodes" {
					if stageCounts[1] != 1 || verifyCounts[1] != 10 {
						t.Fatal("private verification was imported", stageCounts, verifyCounts)
					}
				}
				if mode == "cut-renew" && (!renewRequested || !crossedExpiry || renewCounts[1] == 0 || verifyCounts[1] != 10) {
					t.Fatal("renewal was not recovered", renewRequested, crossedExpiry, renewCounts, verifyCounts)
				}
				if entered == 0 {
					delivery++
					if e := execute(base); e != nil {
						t.Fatal(e)
					}
				}
				if initial != 1 || entered != 1 {
					t.Fatal("execution not exactly once", initial, entered)
				}
				keys, e := storage.Keys(base)
				if e != nil && !errors.Is(e, jetstream.ErrNoKeysFound) || len(keys) != 0 {
					t.Fatal("stored progress leaked", keys, e)
				}
				t.Logf("STORED_WORKER mode=%s wall=%s deliveries=%d stage_batches=%v verify_batches=%v renew_batches=%v saves=%d deletes=%d initial_calls=%d stage_calls=%d", mode, time.Since(started), delivery+1, stageCounts, verifyCounts, renewCounts, storage.saves, storage.deletes, initial, entered)
			})
		}
	}
}
