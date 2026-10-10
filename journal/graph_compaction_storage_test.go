package journal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/sim"
)

type modeledCompactionStorage struct {
	*sim.KVTransport
	key    string
	writes int
}

func (p *modeledCompactionStorage) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	e, err := p.KVTransport.Get(ctx, key)
	return e.Value, e.Revision, err
}
func (p *modeledCompactionStorage) Create(ctx context.Context, key string, data []byte) (uint64, error) {
	p.key = key
	p.writes++
	return p.KVTransport.Create(ctx, key, data)
}
func (p *modeledCompactionStorage) Update(ctx context.Context, key string, data []byte, rev uint64) (uint64, error) {
	p.writes++
	return p.KVTransport.Update(ctx, key, data, rev)
}
func (p *modeledCompactionStorage) Delete(ctx context.Context, key string, rev uint64) error {
	p.writes++
	return p.KVTransport.Delete(ctx, key, rev)
}

func TestGraphCompactionStoredCheckpointCASAndRecovery(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"healthy", "missing", "create-drop", "create-lost", "update-drop", "update-lost", "delete-drop", "delete-lost", "stale-writer", "stale-read", "read-lost", "corrupt", "expired", "source-change", "pending-renewal", "pending-renewal-lost", "verify-progress", "cancel"} {
			t.Run(string(enc)+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				kv := &modeledCompactionStorage{KVTransport: sim.NewKVTransport(sim.NewScheduler(59), 0)}
				store, source, now, runtime := checkpointScanFixture(t, enc, []byte(`{"result":7}`), 4, checkpointScanFixtureOptions{indexed: true, archive: true, storage: kv})
				published := false
				defer func() {
					keys, e := source.RootKeys(ctx)
					if e != nil || len(keys) != 1 {
						t.Error(keys, e)
						return
					}
					root, e := source.ReadRoot(ctx, keys[0])
					if e != nil {
						t.Error(e)
						return
					}
					var cursor struct {
						RetainedFrom uint64 `json:"retained_from"`
					}
					if e := json.Unmarshal(root.Application, &cursor); e != nil {
						t.Error(e)
					}
					want := uint64(0)
					if published {
						want = 9
					}
					if cursor.RetainedFrom != want || len(root.Readers) != 0 {
						t.Error("unexpected publication/reader retention", cursor.RetainedFrom, want, root.Readers)
					}
				}()
				tail := runtime.Sequence + 1
				if err := store.PublishCheckpoint(ctx, "flow", "scan", runtime, tail); err != nil {
					t.Fatal(err)
				}
				op, err := store.BeginCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
				if err != nil {
					t.Fatal(err)
				}
				defer op.Close(ctx)
				for op.Phase() == "confirm" {
					if done, err := op.Advance(ctx, 2, 4); done || err != nil {
						t.Fatal(done, err)
					}
				}
				if done, err := op.Advance(ctx, 2, 4); done || err != nil {
					t.Fatal(done, err)
				}
				saved, err := op.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				fault := func(operation string, kind sim.KVFaultKind) {
					t.Helper()
					if err := kv.QueueFault(sim.KVFault{Operation: operation, Kind: kind}); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "missing" {
					loaded, rev, err := store.ResumeStoredCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
					if err != nil || loaded != nil || rev != 0 || kv.writes != 0 {
						t.Fatal(loaded, rev, err)
					}
					return
				}
				if mode == "cancel" {
					c, stop := context.WithCancel(ctx)
					stop()
					if _, err := op.SaveCheckpoint(c, 0); !errors.Is(err, context.Canceled) || kv.writes != 0 {
						t.Fatal(err, kv.writes)
					}
					return
				}
				if mode == "create-drop" {
					fault("create", sim.KVDropBeforeCommit)
				}
				if mode == "create-lost" {
					fault("create", sim.KVLoseAckAfterCommit)
				}
				rev, err := op.SaveCheckpoint(ctx, 0)
				if mode == "create-drop" || mode == "create-lost" {
					if !errors.Is(err, journal.ErrUnknown) || rev != 0 || kv.writes != 1 {
						t.Fatal("unknown create retried", rev, err, kv.writes)
					}
					loaded, r, e := store.ResumeStoredCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
					if mode == "create-drop" {
						if e != nil || loaded != nil || r != 0 {
							t.Fatal(loaded, r, e)
						}
					} else {
						if e != nil || loaded == nil || r == 0 {
							t.Fatal(loaded, r, e)
						}
						loaded.Close(ctx)
					}
					return
				}
				if err != nil || rev == 0 {
					t.Fatal(rev, err)
				}
				if _, err := op.SaveCheckpoint(ctx, 0); !errors.Is(err, journal.ErrStale) {
					t.Fatal("duplicate create", err)
				}
				originalRev := rev
				if mode == "update-drop" || mode == "update-lost" || mode == "stale-writer" || mode == "stale-read" || mode == "pending-renewal" || mode == "pending-renewal-lost" || mode == "verify-progress" {
					if mode == "pending-renewal" || mode == "pending-renewal-lost" {
						err = op.BeginIntentRenewal(ctx, now.Add(2*time.Minute))
					} else if mode == "verify-progress" {
						for op.Phase() == "stage" && err == nil {
							_, err = op.Advance(ctx, 2, 4)
						}
						if err == nil {
							_, err = op.Advance(ctx, 2, 4)
						}
					} else {
						_, err = op.Advance(ctx, 2, 4)
					}
					if err != nil {
						t.Fatal(err)
					}
					if mode == "update-drop" {
						fault("update", sim.KVDropBeforeCommit)
					}
					if mode == "update-lost" {
						fault("update", sim.KVLoseAckAfterCommit)
					}
					before := kv.writes
					updated, e := op.SaveCheckpoint(ctx, rev)
					if mode == "update-drop" || mode == "update-lost" {
						if !errors.Is(e, journal.ErrUnknown) || updated != 0 || kv.writes != before+1 {
							t.Fatal("unknown update retried", updated, e, kv.writes)
						}
					} else if e != nil || updated <= rev {
						t.Fatal(updated, e)
					}
					entry, e := kv.KVTransport.Get(ctx, kv.key)
					if e != nil {
						t.Fatal(e)
					}
					rev = entry.Revision
					if mode == "update-drop" {
						if rev != originalRev || !bytes.Equal(entry.Value, saved) {
							t.Fatal("dropped update changed bytes")
						}
					}
					if mode == "update-lost" {
						if rev <= originalRev || bytes.Equal(entry.Value, saved) {
							t.Fatal("lost update not committed")
						}
					}
					if mode == "stale-writer" {
						if _, err := op.SaveCheckpoint(ctx, originalRev); !errors.Is(err, journal.ErrStale) {
							t.Fatal("old writer overwrote newer progress", err)
						}
						if err := store.DeleteCompactionCheckpoint(ctx, "flow", "scan", runtime, tail, originalRev); !errors.Is(err, journal.ErrStale) {
							t.Fatal("old deletion removed new progress", err)
						}
					}
				}
				if mode == "pending-renewal-lost" {
					if err := source.QueueFault("cas_blob", sim.LoseAckAfterCommit); err != nil {
						t.Fatal(err)
					}
					for err == nil {
						_, err = op.Advance(ctx, 2, 4)
					}
					if !errors.Is(err, journal.ErrUnknown) {
						t.Fatal("grant loss not unknown", err)
					}
				}
				if mode == "delete-drop" || mode == "delete-lost" {
					kind := sim.KVDropBeforeCommit
					if mode == "delete-lost" {
						kind = sim.KVLoseAckAfterCommit
					}
					fault("delete", kind)
					before := kv.writes
					err := store.DeleteCompactionCheckpoint(ctx, "flow", "scan", runtime, tail, rev)
					if !errors.Is(err, journal.ErrUnknown) || kv.writes != before+1 {
						t.Fatal("unknown delete retried", err, kv.writes)
					}
					loaded, r, e := store.ResumeStoredCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
					if mode == "delete-lost" {
						if e != nil || loaded != nil || r != 0 {
							t.Fatal(loaded, r, e)
						}
					} else {
						if e != nil || loaded == nil || r != rev {
							t.Fatal(loaded, r, e)
						}
						loaded.Close(ctx)
					}
					return
				}
				if mode == "corrupt" {
					if _, err := kv.KVTransport.Update(ctx, kv.key, []byte(`{"schema":"foreign"}`), rev); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "expired" {
					*now = now.Add(2 * time.Minute)
				}
				if mode == "source-change" {
					v, err := store.OpenExisting(ctx, "flow", "scan", runtime.InvSeq)
					if err != nil {
						t.Fatal(err)
					}
					if err = v.Close(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "stale-read" {
					fault("get", sim.KVStaleRead)
				}
				if mode == "read-lost" {
					fault("get", sim.KVGetTransportLost)
				}
				// Discard the old operation; a newly loaded handle trusts no private progress.
				if err := op.Close(ctx); err != nil {
					t.Fatal(err)
				}
				protocol := source.GraphPublicationTransport.Protocol()
				protocol.Port = source
				store, err = journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Encoding: enc, Now: func() time.Time { return *now }, PinTTL: 4 * time.Second, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true, CompactionCheckpoints: kv})
				if err != nil {
					t.Fatal(err)
				}
				loaded, observed, e := store.ResumeStoredCheckpointCompaction(ctx, "flow", "scan", runtime, tail)
				if mode == "corrupt" || mode == "expired" || mode == "source-change" || mode == "read-lost" {
					if e == nil || loaded != nil {
						t.Fatal("unsafe input resumed", loaded, observed, e)
					}
					return
				}
				if e != nil || loaded == nil {
					t.Fatal(loaded, observed, e)
				}
				defer loaded.Close(ctx)
				if mode == "stale-read" {
					if observed != originalRev {
						t.Fatal("stale observation not exercised", observed, originalRev)
					}
					if _, e := loaded.SaveCheckpoint(ctx, observed); !errors.Is(e, journal.ErrStale) {
						t.Fatal("stale read overwrote new descriptor", e)
					}
					observed = rev
				} else if observed != rev {
					t.Fatal(observed, rev)
				}
				renewalBatches, verifyBatches := 0, 0
				for {
					phase := loaded.Phase()
					done, err := loaded.Advance(ctx, 2, 4)
					if err != nil {
						t.Fatal(err)
					}
					if phase == "renew" {
						renewalBatches++
					}
					if phase == "verify" {
						verifyBatches++
					}
					if done {
						break
					}
				}
				published = true
				if verifyBatches != 10 {
					t.Fatal("saved progress skipped private verification", verifyBatches)
				}
				if (mode == "pending-renewal" || mode == "pending-renewal-lost") && renewalBatches == 0 {
					t.Fatal("pending renewal was discarded")
				}
				if err := store.DeleteCompactionCheckpoint(ctx, "flow", "scan", runtime, tail, observed); err != nil {
					t.Fatal(err)
				}
				if _, err := kv.KVTransport.Get(ctx, kv.key); !errors.Is(err, jetstream.ErrKeyNotFound) {
					t.Fatal("completed progress remained", err)
				}
				keys, err := source.RootKeys(ctx)
				if err != nil || len(keys) != 1 {
					t.Fatal(keys, err)
				}
				root, err := source.ReadRoot(ctx, keys[0])
				if err != nil || len(root.Readers) != 0 {
					t.Fatal(root, err)
				}
				t.Logf("STORED_COMPACTION mode=%s verify_batches=%d renewal_batches=%d revision=%d readers=0", mode, verifyBatches, renewalBatches, observed)
			})
		}
	}
}

var _ journal.CompactionCheckpointPort = (*modeledCompactionStorage)(nil)
