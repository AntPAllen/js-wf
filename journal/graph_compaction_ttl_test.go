package journal_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

type expiryRecordingPort struct {
	graphpublication.Port
	expiries []time.Time
}

func (p *expiryRecordingPort) CASBlob(ctx context.Context, k string, rev uint64, f graphpublication.Fence) (graphpublication.Record, error) {
	for _, intent := range f.Intents {
		p.expiries = append(p.expiries, intent.Expires)
	}
	return p.Port.CASBlob(ctx, k, rev, f)
}

func TestGraphCompactionSeparateIntentLifetime(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, tc := range []struct {
			name                             string
			appendTTL, compactTTL, effective time.Duration
		}{
			{"default", 0, 0, time.Minute},
			{"inherit", 6 * time.Second, 0, 6 * time.Second},
			{"long", 6 * time.Second, 30 * time.Minute, 30 * time.Minute},
			{"short", time.Minute, 6 * time.Second, 6 * time.Second},
		} {
			t.Run(string(enc)+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				var recorder *expiryRecordingPort
				storage := &modeledCompactionStorage{KVTransport: sim.NewKVTransport(sim.NewScheduler(23), 0)}
				store, _, now, runtime := checkpointScanFixture(t, enc, []byte(`{"result":7}`), 4, checkpointScanFixtureOptions{
					indexed: true, archive: true, intentTTL: tc.appendTTL, compactionTTL: tc.compactTTL, storage: storage,
					wrap: func(p *checkpointCostPort) graphpublication.Port {
						recorder = &expiryRecordingPort{Port: p}
						return recorder
					},
				})
				appendTTL := tc.appendTTL
				if appendTTL == 0 {
					appendTTL = time.Minute
				}
				if len(recorder.expiries) == 0 {
					t.Fatal("no append grants")
				}
				for _, expiry := range recorder.expiries {
					if !expiry.Equal(now.Add(appendTTL)) {
						t.Fatal("append lifetime changed", expiry)
					}
				}
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
				saved, err := op.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				var descriptor struct{ Stage struct{ Expires time.Time } }
				if err = json.Unmarshal(saved, &descriptor); err != nil {
					t.Fatal(err)
				}
				start := *now
				if _, err := op.SaveCheckpoint(ctx, 0); err != nil {
					t.Fatal(err)
				}
				if !descriptor.Stage.Expires.Equal(start.Add(tc.effective)) {
					t.Fatal("wrong initial compaction expiry", descriptor.Stage.Expires)
				}
				*now = start.Add(tc.effective - tc.effective/3 - time.Nanosecond)
				if began, err := op.BeginIntentRenewalIfNeeded(ctx); began || err != nil {
					t.Fatal("early renewal", began, err)
				}
				*now = now.Add(time.Nanosecond)
				if began, err := op.BeginIntentRenewalIfNeeded(ctx); !began || err != nil {
					t.Fatal("missing renewal", began, err)
				}
				pending, err := op.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				var renewal struct {
					RenewTo time.Time `json:"renew_to"`
				}
				if err = json.Unmarshal(pending, &renewal); err != nil || !renewal.RenewTo.Equal(now.Add(tc.effective)) {
					t.Fatal("wrong renewal lifetime", renewal, err)
				}
				// Saved input uses its original expiry even with a larger configured TTL.
				*now = descriptor.Stage.Expires
				if recovered, _, err := store.ResumeStoredCheckpointCompaction(ctx, "flow", "scan", runtime, tail); recovered != nil || err == nil {
					t.Fatal("expired descriptor revived", recovered, err)
				}
			})
		}
	}
}

func TestGraphCompactionNegativeIntentLifetime(t *testing.T) {
	if _, err := journal.NativeGraphStreamConfigs(journal.NativeGraphConfig{AuthorityStream: "AUTH", AuthorityPrefix: "wf.graph", ObjectBucket: "OBJECTS", CompactionIntentTTL: -time.Second}, 1); err == nil {
		t.Fatal("negative native lifetime accepted")
	}
	model := sim.NewGraphPublicationTransport(sim.NewScheduler(23))
	if _, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CompactionIntentTTL: -time.Second}); err == nil {
		t.Fatal("negative lifetime accepted")
	}
}
