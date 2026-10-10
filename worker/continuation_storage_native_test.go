package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

func TestNativeGraphStoredMaintenanceFreshWorkerRecovery(t *testing.T) {
	testNativeGraphStoredMaintenanceFreshWorkerRecovery(t, false)
}

func TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery(t *testing.T) {
	testNativeGraphStoredMaintenanceFreshWorkerRecovery(t, true)
}

func TestNativeGraphIndexedProfileStoredRenewalFreshWorkerRecovery(t *testing.T) {
	if _, ok := any(&graphLimitProfilePort{}).(graphpublication.OwnerScopePort); ok {
		t.Fatal("legacy profiler advertises indexed authority")
	}
	testNativeGraphStoredMaintenanceFreshWorkerRecovery(t, true, true)
}

func testNativeGraphStoredMaintenanceFreshWorkerRecovery(t *testing.T, indexed bool, profileArgs ...bool) {
	profiled := len(profileArgs) > 0 && profileArgs[0]
	for _, replicas := range []int{1, 3} {
		t.Run(fmt.Sprintf("R%d-domain", replicas), func(t *testing.T) {
			cluster, e := testcluster.StartWithDomain(t.TempDir(), replicas, "STOREDWORKER")
			if e != nil {
				t.Fatal(e)
			}
			defer cluster.Close()
			ctx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
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
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
			var executing atomic.Bool
			var scopedCensus, fullCensus atomic.Int64
			trace := jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(_ string, body []byte) {
				if !executing.Load() {
					return
				}
				var request struct {
					Filter string `json:"subjects_filter"`
				}
				if json.Unmarshal(body, &request) != nil {
					return
				}
				if strings.HasPrefix(request.Filter, "wf.graph.storedworker.owner.") {
					scopedCensus.Add(1)
				} else if request.Filter == "wf.graph.storedworker.>" {
					fullCensus.Add(1)
				}
			}})
			js, e := jetstream.NewWithDomain(cluster.Clients[0], "STOREDWORKER", trace)
			if e != nil {
				t.Fatal(e)
			}
			if e = provision.Ensure(ctx, js, replicas); e != nil {
				t.Fatal(e)
			}
			kv, e := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "STORED_PROGRESS", Replicas: replicas, Storage: jetstream.FileStorage, MaxValueSize: journal.MaxCompactionCheckpointBytes})
			if e != nil {
				t.Fatal(e)
			}
			cfg := journal.NativeGraphConfig{AuthorityStream: "STORED_AUTH", AuthorityPrefix: "wf.graph.storedworker", ObjectBucket: "STORED_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true}
			now := time.Now().UTC()
			if indexed {
				cfg.OwnerScopeIndex = true
				cfg.Now = func() time.Time { return now }
			}
			configs, e := journal.NativeGraphStreamConfigs(cfg, replicas)
			if e != nil {
				t.Fatal(e)
			}
			for _, config := range configs {
				if _, e = js.CreateStream(ctx, config); e != nil {
					t.Fatal(e)
				}
			}
			var profiles []*graphLimitProfilePort
			openStore := func() *journal.GraphStore {
				t.Helper()
				readCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				defer stop()
				bucket, e := js.KeyValue(readCtx, "STORED_PROGRESS")
				if e != nil {
					t.Fatal(e)
				}
				next := cfg
				next.CompactionCheckpoints = journal.NewCompactionCheckpointPort(bucket)
				store, e := journal.OpenNativeGraphStore(readCtx, js, next)
				if e != nil {
					t.Fatal(e)
				}
				if profiled {
					authority, e := graphpublication.OpenOwnerIndexedNativeAuthority(readCtx, js, next.AuthorityStream, next.AuthorityPrefix)
					if e != nil {
						t.Fatal(e)
					}
					core, port, e := openGraphLimitProfile(readCtx, authority, next.ObjectBucket, indexed)
					if e != nil {
						t.Fatal(e)
					}
					profiles = append(profiles, core)
					store, e = journal.NewGraphStore(journal.GraphConfig{Protocol: graphpublication.Protocol{Port: port}, Now: next.Now, PinTTL: next.PinTTL, IntentTTL: next.IntentTTL, CompactionIntentTTL: next.CompactionIntentTTL, Encoding: next.Encoding, PayloadReadLimit: next.PayloadReadLimit, CanonicalStarts: next.CanonicalStarts, CanonicalSignals: next.CanonicalSignals, CheckpointIndex: next.CheckpointIndex, ArchiveCheckpoints: next.ArchiveCheckpoints, CompactionCheckpoints: next.CompactionCheckpoints})
					if e != nil {
						t.Fatal(e)
					}
				}
				return store
			}
			store := openStore()
			c, e := client.New(js).WithGraphJournal(store)
			if e != nil {
				t.Fatal(e)
			}
			h, e := c.Start(ctx, "stored", "native", []byte(`7`))
			if e != nil {
				t.Fatal(e)
			}
			initial, entered := 0, 0
			handlers := map[string]Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				initial++
				for i := 0; i < 4; i++ {
					if e := c.SetState("padding", i); e != nil {
						return nil, e
					}
				}
				return nil, wf.Continue(c, "next", 42)
			}}
			stages := map[string]ContinuationHandler{"next": func(_ *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
				entered++
				if string(locals) != "42" {
					return nil, fmt.Errorf("changed locals")
				}
				return json.RawMessage(`42`), nil
			}}
			stageCounts, verifyCounts := [3]int{}, [3]int{}
			renewCounts := [3]int{}
			censusCounts := [3]int64{}
			renewRequested, crossedExpiry := false, false
			cut := false
			for delivery := 0; delivery < 3; delivery++ {
				runCtx, cancel := context.WithCancel(ctx)
				saves := 0
				observe := func(event OperationEvent) {
					if event.Error != "" {
						return
					}
					switch event.Operation {
					case "continuation_archive_stage_batch":
						stageCounts[delivery]++
						if indexed && delivery == 0 && !renewRequested {
							renewRequested = true
							now = now.Add(41 * time.Second)
						}
						if indexed && delivery == 1 && !crossedExpiry {
							crossedExpiry = true
							now = now.Add(20 * time.Second)
						}
					case "continuation_archive_renew_batch":
						renewCounts[delivery]++
						if indexed && delivery == 0 && renewCounts[0] == 1 {
							cut = true
							cancel()
						}
					case "continuation_archive_verify_batch":
						verifyCounts[delivery]++
					case "continuation_archive_checkpoint_save":
						saves++
						if !indexed && delivery == 0 && saves == 2 {
							cut = true
							cancel()
						}
					}
				}
				runner, e := New(ctx, js, fmt.Sprint("native-stored-", delivery), handlers, WithGraphJournal(openStore()), WithOperationObserver(observe))
				if e != nil {
					t.Fatal(e)
				}
				// Closed migration control: public graph continuation admission is unchanged.
				runner.continuations = map[string]map[string]ContinuationHandler{h.Type: stages}
				runner.continuationVerifyBatch = 2
				owner, e := runner.leases.Acquire(ctx, h.Type, h.ID, runner.ID)
				if e != nil {
					t.Fatal(e)
				}
				var noOp bool
				beforeCensus := scopedCensus.Load()
				executing.Store(true)
				e = runner.execute(runCtx, h.Type, h.ID, owner, time.Time{}, timerWakeup{}, &noOp, runner.deliveryOperations(h.Type, h.ID, 0, 0))
				executing.Store(false)
				censusCounts[delivery] = scopedCensus.Load() - beforeCensus
				cancel()
				releaseErr := owner.Release(ctx)
				closeErr := runner.Close()
				if releaseErr != nil || closeErr != nil {
					t.Fatal(releaseErr, closeErr)
				}
				if delivery == 0 {
					if !cut || !errors.Is(e, context.Canceled) {
						t.Fatal("cut missed", cut, e)
					}
				} else if e != nil {
					t.Fatal("fresh worker recovery", delivery, e)
				}
				if initial != 1 || entered != delivery/2 {
					t.Fatal("early/duplicate stage", delivery, initial, entered)
				}
				openAuthority := graphpublication.OpenNativeAuthority
				if indexed {
					openAuthority = graphpublication.OpenOwnerIndexedNativeAuthority
				}
				authority, e := openAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
				if e != nil {
					t.Fatal(e)
				}
				port, e := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
				if e != nil {
					t.Fatal(e)
				}
				keys, e := port.RootKeys(ctx)
				if e != nil || len(keys) != 1 {
					t.Fatal(keys, e)
				}
				root, e := port.ReadRoot(ctx, keys[0])
				if e != nil {
					t.Fatal(e)
				}
				var cursor struct {
					RetainedFrom uint64 `json:"retained_from"`
				}
				if e = json.Unmarshal(root.Application, &cursor); e != nil {
					t.Fatal(e)
				}
				want := uint64(9)
				if delivery == 0 {
					want = 0
				}
				if cursor.RetainedFrom != want || len(root.Readers) != 0 {
					t.Fatal("wrong authority/readers", cursor, root.Readers)
				}
				keys, e = kv.Keys(ctx)
				if delivery == 0 {
					if e != nil || len(keys) != 1 {
						t.Fatal(keys, e)
					}
					entry, e := kv.Get(ctx, keys[0])
					if e != nil {
						t.Fatal(e)
					}
					var saved struct {
						Stage   struct{ Next, Expected uint64 } `json:"stage"`
						RenewTo time.Time                       `json:"renew_to"`
					}
					if e = json.Unmarshal(entry.Value(), &saved); e != nil || saved.Stage.Next != 2 || saved.Stage.Expected != root.Head {
						t.Fatal("reader changed saved authority", saved, root.Head, e)
					}
					if indexed && saved.RenewTo.IsZero() {
						t.Fatal("pending renewal not stored before grant updates")
					}
				} else if !errors.Is(e, jetstream.ErrNoKeysFound) || len(keys) != 0 {
					t.Fatal("descriptor not deleted", keys, e)
				}
			}
			if stageCounts[0] != 1 || stageCounts[1] != 5 || verifyCounts[1] != 10 || initial != 1 || entered != 1 {
				t.Fatal("progress not recovered", stageCounts, verifyCounts, initial, entered)
			}
			if indexed && (!renewRequested || !crossedExpiry || renewCounts[0] != 1 || renewCounts[1] == 0) {
				t.Fatal("pending indexed renewal not resumed", renewRequested, crossedExpiry, renewCounts)
			}
			if indexed && (censusCounts[0] == 0 || censusCounts[1] == 0 || fullCensus.Load() != 0) {
				t.Fatal("indexed worker did not use owner-filtered discovery", censusCounts, fullCensus.Load())
			}
			if profiled {
				var scans, markers, census uint64
				for _, core := range profiles {
					timings := core.snapshot()
					scans += timings["BeginOwnerScopeScan"].Calls
					markers += timings["ValidateOwnerScope"].Calls
					census += timings["BlobKeysForOwner"].Calls
				}
				if scans != uint64(censusCounts[0]+censusCounts[1]+censusCounts[2]) || scans != 2 || markers == 0 || census != 0 {
					t.Fatal("profiler lost incremental discovery", scans, markers, census, censusCounts)
				}
				t.Logf("NATIVE_PROFILED_WORKER replicas=%d owner_scan_calls=%d marker_calls=%d static_census_calls=%d preserved_incremental=true", replicas, scans, markers, census)
			}
			t.Logf("NATIVE_STORED_WORKER replicas=%d indexed=%t fresh_workers=3 stage_batches=%v verify_batches=%v renew_batches=%v owner_census=%v full_census=%d initial_calls=1 stage_calls=1 pending_next=2 source_head_unchanged=true descriptors_after_recovery=0 readers=0", replicas, indexed, stageCounts, verifyCounts, renewCounts, censusCounts, fullCensus.Load())
		})
	}
}
