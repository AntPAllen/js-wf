package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"testing"
	"time"

	"js-wf/journal"
)

// Full cardinality CPU-only diagnostic. No transport, retained-state reads or
// workflow runtime is simulated here; this is not a capacity/integrity pass.
func TestAuditReductionFullPopulationCost(t *testing.T) {
	if os.Getenv("WF_AUDIT_REDUCTION_PROFILE") != "1" {
		t.Skip("opt-in 400k/4.8M decoder and reduction cost profile")
	}
	const population = 400000
	// Same JSON template as TestConcurrentStateR5PopulationCapacity: Started,
	// five requested/completed pairs, Completed. Subject order is contiguous
	// here, unlike the eight-publisher native population's interleaved order.
	entries := []journal.Entry{{Index: 0, Epoch: 1, Kind: journal.Started, WorkerID: "audit-proof"}}
	for n := uint64(0); n < 5; n++ {
		entries = append(entries, journal.Entry{Index: 1 + 2*n, Epoch: 1, Kind: journal.StepRequested, WorkerID: "audit-proof"}, journal.Entry{Index: 2 + 2*n, Epoch: 1, Kind: journal.StepCompleted, WorkerID: "audit-proof"})
	}
	entries = append(entries, journal.Entry{Index: 11, Epoch: 1, Kind: journal.Completed, WorkerID: "audit-proof", Payload: json.RawMessage(`"ok"`)})
	if len(entries) != 12 {
		t.Fatal("incorrect template")
	}
	encoded := make([][]byte, len(entries))
	for n, e := range entries {
		var err error
		encoded[n], err = json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
	}
	subjects := make([]string, population)
	for n := range subjects {
		subjects[n] = fmt.Sprintf("wf.jrn.audit.capacity-%06d", n)
	}
	lookup := func() ([]byte, error) { return []byte(`"ok"`), nil }
	oracle := make([]journal.Record, len(entries))
	for n, e := range entries {
		oracle[n] = journal.Record{Entry: e, Sequence: uint64(n + 1)}
	}
	if count, terminal, err := checkJournalRecords(subjects[0], oracle, lookup); err != nil || count != 12 || !terminal {
		t.Fatalf("template oracle: %d/%v/%v", count, terminal, err)
	}
	type result struct {
		Mode           string
		Records        int
		Report         Report
		ElapsedNS      int64
		AllocatedBytes uint64
		GCCycles       uint32
	}
	var results []result
	for _, mode := range []string{"decode_only", "reduce_predecoded", "decode_and_reduce"} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		began := time.Now()
		seen := 0
		var report Report
		var failure error
		pprof.Do(context.Background(), pprof.Labels("reduction_phase", mode), func(ctx context.Context) {
			groups := map[string]struct{}{}
			summaries := map[string]*journalAudit{}
			for n, subject := range subjects {
				for index, template := range entries {
					e := template
					if mode != "reduce_predecoded" {
						e = journal.Entry{}
						if err := journal.UnmarshalEntry(encoded[index], &e); err != nil {
							failure = err
							return
						}
						// Consume decoded fields in every iteration and verify the
						// expected identity; this validation is included in timing.
						if e.Index != uint64(index) || e.Epoch != 1 || e.WorkerID != "audit-proof" || e.Kind != template.Kind {
							failure = fmt.Errorf("decode identity %d/%d", n, index)
							return
						}
					}
					seen++
					if mode == "decode_only" {
						continue
					}
					groups[subject] = struct{}{}
					if summaries[subject] == nil {
						summaries[subject] = &journalAudit{}
					}
					summaries[subject].add(subject, journal.Record{Entry: e, Sequence: uint64(n*12 + index + 1)})
				}
			}
			if mode != "decode_only" {
				ordered := make([]string, 0, len(groups))
				for subject := range groups {
					ordered = append(ordered, subject)
				}
				sort.Strings(ordered)
				// Include the production sixteen-journal parallel finish/reduce
				// lifecycle, with an in-memory constant terminal lookup only.
				report, failure = auditJournals(ctx, ordered, func(_ context.Context, subject string) (int, bool, error) {
					return summaries[subject].finish(subject, lookup)
				})
			}
		})
		elapsed := time.Since(began)
		runtime.ReadMemStats(&after)
		if failure != nil || seen != population*12 {
			t.Fatalf("%s seen=%d err=%v", mode, seen, failure)
		}
		if mode != "decode_only" && report != (Report{Journals: population, Entries: population * 12, Terminal: population}) {
			t.Fatalf("%s report=%+v", mode, report)
		}
		results = append(results, result{mode, seen, report, int64(elapsed), after.TotalAlloc - before.TotalAlloc, after.NumGC - before.NumGC})
		t.Logf("reduction cost %+v", results[len(results)-1])
	}
	if root := os.Getenv("WF_AUDIT_BATCH_ROOT"); root != "" {
		// No cluster fixture creates a directory in this diagnostic.
		out := filepath.Join(root, t.Name())
		if err := os.MkdirAll(out, 0755); err != nil {
			t.Fatal(err)
		}
		data, err := json.MarshalIndent(struct {
			Results []result
			Scope   string
		}{results, "CPU-only full400k/4.8M JSON template; contiguous subjects, reused raw bytes and constant terminal lookup; no INV/state/transport/retained data/faults; profile instrumentation and decode identity checks included"}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "reduction-cost.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
