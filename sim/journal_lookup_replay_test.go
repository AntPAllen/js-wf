package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

var errUnboundedLookup = errors.New("metadata request is unbounded")
var errPermanentLookup = errors.New("permanent metadata failure")

// Only metadata preparation is added to the existing narrow live-read port.
// Timeout returns are explicit transport faults advancing virtual time; they
// never sleep or wait for a real context deadline. Deadline presence/bounds are
// checked but wall-clock timestamps never enter the decision trace.
type journalLookupTransport struct {
	*JournalTransport
	faults   []string
	requests int
}

func (p *journalLookupTransport) Lookup(ctx context.Context) error {
	p.requests++
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Second {
		return errUnboundedLookup
	}
	kind := "ok"
	if len(p.faults) > 0 {
		kind, p.faults = p.faults[0], p.faults[1:]
	}
	if kind == "timeout" {
		if err := p.schedule.AdvanceMillis(2000); err != nil {
			return err
		}
	}
	p.schedule.RecordTransport(TransportEvent{Operation: "journal_metadata_lookup", Subject: "WF_JRN", Outcome: kind, AtMillis: p.schedule.NowMillis()})
	switch kind {
	case "ok":
		return nil
	case "timeout":
		return context.DeadlineExceeded
	case "no_responders":
		return nats.ErrNoResponders
	case "unavailable":
		return &jetstream.APIError{Code: 503, ErrorCode: 10008, Description: "unavailable"}
	case "permanent":
		return errPermanentLookup
	default:
		return fmt.Errorf("unknown metadata fault %q", kind)
	}
}

func runInitialJournalMetadata(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("journal_initial_metadata_recovery"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"normal", "one_timeout", "two_timeouts", "no_responders", "unavailable", "exhausted", "permanent", "canceled"})
	if err != nil {
		return trace, err
	}
	model := NewJournalTransport(schedule)
	lookup := &journalLookupTransport{JournalTransport: model}
	store := journal.NewWithPorts(model, lookup)
	ctx := context.Background()
	entries := []journal.Entry{{Index: 0, Kind: journal.Started}, {Index: 1, Kind: journal.StepRequested}, {Index: 2, Kind: journal.StepCompleted}, {Index: 3, Kind: journal.Completed}}
	var tail uint64
	for _, entry := range entries {
		tail, err = store.Append(ctx, "test", "metadata", entry, tail)
		if err != nil {
			return trace, err
		}
	}
	before := model.Messages("wf.jrn.test.metadata")
	initialRequests, wantMillis := 1, int64(0)
	switch mode {
	case "one_timeout":
		lookup.faults = []string{"timeout"}
		initialRequests, wantMillis = 2, 2025
	case "two_timeouts":
		lookup.faults = []string{"timeout", "timeout"}
		initialRequests, wantMillis = 3, 4050
	case "no_responders", "unavailable":
		lookup.faults = []string{mode}
		initialRequests, wantMillis = 2, 25
	case "exhausted":
		lookup.faults = []string{"timeout", "timeout", "timeout"}
		initialRequests, wantMillis = 3, 6050
	case "permanent":
		lookup.faults = []string{"permanent"}
	case "canceled":
		initialRequests = 0
	}
	readCtx := ctx
	if mode == "canceled" {
		child, cancel := context.WithCancel(ctx)
		cancel()
		readCtx = child
	}
	records, observed, readErr := store.Read(readCtx, "test", "metadata")
	if errors.Is(readErr, errUnboundedLookup) {
		return trace, errUnboundedLookup
	}
	if lookup.requests != initialRequests || schedule.NowMillis() != wantMillis {
		return trace, fmt.Errorf("metadata attempts=%d time=%d want=%d/%d", lookup.requests, schedule.NowMillis(), initialRequests, wantMillis)
	}
	failed := mode == "exhausted" || mode == "permanent" || mode == "canceled"
	if failed {
		want := context.DeadlineExceeded
		if mode == "permanent" {
			want = errPermanentLookup
		}
		if mode == "canceled" {
			want = context.Canceled
		}
		if !errors.Is(readErr, want) || len(records) != 0 || observed != 0 {
			return trace, fmt.Errorf("metadata failure mode=%s records=%d tail=%d err=%v", mode, len(records), observed, readErr)
		}
		// Failed preparation is not cached: the next Read can recover.
		records, observed, readErr = store.Read(ctx, "test", "metadata")
		if lookup.requests != initialRequests+1 {
			return trace, fmt.Errorf("failed lookup was cached")
		}
	}
	if readErr != nil || observed != tail || len(records) != 4 {
		return trace, fmt.Errorf("metadata read records=%d tail=%d err=%v", len(records), observed, readErr)
	}
	for i, record := range records {
		if !reflect.DeepEqual(record.Entry, entries[i]) || record.Sequence != uint64(i+1) {
			return trace, fmt.Errorf("metadata changed record %d", i)
		}
	}
	requests := lookup.requests
	lookup.faults = []string{"timeout"}
	if _, _, err := store.Read(ctx, "test", "metadata"); err != nil || lookup.requests != requests || len(lookup.faults) != 1 {
		return trace, fmt.Errorf("successful lookup not cached: %v", err)
	}
	if !reflect.DeepEqual(before, model.Messages("wf.jrn.test.metadata")) {
		return trace, fmt.Errorf("metadata fault mutated retained journal")
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_metadata_recovery", Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededInitialJournalMetadataReplay(t *testing.T) {
	if path := os.Getenv("SIM_JOURNAL_LOOKUP_OUT"); path != "" {
		trace, err := runInitialJournalMetadata(42, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	coverage := map[string]bool{}
	for seed := range seededSchedules(t) {
		trace, err := runInitialJournalMetadata(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "metadata-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("seed=%d trace=%s: %v", seed, path, err)
		}
		coverage[trace.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runInitialJournalMetadata(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, replayed) {
				t.Fatalf("metadata replay seed=%d: %v", seed, err)
			}
		}
	}
	if len(coverage) != 8 {
		t.Fatal("missing metadata fault modes", coverage)
	}
}
