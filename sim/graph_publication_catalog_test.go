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

	"js-wf/internal/graphpublication"
)

var graphCatalogModes = []string{"empty_only", "live_reader", "closed_grants", "renewal_wins", "unknown_catalog", "unknown_root", "unknown_expiry", "late_root", "published_root"}

func runGraphCatalog(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_destination_catalog"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphCatalogModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(s)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	start := now()
	if _, err = m.ReadRoot(ctx, "absence"); err != nil {
		return trace, err
	}
	for _, destination := range []string{"empty-a", "empty-b"} {
		head := uint64(0)
		for i := 0; i < 2; i++ {
			_, root, e := p.AcquireReader(ctx, destination, head, start.Add(time.Second))
			if e != nil {
				return trace, e
			}
			head = root.Head
		}
	}
	var reader graphpublication.Reader
	if mode != "empty_only" {
		prepared, e := p.PrepareAppend(ctx, "owner", 0, []byte("catalog-value"), [][]byte{[]byte("catalog-payload")}, start.Add(time.Second))
		if e != nil {
			return trace, e
		}
		root, e := p.Commit(ctx, prepared)
		if e != nil {
			return trace, e
		}
		reader, root, e = p.AcquireReader(ctx, "owner", root.Head, start.Add(3*time.Second))
		if e != nil {
			return trace, e
		}
		if mode != "published_root" {
			if e = p.RetireLive(ctx, "owner", root.Head); e != nil {
				return trace, e
			}
		}
	}
	if err = s.AdvanceMillis(1000); err != nil {
		return trace, err
	}
	if mode == "unknown_catalog" || mode == "unknown_root" || mode == "unknown_expiry" {
		op := "root_keys"
		fault := DropBeforeCommit
		if mode == "unknown_root" {
			op = "read_root"
		}
		if mode == "unknown_expiry" {
			op = "cas_root"
			fault = LoseAckAfterCommit
		}
		before, e := m.Objects(ctx)
		if e != nil {
			return trace, e
		}
		if err = m.QueueFault(op, fault); err != nil {
			return trace, err
		}
		if _, err = p.SweepWithReaders(ctx, now()); !errors.Is(err, ErrTransportLost) {
			return trace, fmt.Errorf("uncertain catalog collected: %v", err)
		}
		after, e := m.Objects(ctx)
		if e != nil || !reflect.DeepEqual(before, after) {
			return trace, fmt.Errorf("uncertain catalog deleted bytes")
		}
	}
	if mode == "late_root" {
		// The new root appears after the catalog census. It has no object grants
		// and remains registered for the next census, rather than being forgotten.
		m.PauseBefore("read_root", func() error { _, _, e := p.AcquireReader(ctx, "late", 0, start.Add(time.Second)); return e })
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	for _, destination := range []string{"empty-a", "empty-b"} {
		root, e := m.ReadRoot(ctx, destination)
		if e != nil || root.Head != 3 || len(root.Readers) != 0 || root.Schema != graphpublication.RetentionSchema {
			return trace, fmt.Errorf("empty pin not fenced: %v", e)
		}
	}
	if mode == "late_root" {
		root, e := m.ReadRoot(ctx, "late")
		if e != nil || len(root.Readers) != 1 {
			return trace, fmt.Errorf("late root not retained")
		}
		if _, e = p.SweepWithReaders(ctx, now()); e != nil {
			return trace, e
		}
		root, e = m.ReadRoot(ctx, "late")
		if e != nil || root.Head != 2 || len(root.Readers) != 0 {
			return trace, fmt.Errorf("next census missed late root")
		}
	}
	if mode != "empty_only" {
		got, e := p.ReadRetained(ctx, reader, 0, now())
		if e != nil || string(got.Data) != "catalog-value" || len(got.Blobs) != 1 {
			return trace, fmt.Errorf("catalog lost retained value: %v", e)
		}
		payload, e := m.Get(ctx, got.Blobs[0], 100)
		if e != nil || string(payload) != "catalog-payload" {
			return trace, fmt.Errorf("catalog lost retained payload")
		}
		root, e := m.ReadRoot(ctx, "owner")
		if e != nil {
			return trace, e
		}
		if mode == "renewal_wins" {
			if e = s.AdvanceMillis(2000); e != nil {
				return trace, e
			}
			m.PauseBefore("cas_root", func() error { _, e := p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); return e })
			if _, e = p.SweepWithReaders(ctx, now()); e != nil {
				return trace, e
			}
			if _, e = p.ReadRetained(ctx, reader, 0, now()); e != nil {
				return trace, e
			}
		}
		if mode == "closed_grants" {
			root, e = p.ReleaseReader(ctx, reader, root.Head)
			if e != nil {
				return trace, e
			}
			if _, e = p.SweepWithReaders(ctx, now()); e != nil {
				return trace, e
			}
			objects, e := m.Objects(ctx)
			if e != nil || len(objects) != 0 {
				return trace, fmt.Errorf("grants not closed")
			}
			if _, _, e = p.AcquireReader(ctx, "owner", root.Head, start.Add(2*time.Second)); e != nil {
				return trace, e
			}
		}
		if mode == "published_root" {
			if e = p.RetireLive(ctx, "owner", root.Head); e != nil {
				return trace, e
			}
		}
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = s.AdvanceMillis(10000); err != nil {
		return trace, err
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	keys, err := m.RootKeys(ctx)
	if err != nil {
		return trace, err
	}
	wantCount := 4
	if mode == "empty_only" {
		wantCount = 3
	}
	if mode == "late_root" {
		wantCount = 5
	}
	if len(keys) != wantCount {
		return trace, fmt.Errorf("catalog forgot destination: %v", keys)
	}
	for _, destination := range keys {
		root, e := m.ReadRoot(ctx, destination)
		if e != nil || root.Graph.Count != 0 || len(root.Readers) != 0 {
			return trace, fmt.Errorf("catalog final root retained pins: %v", e)
		}
		if destination == "absence" {
			if root.Head != 0 {
				return trace, fmt.Errorf("absence head advanced")
			}
		} else if root.Schema != graphpublication.RetentionSchema {
			return trace, fmt.Errorf("catalog downgraded schema")
		}
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("catalog final objects not drained")
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_destination_catalog", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphCatalogReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-catalog-failure-")
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphCatalog(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphCatalog(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("catalog replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_CATALOG_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphCatalogModes) {
		t.Fatalf("catalog coverage=%v", observed)
	}
	t.Logf("graph catalog: modes=%v; exact replay, empty pin fences, retained raw receipts and complete drain", observed)
}
