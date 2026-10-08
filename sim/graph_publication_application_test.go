package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"js-wf/internal/graphpublication"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var graphApplicationModes = []string{"ordinary", "lost_append", "lost_update", "dropped_update", "unknown_readback", "stale_append", "renewal_wins", "empty_expiry"}

func runGraphApplication(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_application_state"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphApplicationModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(s)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	start := now()
	app := []byte(`{"generation":1,"index":0,"epoch":2}`)
	prepared, err := p.PrepareAppendWithApplication(ctx, "journal", 0, []byte("entry"), [][]byte{[]byte("input")}, nil, start.Add(time.Second), app)
	if err != nil {
		return trace, err
	}
	if mode == "lost_append" {
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	root, err := p.Commit(ctx, prepared)
	if err != nil {
		return trace, err
	}
	if root.Graph.Count != 1 || !bytes.Equal(root.Application, app) {
		return trace, fmt.Errorf("append and application not atomic")
	}
	reader, root, err := p.AcquireReader(ctx, "journal", root.Head, start.Add(3*time.Second))
	if err != nil {
		return trace, err
	}
	var stale graphpublication.Prepared
	if mode == "stale_append" {
		stale, err = p.PrepareAppend(ctx, "journal", root.Head, []byte("stale"), nil, start.Add(time.Second))
		if err != nil {
			return trace, err
		}
	}
	updated := []byte(`{"generation":1,"index":0,"epoch":2,"terminal":true}`)
	switch mode {
	case "lost_update", "unknown_readback":
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		if mode == "unknown_readback" {
			m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
		}
	case "dropped_update":
		if err = m.QueueFault("cas_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	_, err = p.UpdateApplication(ctx, "journal", root.Head, updated)
	if mode == "dropped_update" || mode == "unknown_readback" {
		if !errors.Is(err, ErrTransportLost) {
			return trace, fmt.Errorf("uncertain application accepted: %v", err)
		}
	} else if err != nil {
		return trace, err
	}
	root, err = m.ReadRoot(ctx, "journal")
	if err != nil {
		return trace, err
	}
	if mode == "dropped_update" {
		updated = app
	}
	if !bytes.Equal(root.Application, updated) {
		return trace, fmt.Errorf("wrong application after fault")
	}
	if mode == "stale_append" {
		if _, err = p.Commit(ctx, stale); !errors.Is(err, graphpublication.ErrConflict) {
			return trace, fmt.Errorf("stale writer accepted: %v", err)
		}
	}
	if err = p.RetireLive(ctx, "journal", root.Head); err != nil {
		return trace, err
	}
	if err = s.AdvanceMillis(1000); err != nil {
		return trace, err
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	record, err := p.ReadRetained(ctx, reader, 0, now())
	if err != nil || string(record.Data) != "entry" || len(record.Blobs) != 1 {
		return trace, fmt.Errorf("retired reader lost value: %v", err)
	}
	payload, err := m.Get(ctx, record.Blobs[0], 100)
	if err != nil || string(payload) != "input" {
		return trace, fmt.Errorf("retired reader lost input")
	}
	root, err = m.ReadRoot(ctx, "journal")
	if err != nil {
		return trace, err
	}
	if root.Graph.Count != 0 || root.Schema != graphpublication.ApplicationSchema || !bytes.Equal(root.Application, updated) {
		return trace, fmt.Errorf("retirement lost lifecycle")
	}
	if mode == "renewal_wins" {
		if err = s.AdvanceMillis(2000); err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", func() error { _, e := p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); return e })
		if _, err = p.SweepWithReaders(ctx, now()); err != nil {
			return trace, err
		}
		if _, err = p.ReadRetained(ctx, reader, 0, now()); err != nil {
			return trace, err
		}
	}
	if err = s.AdvanceMillis(10000); err != nil {
		return trace, err
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	root, err = m.ReadRoot(ctx, "journal")
	if err != nil {
		return trace, err
	}
	if mode == "empty_expiry" {
		_, root, err = p.AcquireReader(ctx, "journal", root.Head, now())
		if err != nil {
			return trace, err
		}
		if _, err = p.SweepWithReaders(ctx, now()); err != nil {
			return trace, err
		}
		root, err = m.ReadRoot(ctx, "journal")
		if err != nil {
			return trace, err
		}
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 || len(root.Readers) != 0 || root.Graph.Count != 0 || root.Schema != graphpublication.ApplicationSchema || !bytes.Equal(root.Application, updated) {
		return trace, fmt.Errorf("final reclamation erased cursor or retained bytes: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_application_state", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphApplicationReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-application-failure-")
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
		generated, err := runGraphApplication(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphApplication(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("application replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_APPLICATION_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphApplicationModes) {
		t.Fatalf("application coverage=%v", observed)
	}
	t.Logf("graph application: modes=%v; exact replay, atomic descriptor, original-head fencing, retained reader, lifecycle survives complete object drain", observed)
}
