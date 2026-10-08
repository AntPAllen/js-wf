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

var graphReaderModes = []string{"retire_read", "release", "expiry", "renew_wins", "collector_wins", "acquire_wins", "retire_wins", "lost_acquire", "lost_renew", "lost_release", "lost_expiry", "uncertain_ancestry"}

func runGraphReaders(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_publication_readers"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphReaderModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(s)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	start := now()
	current := func() (graphpublication.Root, error) { return m.ReadRoot(ctx, "owner") }
	prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("snapshot"), [][]byte{[]byte("payload")}, start.Add(time.Second))
	if err != nil {
		return trace, err
	}
	root, err := p.Commit(ctx, prepared)
	if err != nil {
		return trace, err
	}
	if mode == "lost_acquire" {
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	reader, root, err := p.AcquireReader(ctx, "owner", root.Head, start.Add(4*time.Second))
	if err != nil {
		return trace, err
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	old, err := p.ReadRetained(ctx, reader, 0, now())
	if err != nil || len(old.Blobs) != 1 || string(old.Data) != "snapshot" {
		return trace, fmt.Errorf("initial reader failed: %v", err)
	}
	if mode == "acquire_wins" {
		m.PauseBefore("cas_root", func() error { _, _, e := p.AcquireReader(ctx, "owner", root.Head, start.Add(4*time.Second)); return e })
		if err = p.RetireLive(ctx, "owner", root.Head); !errors.Is(err, graphpublication.ErrConflict) {
			return trace, fmt.Errorf("retire not fenced: %v", err)
		}
		root, err = current()
		if err != nil {
			return trace, err
		}
	}
	if mode == "retire_wins" {
		m.PauseBefore("cas_root", func() error { return p.RetireLive(ctx, "owner", root.Head) })
		if _, _, err = p.AcquireReader(ctx, "owner", root.Head, start.Add(4*time.Second)); !errors.Is(err, graphpublication.ErrConflict) {
			return trace, fmt.Errorf("acquisition not fenced: %v", err)
		}
	} else {
		if err = p.RetireLive(ctx, "owner", root.Head); err != nil {
			return trace, err
		}
	}
	root, err = current()
	if err != nil {
		return trace, err
	}
	if root.Schema != graphpublication.RetentionSchema || root.Graph.Count != 0 || len(root.Readers) == 0 {
		return trace, fmt.Errorf("retirement lost reader")
	}
	if _, err = p.Sweep(ctx, now()); err != nil {
		return trace, err
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	read, err := p.ReadRetained(ctx, reader, 0, now())
	if err != nil || !reflect.DeepEqual(read, old) {
		return trace, fmt.Errorf("retained record changed: %v", err)
	}
	payload, err := m.Get(ctx, read.Blobs[0], 100)
	if err != nil || string(payload) != "payload" {
		return trace, fmt.Errorf("retained payload changed: %v", err)
	}
	switch mode {
	case "release", "lost_release":
		if mode == "lost_release" {
			if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
				return trace, err
			}
		}
		if _, err = p.ReleaseReader(ctx, reader, root.Head); err != nil {
			return trace, err
		}
	case "lost_renew":
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		if _, err = p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); err != nil {
			return trace, err
		}
	case "renew_wins":
		if err = s.AdvanceMillis(4000); err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", func() error { _, e := p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); return e })
		if _, err = p.Sweep(ctx, now()); err != nil {
			return trace, err
		}
		if _, err = p.ReadRetained(ctx, reader, 0, now()); err != nil {
			return trace, err
		}
	case "collector_wins":
		if err = s.AdvanceMillis(4000); err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", func() error { _, e := p.Sweep(ctx, now()); return e })
		if _, err = p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); !errors.Is(err, graphpublication.ErrConflict) {
			return trace, fmt.Errorf("renewal not fenced: %v", err)
		}
		root, err = current()
		if err != nil {
			return trace, err
		}
		if _, err = p.RenewReader(ctx, reader, root.Head, start.Add(6*time.Second)); !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("reader resurrected: %v", err)
		}
	case "expiry", "lost_expiry":
		if err = s.AdvanceMillis(4000); err != nil {
			return trace, err
		}
		if mode == "lost_expiry" {
			if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
				return trace, err
			}
		}
		before, _ := m.Objects(ctx)
		_, err = p.Sweep(ctx, now())
		if mode == "lost_expiry" {
			if !errors.Is(err, ErrTransportLost) {
				return trace, fmt.Errorf("expiry unknown outcome lost: %v", err)
			}
			after, _ := m.Objects(ctx)
			if !reflect.DeepEqual(before, after) {
				return trace, fmt.Errorf("unknown expiry allowed deletes")
			}
		} else if err != nil {
			return trace, err
		}
	case "uncertain_ancestry":
		before, _ := m.Objects(ctx)
		if err = m.QueueFault("get", DropBeforeCommit); err != nil {
			return trace, err
		}
		if _, err = p.Sweep(ctx, now()); !errors.Is(err, ErrTransportLost) {
			return trace, fmt.Errorf("uncertain membership accepted: %v", err)
		}
		after, _ := m.Objects(ctx)
		if !reflect.DeepEqual(before, after) {
			return trace, fmt.Errorf("uncertainty allowed deletes")
		}
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if err = s.AdvanceMillis(10000); err != nil {
		return trace, err
	}
	root, err = current()
	if err != nil {
		return trace, err
	}
	if _, err = p.ExpireReaders(ctx, "owner", root.Head, now()); err != nil {
		return trace, err
	}
	if _, err = p.Sweep(ctx, now()); err != nil {
		return trace, err
	}
	root, err = current()
	if err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 || len(root.Readers) != 0 || root.Graph.Count != 0 || root.Schema != graphpublication.RetentionSchema {
		return trace, fmt.Errorf("reader final census failed: %v", err)
	}
	if _, err = p.ReadRetained(ctx, reader, 0, now()); !errors.Is(err, graphpublication.ErrRevoked) {
		return trace, fmt.Errorf("released reader readable: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_publication_readers", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphReadersReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-reader-failure-")
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
		generated, err := runGraphReaders(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphReaders(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("reader replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_READERS_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphReaderModes) {
		t.Fatalf("reader coverage=%v", observed)
	}
	t.Logf("reader retention: modes=%v; exact replay, retained raw receipt census and final reclamation", observed)
}
