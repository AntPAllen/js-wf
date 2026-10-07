package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"js-wf/internal/blobpublication"
	"testing"
)

func runBlobPublication(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("blob_publication_protocol"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose([]string{"paused_commit", "paused_upload", "lost_upload_reply", "lost_commit_reply", "lost_fence_reply", "lost_close_reply", "lost_delete_reply", "lost_pin_reply", "lost_ready_reply", "shared_reference", "preserve_root", "close_race", "reset_head_control", "drop_before_commit", "partial_prepare"})
	if err != nil {
		return trace, err
	}
	m := NewBlobPublicationTransport(s)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	prepare := func(destination string, blobs ...[]byte) (blobpublication.Prepared, error) {
		return p.Prepare(ctx, destination, []byte("payload:"+destination), blobs, now().Add(time.Second))
	}
	expire := func() error {
		if err := s.AdvanceMillis(2000); err != nil {
			return err
		}
		_, err := p.Sweep(ctx, now())
		return err
	}
	commit := func(v blobpublication.Prepared) error { _, err := p.Commit(ctx, v); return err }
	requireLost := func(err error) error {
		if !errors.Is(err, ErrTransportLost) {
			return fmt.Errorf("expected lost transport reply, got %v", err)
		}
		return nil
	}
	requireConflict := func(err error) error {
		if !errors.Is(err, blobpublication.ErrConflict) {
			return fmt.Errorf("expected fenced publication, got %v", err)
		}
		return nil
	}
	shared := []byte("shared")
	private := []byte("private")
	switch mode {
	case "paused_upload":
		m.PauseBefore("put", expire)
		if _, err = prepare("a", shared); !errors.Is(err, blobpublication.ErrRevoked) {
			return trace, fmt.Errorf("paused upload resumed without revocation: %v", err)
		}
		if err = expire(); err != nil {
			return trace, err
		}
	case "lost_pin_reply", "lost_ready_reply", "lost_upload_reply", "partial_prepare":
		switch mode {
		case "lost_pin_reply":
			err = m.QueueFault("cas_blob", LoseAckAfterCommit)
		case "lost_ready_reply":
			m.PauseBefore("put", func() error { return m.QueueFault("cas_blob", LoseAckAfterCommit) })
		case "lost_upload_reply":
			err = m.QueueFault("put", LoseAckAfterCommit)
		case "partial_prepare":
			m.PauseBefore("put", func() error {
				m.PauseBefore("put", func() error { return m.QueueFault("put", LoseAckAfterCommit) })
				return nil
			})
		}
		if err != nil {
			return trace, err
		}
		_, err = prepare("a", shared, private)
		if err = requireLost(err); err != nil {
			return trace, err
		}
		// A subsequent successful invocation uses new physical attempt names.
		fresh, e := prepare("b", shared)
		if e != nil {
			return trace, e
		}
		if err = commit(fresh); err != nil {
			return trace, err
		}
		if err = expire(); err != nil {
			return trace, err
		}
	case "shared_reference":
		a, e := prepare("a", shared, private, shared)
		if e != nil {
			return trace, e
		}
		if err = commit(a); err != nil {
			return trace, err
		}
		b, e := prepare("b", shared)
		if e != nil {
			return trace, e
		}
		if err = commit(b); err != nil {
			return trace, err
		}
		root, e := m.ReadRoot(ctx, "a")
		if e != nil {
			return trace, e
		}
		if err = p.Retire(ctx, "a", root.Head); err != nil {
			return trace, err
		}
		if err = expire(); err != nil {
			return trace, err
		}
		objects, e := m.Objects(ctx)
		if e != nil || len(objects) != 1 {
			return trace, fmt.Errorf("shared-reference reclamation: objects=%d err=%v", len(objects), e)
		}
	case "preserve_root":
		a, e := prepare("a", shared)
		if e != nil {
			return trace, e
		}
		if err = commit(a); err != nil {
			return trace, err
		}
		old, e := m.ReadRoot(ctx, "a")
		if e != nil {
			return trace, e
		}
		pending, e := prepare("a", private)
		if e != nil {
			return trace, e
		}
		if err = expire(); err != nil {
			return trace, err
		}
		if err = requireConflict(commit(pending)); err != nil {
			return trace, err
		}
		current, e := m.ReadRoot(ctx, "a")
		if e != nil {
			return trace, e
		}
		if current.Token != old.Token || current.Head <= old.Head || !reflect.DeepEqual(current.Blobs, old.Blobs) || !reflect.DeepEqual(current.Data, old.Data) {
			return trace, fmt.Errorf("fencing replaced existing publication")
		}
	default:
		pending, e := prepare("a", shared)
		if e != nil {
			return trace, e
		}
		switch mode {
		case "paused_commit":
			if err = expire(); err != nil {
				return trace, err
			}
			err = requireConflict(commit(pending))
		case "lost_commit_reply":
			if err = m.QueueFault("cas_root", LoseAckAfterCommit); err == nil {
				err = commit(pending)
			}
		case "drop_before_commit":
			if err = m.QueueFault("cas_root", DropBeforeCommit); err != nil {
				return trace, err
			}
			if err = requireLost(commit(pending)); err == nil {
				err = commit(pending)
			}
		case "lost_fence_reply", "lost_close_reply", "lost_delete_reply":
			op := map[string]string{"lost_fence_reply": "cas_root", "lost_close_reply": "cas_blob", "lost_delete_reply": "delete"}[mode]
			if err = m.QueueFault(op, LoseAckAfterCommit); err != nil {
				return trace, err
			}
			if err = requireLost(expire()); err != nil {
				return trace, err
			}
			if err = requireConflict(commit(pending)); err == nil {
				err = expire()
			}
		case "close_race":
			m.PauseBefore("cas_blob", func() error {
				v, e := prepare("b", shared)
				if e != nil {
					return e
				}
				return commit(v)
			})
			err = expire()
		case "reset_head_control":
			if err = expire(); err != nil {
				return trace, err
			}
			m.mu.Lock()
			delete(m.roots, "a")
			m.event("reset_head", "a", 0, 0, nil, "unsafe_control")
			m.mu.Unlock()
			if err = commit(pending); err != nil {
				return trace, fmt.Errorf("unsafe adapter didn't reproduce: %v", err)
			}
			if err = m.CheckReferences(); err == nil {
				return trace, fmt.Errorf("unsafe adapter counterexample missing")
			}
			s.RecordTransport(TransportEvent{Operation: "publication_check_negative_control", Outcome: "dangling_reference_detected"})
			root, e := m.ReadRoot(ctx, "a")
			if e != nil {
				return trace, e
			}
			err = p.Retire(ctx, "a", root.Head)
		}
		if err != nil {
			return trace, err
		}
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}

	// Vary genuine enabled actor orders after the targeted fault: two publishers
	// compete for c's original head, d shares their objects, GC expires pending
	// intents, and retirement fences c. Every transport edge stays in the trace.
	c1, e := prepare("c", shared, private)
	if e != nil {
		return trace, e
	}
	c2, e := prepare("c", shared)
	if e != nil {
		return trace, e
	}
	d, e := prepare("d", shared)
	if e != nil {
		return trace, e
	}
	actors := []string{"commit-c1", "commit-c2", "commit-d", "collect", "retire-c"}
	for len(actors) > 0 {
		actor, e := s.Choose(actors)
		if e != nil {
			return trace, e
		}
		switch actor {
		case "commit-c1":
			err = commit(c1)
		case "commit-c2":
			err = commit(c2)
		case "commit-d":
			err = commit(d)
		case "collect":
			err = expire()
		case "retire-c":
			root, e := m.ReadRoot(ctx, "c")
			if e != nil {
				return trace, e
			}
			err = p.Retire(ctx, "c", root.Head)
		}
		if err != nil && !errors.Is(err, blobpublication.ErrConflict) {
			return trace, err
		}
		if err = m.CheckReferences(); err != nil {
			return trace, err
		}
		for i, name := range actors {
			if name == actor {
				actors = append(actors[:i], actors[i+1:]...)
				break
			}
		}
	}

	// Reclaim all remaining publications and crashed intentions, preserving heads.
	for _, destination := range []string{"a", "b", "c", "d"} {
		root, e := m.ReadRoot(ctx, destination)
		if e != nil {
			return trace, e
		}
		if err = p.Retire(ctx, destination, root.Head); err != nil {
			return trace, err
		}
	}
	if err = expire(); err != nil {
		return trace, err
	}
	objects, e := m.Objects(ctx)
	if e != nil || len(objects) != 0 {
		return trace, fmt.Errorf("terminal orphan leak: objects=%d err=%v", len(objects), e)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_blob_publication_protocol", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededBlobPublicationReplay(t *testing.T) {
	fail := func(seed int64, trace Trace, cause error) {
		t.Helper()
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-blob-publication-failure-")
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
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runBlobPublication(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runBlobPublication(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("replay differs: %v", err))
		}
		if root := os.Getenv("SIM_BLOB_PUBLICATION_ROOT"); root != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(root, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != 15 {
		t.Fatalf("publication coverage=%v", observed)
	}
	t.Logf("blob publication: modes=%v every generated trace exactly replayed; referenced bytes and terminal reclamation checked; unsafe head reset detected", observed)
}

func TestBlobPublicationTransportAuthorityCopies(t *testing.T) {
	m := NewBlobPublicationTransport(NewScheduler(1))
	ctx := context.Background()
	f := blobpublication.Fence{Generation: 1, Phase: "uploading", Intents: map[string]blobpublication.Intent{"token": {Root: "a", Expected: 0}}}
	record, err := m.CASBlob(ctx, "key", 0, f)
	if err != nil {
		t.Fatal(err)
	}
	delete(f.Intents, "token")
	delete(record.Fence.Intents, "token")
	current, err := m.ReadBlob(ctx, "key")
	if err != nil || len(current.Fence.Intents) != 1 {
		t.Fatal("aliased authority", err)
	}
	if _, err = m.CASBlob(ctx, "key", 0, f); !errors.Is(err, blobpublication.ErrConflict) {
		t.Fatal("stale metadata CAS admitted", err)
	}
	root := blobpublication.Root{Token: "token", Blobs: map[string]blobpublication.Reference{"key": {Generation: 1, Object: "object"}}, Data: []byte("value")}
	stored, err := m.CASRoot(ctx, "a", 0, root)
	if err != nil {
		t.Fatal(err)
	}
	delete(root.Blobs, "key")
	stored.Data[0] = 'X'
	delete(stored.Blobs, "key")
	actual, err := m.ReadRoot(ctx, "a")
	if err != nil || len(actual.Blobs) != 1 || string(actual.Data) != "value" {
		t.Fatal("aliased root", err)
	}
	if err = m.Protocol().Retire(ctx, "a", actual.Head); err != nil {
		t.Fatal(err)
	}
	if _, err = m.CASRoot(ctx, "a", 0, root); !errors.Is(err, blobpublication.ErrConflict) {
		t.Fatal("retirement reset authority", err)
	}
	if err = m.Put(ctx, "physical", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err = m.Put(ctx, "physical", []byte("two")); err == nil {
		t.Fatal("physical attempt reused")
	}
}
