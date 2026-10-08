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

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

var graphPublicationModes = []string{"append_inherit", "owned_reuse", "paused_commit", "paused_upload", "lost_pin", "lost_ready", "lost_put", "lost_root", "lost_fence", "lost_close", "lost_delete", "commit_wins", "collector_wins", "read_root_lost", "read_blob_lost", "get_lost", "foreign_reuse"}

func runGraphPublication(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("graph_publication_protocol"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphPublicationModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(schedule)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	root, err := m.ReadRoot(ctx, "owner")
	if err != nil {
		return trace, err
	}
	base, err := p.PrepareAppend(ctx, "owner", root.Head, []byte("base"), [][]byte{[]byte("shared")}, now().Add(time.Second))
	if err != nil {
		return trace, err
	}
	root, err = p.Commit(ctx, base)
	if err != nil {
		return trace, err
	}
	record, err := retainedgraph.Read(ctx, graphReadStore{m}, root.Graph, 0)
	if err != nil || len(record.Blobs) != 1 {
		return trace, fmt.Errorf("missing initial payload: %v", err)
	}
	source := graphpublication.OwnedPayload{Index: 0, Link: record.Blobs[0]}
	prepare := func(reuse bool) (graphpublication.Prepared, error) {
		current, err := m.ReadRoot(ctx, "owner")
		if err != nil {
			return graphpublication.Prepared{}, err
		}
		if reuse {
			return p.PrepareAppendWithOwned(ctx, "owner", current.Head, []byte("next"), nil, []graphpublication.OwnedPayload{source}, now().Add(time.Second))
		}
		return p.PrepareAppend(ctx, "owner", current.Head, []byte("next"), [][]byte{[]byte("private")}, now().Add(time.Second))
	}
	expire := func() error {
		if err := schedule.AdvanceMillis(2000); err != nil {
			return err
		}
		_, err := p.Sweep(ctx, now())
		return err
	}
	lost := func(err error) error {
		if !errors.Is(err, ErrTransportLost) {
			return fmt.Errorf("expected uncertain transport: %v", err)
		}
		return nil
	}
	conflict := func(err error) error {
		if !errors.Is(err, graphpublication.ErrConflict) {
			return fmt.Errorf("expected original-head fence: %v", err)
		}
		return nil
	}
	commit := func(prepared graphpublication.Prepared) error { _, err := p.Commit(ctx, prepared); return err }
	switch mode {
	case "append_inherit", "owned_reuse":
		prepared, err := prepare(mode == "owned_reuse")
		if err != nil {
			return trace, err
		}
		if err = commit(prepared); err != nil {
			return trace, err
		}
		if err = expire(); err != nil {
			return trace, err
		}
	case "paused_upload":
		m.PauseBefore("put", expire)
		if _, err = prepare(false); !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("late upload not revoked: %v", err)
		}
		if err = expire(); err != nil {
			return trace, err
		}
	case "lost_pin", "lost_ready", "lost_put", "read_root_lost", "read_blob_lost", "get_lost":
		switch mode {
		case "lost_pin":
			err = m.QueueFault("cas_blob", LoseAckAfterCommit)
		case "lost_ready":
			m.PauseBefore("put", func() error { return m.QueueFault("cas_blob", LoseAckAfterCommit) })
		case "lost_put":
			err = m.QueueFault("put", LoseAckAfterCommit)
		case "read_root_lost":
			err = m.QueueFault("read_root", DropBeforeCommit)
		case "read_blob_lost":
			err = m.QueueFault("read_blob", DropBeforeCommit)
		case "get_lost":
			err = m.QueueFault("get", DropBeforeCommit)
		}
		if err != nil {
			return trace, err
		}
		_, err = prepare(false)
		if err = lost(err); err != nil {
			return trace, err
		}
		if err = expire(); err != nil {
			return trace, err
		}
	case "lost_root":
		prepared, err := prepare(false)
		if err != nil {
			return trace, err
		}
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		if err = commit(prepared); err != nil {
			return trace, fmt.Errorf("exact publication readback failed: %v", err)
		}
	case "paused_commit", "collector_wins":
		prepared, err := prepare(mode == "collector_wins")
		if err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", expire)
		if err = conflict(commit(prepared)); err != nil {
			return trace, err
		}
	case "commit_wins":
		prepared, err := prepare(true)
		if err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", func() error { return commit(prepared) })
		if err = expire(); err != nil {
			return trace, err
		}
	case "lost_fence", "lost_close", "lost_delete":
		prepared, err := prepare(false)
		if err != nil {
			return trace, err
		}
		op := "cas_root"
		if mode == "lost_close" {
			op = "cas_blob"
		}
		if mode == "lost_delete" {
			op = "delete"
		}
		if err = m.QueueFault(op, LoseAckAfterCommit); err != nil {
			return trace, err
		}
		if err = lost(expire()); err != nil {
			return trace, err
		}
		if err = conflict(commit(prepared)); err != nil {
			return trace, err
		}
		if err = expire(); err != nil {
			return trace, err
		}
	case "foreign_reuse":
		_, err = p.PrepareAppendWithOwned(ctx, "foreign", 0, []byte("foreign"), nil, []graphpublication.OwnedPayload{source}, now().Add(time.Second))
		if !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("foreign graph adopted payload: %v", err)
		}
	default:
		return trace, fmt.Errorf("unknown graph publication mode")
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	current, err := m.ReadRoot(ctx, "owner")
	if err != nil {
		return trace, err
	}
	wantCount := uint64(1)
	if mode == "append_inherit" || mode == "owned_reuse" || mode == "lost_root" || mode == "commit_wins" {
		wantCount = 2
	}
	if current.Graph.Count != wantCount {
		return trace, fmt.Errorf("canonical population=%d expected=%d mode=%s", current.Graph.Count, wantCount, mode)
	}
	for index := uint64(0); index < wantCount; index++ {
		value, err := retainedgraph.Read(ctx, graphReadStore{m}, current.Graph, index)
		if err != nil {
			return trace, err
		}
		wantData := "base"
		if index == 1 {
			wantData = "next"
		}
		if string(value.Data) != wantData || len(value.Blobs) != 1 {
			return trace, fmt.Errorf("canonical record changed at %d", index)
		}
		if index == 1 && (mode == "owned_reuse" || mode == "commit_wins") && value.Blobs[0] != source.Link {
			return trace, fmt.Errorf("owned payload receipt changed")
		}
	}
	if err = p.Retire(ctx, "owner", current.Head); err != nil {
		return trace, err
	}
	if err = expire(); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil {
		return trace, err
	}
	if len(objects) != 0 {
		return trace, fmt.Errorf("retired graph left %d objects", len(objects))
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_publication_protocol", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededGraphPublicationReplay(t *testing.T) {
	fail := func(seed int64, trace Trace, cause error) {
		t.Helper()
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-publication-failure-")
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
		generated, err := runGraphPublication(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphPublication(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("graph replay differs: %v", err))
		}
		if root := os.Getenv("SIM_GRAPH_PUBLICATION_ROOT"); root != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(root, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphPublicationModes) {
		t.Fatalf("graph publication coverage=%v", observed)
	}
	t.Logf("graph publication: modes=%v every trace exactly replayed; live receipt census and complete retirement checked", observed)
}

type graphReadStore struct{ *GraphPublicationTransport }

func (graphReadStore) Put(context.Context, string, []byte) (blobpublication.Reference, error) {
	return blobpublication.Reference{}, fmt.Errorf("read-only graph view")
}

func TestGraphPublicationTransportCopiesFaultsAndIndependentCensus(t *testing.T) {
	m := NewGraphPublicationTransport(NewScheduler(1))
	p := m.Protocol()
	ctx := context.Background()
	prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("data"), [][]byte{[]byte("payload")}, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	got, err := m.ReadRoot(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	got.Graph.Frontier[0].Link.Reference.Object += "changed"
	if reflect.DeepEqual(got, m.roots["owner"]) {
		t.Fatal("root frontier aliases authority")
	}
	keys, err := m.BlobKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		record, err := m.ReadBlob(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		for token, intent := range record.Fence.Intents {
			intent.Locations[0].First++
			record.Fence.Intents[token] = intent
		}
		if reflect.DeepEqual(record, m.blobs[key]) {
			t.Fatal("grant locations alias authority")
		}
	}
	if err = m.CheckReferences(); err != nil {
		t.Fatal(err)
	}
	leaf, err := retainedgraph.Read(ctx, graphReadStore{m}, root.Graph, 0)
	if err != nil {
		t.Fatal(err)
	}
	for key, record := range m.blobs {
		if record.Fence.Hash == leaf.Blobs[0].Hash {
			saved := graphFenceCopy(record.Fence)
			for token, intent := range record.Fence.Intents {
				intent.Locations[0].First = 99
				record.Fence.Intents[token] = intent
			}
			m.blobs[key] = record
			if err = m.CheckReferences(); err == nil {
				t.Fatal("independent census missed absent payload origin")
			}
			record.Fence = saved
			m.blobs[key] = record
		}
	}
	if err = m.QueueFault("get", LoseAckAfterCommit); err == nil {
		t.Fatal("unsupported read fault accepted")
	}
	if err = m.QueueFault("unknown", DropBeforeCommit); err == nil {
		t.Fatal("unknown operation accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = m.Get(canceled, root.Graph.Frontier[0].Link, retainedgraph.MaxNodeBytes); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled read ignored", err)
	}
}
