package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
)

var graphStreamsModes = []string{"ordinary", "lost_append", "dropped_append", "unknown_readback", "other_stream_wins", "purge_fence_wins", "retirement_wins", "collector_wins", "lost_retirement", "dropped_retirement", "renewed_reader", "expired_reader", "forged_checkpoint", "wrong_grant_stream", "copied_roots", "schema_downgrade"}

func runGraphStreams(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_independent_streams"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphStreamsModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(s)
	p := m.Protocol()
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	start := now()
	root := graphpublication.EmptyRoot()
	// Identical bytes in two forests have separate physical origin grants.
	payload := []byte("shared input and signal body")
	for _, name := range []string{"input", "signals"} {
		prepared, e := p.PrepareStreamAppendWithApplication(ctx, "workflow", root.Head, name, []byte(name), [][]byte{payload}, nil, start.Add(time.Second), []byte("active"))
		if e != nil {
			return trace, e
		}
		root, e = p.Commit(ctx, prepared)
		if e != nil {
			return trace, e
		}
	}
	prepared, err := p.PrepareAppendWithApplication(ctx, "workflow", root.Head, []byte("started"), nil, nil, start.Add(time.Second), []byte("active"))
	if err != nil {
		return trace, err
	}
	root, err = p.Commit(ctx, prepared)
	if err != nil {
		return trace, err
	}
	if root.Schema != graphpublication.StreamsSchema || root.Graph.Count != 1 || len(root.Streams) != 2 || root.Streams[0].Graph.Count != 1 || root.Streams[1].Graph.Count != 1 {
		return trace, fmt.Errorf("indexes not independent")
	}
	reader, root, err := p.AcquireReader(ctx, "workflow", root.Head, start.Add(3*time.Second))
	if err != nil {
		return trace, err
	}
	checkpoint, err := reader.Checkpoint()
	if err != nil {
		return trace, err
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	pending, err := p.PrepareStreamAppendWithApplication(ctx, "workflow", root.Head, "signals", []byte("second"), [][]byte{[]byte("second-body")}, nil, start.Add(time.Second), []byte("active"))
	if err != nil {
		return trace, err
	}
	conflict := false
	switch mode {
	case "lost_append", "unknown_readback":
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		if mode == "unknown_readback" {
			m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
		}
	case "dropped_append":
		if err = m.QueueFault("cas_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	case "other_stream_wins":
		other, e := p.PrepareStreamAppendWithApplication(ctx, "workflow", root.Head, "input", []byte("input-second"), nil, nil, start.Add(time.Second), []byte("active"))
		if e != nil {
			return trace, e
		}
		root, err = p.Commit(ctx, other)
		conflict = true
	case "purge_fence_wins":
		root, err = p.UpdateApplication(ctx, "workflow", root.Head, []byte("purging"))
		conflict = true
	case "retirement_wins":
		root, err = p.RetireLiveWithApplication(ctx, "workflow", root.Head, []byte("retired"))
		conflict = true
	case "collector_wins":
		if e := s.AdvanceMillis(2000); e != nil {
			return trace, e
		}
		_, err = p.SweepWithReaders(ctx, now())
		conflict = true
	case "wrong_grant_stream":
		// Corrupt only pending grants. Ready canonical graphs remain intact.
		m.mu.Lock()
		for scope, record := range m.blobs {
			for token, intent := range record.Fence.Intents {
				if intent.Expected == root.Head {
					for i := range intent.Locations {
						intent.Locations[i].Stream = "input"
					}
					record.Fence.Intents[token] = intent
					m.blobs[scope] = record
				}
			}
		}
		m.mu.Unlock()
	}
	if err != nil {
		return trace, err
	}
	_, err = p.Commit(ctx, pending)
	switch {
	case conflict:
		if !errors.Is(err, graphpublication.ErrConflict) {
			return trace, fmt.Errorf("stale named publisher accepted: %v", err)
		}
	case mode == "wrong_grant_stream":
		if !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("foreign coordinates accepted: %v", err)
		}
	case mode == "unknown_readback" || mode == "dropped_append":
		if err == nil {
			return trace, fmt.Errorf("uncertain append accepted")
		}
		if _, err = p.Commit(ctx, pending); err != nil {
			return trace, err
		}
	default:
		if err != nil {
			return trace, err
		}
	}
	root, err = m.ReadRoot(ctx, "workflow")
	if err != nil {
		return trace, err
	}
	signals, e := root.StreamSnapshot("signals")
	if e != nil {
		return trace, e
	}
	wantSignals := uint64(2)
	if conflict || mode == "wrong_grant_stream" {
		wantSignals = 1
	}
	if mode == "retirement_wins" {
		wantSignals = 0
	}
	if signals.Count != wantSignals {
		return trace, fmt.Errorf("signal count=%d expected=%d", signals.Count, wantSignals)
	}
	if mode != "retirement_wins" && root.Graph.Count != 1 {
		return trace, fmt.Errorf("signal append changed journal index")
	}
	if mode == "schema_downgrade" {
		old := root
		old.Schema = graphpublication.ApplicationSchema
		old.Streams = nil
		if _, e = m.CASRoot(ctx, "workflow", root.Head, old); e == nil {
			return trace, fmt.Errorf("schema downgrade accepted")
		}
	}
	if mode == "copied_roots" {
		copy, e := m.ReadRoot(ctx, "workflow")
		if e != nil {
			return trace, e
		}
		copy.Streams[0].Graph.Frontier[0].Link.Hash = "mutated-copy"
		copy.Readers[0].Streams[0].Graph.Frontier[0].Link.Hash = "mutated-pin-copy"
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if mode == "lost_retirement" {
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	if mode == "dropped_retirement" {
		if err = m.QueueFault("cas_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	root, err = p.RetireLiveWithApplication(ctx, "workflow", root.Head, []byte("retired"))
	if mode == "dropped_retirement" {
		if err == nil {
			return trace, fmt.Errorf("dropped retirement accepted")
		}
		actual, e := m.ReadRoot(ctx, "workflow")
		if e != nil || actual.Graph.Count != 1 {
			return trace, fmt.Errorf("drop cleared journal: %v", e)
		}
		root, err = p.RetireLiveWithApplication(ctx, "workflow", actual.Head, []byte("retired"))
	}
	if err != nil {
		return trace, err
	}
	if mode == "renewed_reader" {
		root, err = p.RenewReader(ctx, reader, root.Head, start.Add(5*time.Second))
		if err != nil {
			return trace, err
		}
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	if mode == "forged_checkpoint" {
		var value map[string]any
		if err = json.Unmarshal(checkpoint, &value); err != nil {
			return trace, err
		}
		value["StreamsSHA256"] = digest([]byte("forged"))
		forged, _ := json.Marshal(value)
		if _, _, err = p.ResumeReader(ctx, forged, now()); err == nil {
			return trace, fmt.Errorf("forged graph-set checkpoint accepted")
		}
	}
	resumed, root, err := p.ResumeReader(ctx, checkpoint, now())
	if err != nil {
		return trace, err
	}
	previousObject := ""
	for _, stream := range []string{"input", "signals"} {
		r, e := p.ReadRetainedStream(ctx, resumed, stream, 0, now())
		if e != nil || string(r.Data) != stream || len(r.Blobs) != 1 {
			return trace, fmt.Errorf("old reader lost %s: %v", stream, e)
		}
		body, e := m.Get(ctx, r.Blobs[0], len(payload))
		if e != nil || !bytes.Equal(body, payload) || r.Blobs[0].Reference.Object == previousObject {
			return trace, fmt.Errorf("missing/exchanged stream origin: %v", e)
		}
		previousObject = r.Blobs[0].Reference.Object
	}
	if r, e := p.ReadRetained(ctx, resumed, 0, now()); e != nil || string(r.Data) != "started" {
		return trace, fmt.Errorf("old journal snapshot lost: %v", e)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	if mode == "expired_reader" {
		if e := s.AdvanceMillis(4000); e != nil {
			return trace, e
		}
		if _, err = p.SweepWithReaders(ctx, now()); err != nil {
			return trace, err
		}
		if _, err = p.ReadRetainedStream(ctx, resumed, "signals", 0, now()); !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("expired reader usable: %v", err)
		}
	} else {
		root, err = p.ReleaseReader(ctx, resumed, root.Head)
		if err != nil {
			return trace, err
		}
	}
	if e := s.AdvanceMillis(6000); e != nil {
		return trace, e
	}
	if _, err = p.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	root, err = m.ReadRoot(ctx, "workflow")
	if err != nil || root.Schema != graphpublication.StreamsSchema || root.Graph.Count != 0 || len(root.Readers) != 0 || string(root.Application) != "retired" {
		return trace, fmt.Errorf("final lifecycle: %v", err)
	}
	for _, stream := range root.Streams {
		if stream.Graph.Count != 0 {
			return trace, fmt.Errorf("retained live named graph")
		}
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("object drain: %d %v", len(objects), err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_independent_streams", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphStreamsReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-streams-failure-")
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
		generated, err := runGraphStreams(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphStreams(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("streams replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_STREAMS_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphStreamsModes) {
		t.Fatalf("streams coverage=%v", observed)
	}
	t.Logf("graph streams: modes=%v; independent indexes, shared head, qualified ownership, complete reader snapshot, exact replay and all physical objects drained", observed)
}
