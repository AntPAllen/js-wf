package graphpublication

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

func appendStream(t *testing.T, p Protocol, root Root, stream, value string, owned []OwnedPayload) Root {
	t.Helper()
	prepared, err := p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, stream, []byte(value), [][]byte{[]byte("payload:" + value)}, owned, epoch.Add(time.Hour), []byte("active"))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestGraphStreamsIndependentIndexesPinnedRetirementAndResume(t *testing.T) {
	m, p := newModel("streams")
	root := EmptyRoot()
	for i := 0; i < 17; i++ {
		for _, stream := range []string{"input", "signals"} {
			root = appendStream(t, p, root, stream, fmt.Sprintf("%s:%d", stream, i), nil)
		}
		prepared, err := p.PrepareAppend(ctx, "history", root.Head, []byte(fmt.Sprintf("journal:%d", i)), nil, epoch.Add(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		root, err = p.Commit(ctx, prepared)
		if err != nil {
			t.Fatal(err)
		}
	}
	if root.Schema != StreamsSchema || root.Graph.Count != 17 || len(root.Streams) != 2 || root.Streams[0].Graph.Count != 17 || root.Streams[1].Graph.Count != 17 {
		t.Fatal(root)
	}
	reader, root, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := reader.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	var checkpointValue readerCheckpoint
	if json.Unmarshal(checkpoint, &checkpointValue) != nil || checkpointValue.Schema != StreamReaderCheckpointSchema {
		t.Fatal(string(checkpoint))
	}
	for _, stream := range []string{"input", "signals"} {
		r, err := p.ReadRetainedStream(ctx, reader, stream, 0, epoch)
		if err != nil {
			t.Fatal(err)
		}
		before := len(m.objects)
		prepared, err := p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, stream, []byte("reused"), nil, []OwnedPayload{{Stream: stream, Index: 0, Link: r.Blobs[0]}}, epoch.Add(time.Hour), root.Application)
		if err != nil {
			t.Fatal(err)
		}
		root, err = p.Commit(ctx, prepared)
		if err != nil {
			t.Fatal(err)
		}
		if len(m.objects)-before != 2 {
			t.Fatal("payload reuse uploaded bytes", len(m.objects)-before)
		}
	}
	root, err = p.RetireLiveWithApplication(ctx, "history", root.Head, []byte("retired"))
	if err != nil || root.Graph.Count != 0 || root.Streams[0].Graph.Count != 0 || root.Streams[1].Graph.Count != 0 || root.Schema != StreamsSchema {
		t.Fatal(root, err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	resumed, root, err := p.ResumeReader(ctx, checkpoint, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 17; i++ {
		r, err := p.ReadRetained(ctx, resumed, i, epoch.Add(time.Hour))
		if err != nil || string(r.Data) != fmt.Sprintf("journal:%d", i) {
			t.Fatal(r, err)
		}
		for _, stream := range []string{"input", "signals"} {
			r, err = p.ReadRetainedStream(ctx, resumed, stream, i, epoch.Add(time.Hour))
			if err != nil || string(r.Data) != fmt.Sprintf("%s:%d", stream, i) {
				t.Fatal(r, err)
			}
			data, err := m.Get(ctx, r.Blobs[0], 100)
			if err != nil || string(data) != "payload:"+string(r.Data) {
				t.Fatal(string(data), err)
			}
		}
	}
	// Returned roots and stream snapshots cannot mutate the canonical pin.
	root.Readers[0].Streams[0].Graph.Frontier[0].Link.Hash = "mutated"
	if _, err = p.ReadRetainedStream(ctx, resumed, "input", 0, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	root, err = p.ReleaseReader(ctx, resumed, root.Head)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(3*time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(len(m.objects), err)
	}
	if _, _, err = p.ResumeReader(ctx, checkpoint, epoch.Add(time.Hour)); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	for _, schema := range []string{Schema, RetentionSchema, ApplicationSchema} {
		next := root
		next.Schema = schema
		next.Streams = nil
		if _, err = m.CASRoot(ctx, "history", root.Head, next); err == nil {
			t.Fatal("schema downgrade", schema)
		}
	}
}

func TestGraphStreamsSingleHeadFencesAllPublishers(t *testing.T) {
	for _, winner := range []string{"retire", "other-stream", "application", "collector"} {
		t.Run(winner, func(t *testing.T) {
			m, p := newModel("fence")
			root := appendStream(t, p, EmptyRoot(), "signals", "base", nil)
			pending, err := p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, "input", []byte("pending"), nil, nil, epoch.Add(time.Second), []byte("active"))
			if err != nil {
				t.Fatal(err)
			}
			switch winner {
			case "retire":
				root, err = p.RetireLiveWithApplication(ctx, "history", root.Head, []byte("retired"))
			case "other-stream":
				root = appendStream(t, p, root, "signals", "winner", nil)
			case "application":
				root, err = p.UpdateApplication(ctx, "history", root.Head, []byte("purging"))
			case "collector":
				_, err = p.SweepWithReaders(ctx, epoch.Add(2*time.Second))
				root = m.roots["history"]
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.Commit(ctx, pending); !errors.Is(err, ErrConflict) {
				t.Fatal("stale publisher accepted", err)
			}
			if g, _ := root.StreamSnapshot("input"); g.Count != 0 {
				t.Fatal("uncommitted input published", g)
			}
			root, err = p.RetireLiveWithApplication(ctx, "history", root.Head, []byte("retired"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(ctx, epoch.Add(2*time.Hour)); err != nil || len(m.objects) != 0 {
				t.Fatal(len(m.objects), err)
			}
		})
	}
}

func TestGraphStreamsExactUnknownReadback(t *testing.T) {
	for _, altered := range []bool{false, true} {
		m, p := newModel("lost")
		root := appendStream(t, p, EmptyRoot(), "input", "one", nil)
		pending, err := p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, "signals", []byte("signal"), nil, nil, epoch.Add(time.Hour), []byte("active"))
		if err != nil {
			t.Fatal(err)
		}
		m.rootAfter = func() error {
			if altered {
				actual := m.roots["history"]
				actual.Streams[0].Graph = retainedgraph.Empty()
				m.roots["history"] = actual
			}
			return lostReply
		}
		_, err = p.Commit(ctx, pending)
		if !altered && err != nil || altered && !errors.Is(err, lostReply) {
			t.Fatal(altered, err)
		}
	}
}

func TestGraphStreamsRejectCoordinateAndInheritanceForgery(t *testing.T) {
	for _, attack := range []string{"other-forest", "stream-coordinate", "cross-stream-reuse", "false-source-stream", "reader-checkpoint"} {
		t.Run(attack, func(t *testing.T) {
			m, p := newModel("hostile")
			root := appendStream(t, p, EmptyRoot(), "input", "original", nil)
			root = appendStream(t, p, root, "signals", "signal", nil)
			pending, err := p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, "signals", []byte("next"), nil, nil, epoch.Add(time.Hour), root.Application)
			if err != nil {
				t.Fatal(err)
			}
			switch attack {
			case "other-forest":
				pending.publication.Streams[0].Graph = retainedgraph.Empty()
				if _, err = p.Commit(ctx, pending); err == nil {
					t.Fatal("changed inherited forest accepted")
				}
			case "stream-coordinate":
				for scope, record := range m.blobs {
					if record.Fence.Owner == pending.publication.Token {
						intent := record.Fence.Intents[record.Fence.Owner]
						for i := range intent.Locations {
							intent.Locations[i].Stream = "input"
						}
						record.Fence.Intents[record.Fence.Owner] = intent
						m.blobs[scope] = record
					}
				}
				if _, err = p.Commit(ctx, pending); !errors.Is(err, ErrRevoked) {
					t.Fatal(err)
				}
			case "cross-stream-reuse", "false-source-stream":
				r, err := retainedgraph.Read(ctx, stageStore{protocol: p}, root.Streams[0].Graph, 0)
				if err != nil {
					t.Fatal(err)
				}
				source := "input"
				if attack == "false-source-stream" {
					source = "signals"
				}
				if _, err = p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, "signals", nil, nil, []OwnedPayload{{Stream: source, Index: 0, Link: r.Blobs[0]}}, epoch.Add(time.Hour), nil); err == nil {
					t.Fatal("cross-stream edge accepted")
				}
			case "reader-checkpoint":
				reader, _, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(time.Hour))
				if err != nil {
					t.Fatal(err)
				}
				data, err := reader.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				var v readerCheckpoint
				if json.Unmarshal(data, &v) != nil {
					t.Fatal("decode")
				}
				v.StreamsSHA256 = key([]byte("forged"))
				data, _ = json.Marshal(v)
				if _, _, err = p.ResumeReader(ctx, data, epoch); !errors.Is(err, ErrRevoked) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestGraphStreamsBoundsCanonicalEncodingAndLegacyBytes(t *testing.T) {
	m, p := newModel("bounds")
	for _, name := range []string{"", "Upper", "has space", string(bytes.Repeat([]byte{'a'}, 33))} {
		if _, err := p.PrepareStreamAppendWithApplication(ctx, "history", 0, name, nil, nil, nil, epoch.Add(time.Hour), nil); err == nil {
			t.Fatal(name)
		}
	}
	root := EmptyRoot()
	for _, name := range []string{"a", "b", "c", "d"} {
		root = appendStream(t, p, root, name, name, nil)
	}
	if _, err := p.PrepareStreamAppendWithApplication(ctx, "history", root.Head, "e", nil, nil, nil, epoch.Add(time.Hour), nil); err == nil {
		t.Fatal("stream bound")
	}
	for _, attack := range []string{"order", "duplicate", "unknown-schema", "missing-token"} {
		next := cloneRoot(root)
		switch attack {
		case "order":
			next.Streams[0], next.Streams[1] = next.Streams[1], next.Streams[0]
		case "duplicate":
			next.Streams[1].Name = next.Streams[0].Name
		case "unknown-schema":
			next.Schema = ApplicationSchema
		case "missing-token":
			next.Token = ""
		}
		if _, err := normalizeRoot(next); err == nil {
			t.Fatal(attack)
		}
	}
	legacy := EmptyRoot()
	encoded, _ := json.Marshal(legacy)
	if string(encoded) != `{"Schema":"js-wf-graph-publication-v1","Head":0,"Token":"","Graph":{"schema":"js-wf-retained-append-graph-v1","count":0,"frontier":[]}}` {
		t.Fatal(string(encoded))
	}
	location, _ := json.Marshal(Location{Kind: "node", First: 0, Height: 0})
	if string(location) != `{"Kind":"node","First":0,"Height":0}` {
		t.Fatal(string(location))
	}
	copy, _ := m.ReadRoot(ctx, "history")
	copy.Streams[0].Graph.Frontier[0].Link.Hash = "mutated"
	actual, _ := m.ReadRoot(ctx, "history")
	if reflect.DeepEqual(copy.Streams, actual.Streams) {
		t.Fatal("aliased named graph")
	}
}

func TestGraphStreamsOldReaderAcrossUpgradeAndUnknownCollection(t *testing.T) {
	m, p := newModel("upgrade")
	pending, err := p.PrepareAppend(ctx, "history", 0, []byte("old"), nil, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.Commit(ctx, pending)
	if err != nil {
		t.Fatal(err)
	}
	reader, root, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := reader.Checkpoint()
	if err != nil || bytes.Contains(checkpoint, []byte("StreamsSHA256")) {
		t.Fatal(string(checkpoint), err)
	}
	root = appendStream(t, p, root, "signals", "new", nil)
	resumed, _, err := p.ResumeReader(ctx, checkpoint, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if r, e := p.ReadRetained(ctx, resumed, 0, epoch); e != nil || string(r.Data) != "old" {
		t.Fatal(r, e)
	}
	if _, e := p.ReadRetainedStream(ctx, resumed, "signals", 0, epoch); !errors.Is(e, retainedgraph.ErrIndex) {
		t.Fatal("old reader adopted later forest", e)
	}
	before := len(m.objects)
	m.readRootHook = func(string) error { return lostReply }
	if _, err = p.SweepWithReaders(ctx, epoch.Add(2*time.Hour)); !errors.Is(err, lostReply) || len(m.objects) != before || m.deletes != 0 {
		t.Fatal("unknown root permitted deletion", err)
	}
	m.readRootHook = nil
	root, err = p.RetireLiveWithApplication(ctx, "history", root.Head, []byte("retired"))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.ReleaseReader(ctx, resumed, root.Head)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(4*time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(len(m.objects), err)
	}
}
