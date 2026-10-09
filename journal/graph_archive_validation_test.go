package journal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
)

var archiveProbeReached = errors.New("valid descriptor reached ownership operation")

// This probe returns independent root copies but has no payload authority. It
// distinguishes rejection during metadata validation from rejection later in
// reader acquisition or payload ownership checks.
type archiveDescriptorProbe struct {
	graphpublication.Port
	root             graphpublication.Root
	cas, gets, blobs int
}

func (p *archiveDescriptorProbe) ReadRoot(context.Context, string) (graphpublication.Root, error) {
	data, err := json.Marshal(p.root)
	if err != nil {
		return graphpublication.Root{}, err
	}
	var copy graphpublication.Root
	err = json.Unmarshal(data, &copy)
	return copy, err
}
func (p *archiveDescriptorProbe) CASRoot(context.Context, string, uint64, graphpublication.Root) (graphpublication.Root, error) {
	p.cas++
	return graphpublication.Root{}, archiveProbeReached
}
func (p *archiveDescriptorProbe) ReadBlob(context.Context, string) (graphpublication.Record, error) {
	p.blobs++
	return graphpublication.Record{}, archiveProbeReached
}
func (p *archiveDescriptorProbe) Get(context.Context, retainedgraph.Link, int) ([]byte, error) {
	p.gets++
	return nil, archiveProbeReached
}

func TestGraphArchiveDescriptorRejectsMalformedAuthority(t *testing.T) {
	ctx := context.Background()
	model := sim.NewGraphPublicationTransport(sim.NewScheduler(3))
	now := time.Unix(1000, 0).UTC()
	config := journal.GraphConfig{Protocol: model.Protocol(), Now: func() time.Time { return now }, PinTTL: 3 * time.Hour, IntentTTL: time.Second, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true}
	var captured graphpublication.Root
	checkpointArchiveScenario(t, ctx, 3, config, model, &now, func() *journal.GraphStore {
		keys, err := model.RootKeys(ctx)
		if err != nil || len(keys) != 1 {
			t.Fatal(keys, err)
		}
		captured, err = model.ReadRoot(ctx, keys[0])
		if err != nil {
			t.Fatal(err)
		}
		store, err := journal.NewGraphStore(config)
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
	var cursor struct {
		Invocation   uint64 `json:"invocation"`
		Count        uint64 `json:"count"`
		RetainedFrom uint64 `json:"retained_from"`
		Base         uint64 `json:"base"`
		Epoch        uint64 `json:"epoch"`
	}
	if err := json.Unmarshal(captured.Application, &cursor); err != nil || cursor.RetainedFrom == 0 {
		t.Fatal(cursor, err)
	}
	newProbe := func() (*archiveDescriptorProbe, *journal.GraphStore) {
		probe := &archiveDescriptorProbe{root: captured}
		root, err := probe.ReadRoot(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		probe.root = root
		cfg := config
		cfg.Now = func() time.Time { return time.Unix(1000, 0).UTC() }
		cfg.Protocol = graphpublication.Protocol{Port: probe, NewID: func() (string, error) { return "probe-reader", nil }}
		store, err := journal.NewGraphStore(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return probe, store
	}
	// A valid descriptor permits metadata observation and reaches the reader CAS.
	probe, store := newProbe()
	status, err := store.InspectStart(ctx, "flow", "archive")
	if err != nil || status.JournalCount != cursor.Count {
		t.Fatal("valid metadata rejected", status, err)
	}
	if _, err = store.Open(ctx, "flow", "archive", cursor.Invocation); !errors.Is(err, archiveProbeReached) || probe.cas != 1 || probe.gets != 0 {
		t.Fatal("positive reader control", probe, err)
	}
	setField := func(root *graphpublication.Root, name string, value uint64) {
		pattern := regexp.MustCompile(`"` + name + `":\d+`)
		if !pattern.Match(root.Application) {
			t.Fatal("field absent", name)
		}
		root.Application = pattern.ReplaceAll(root.Application, []byte(fmt.Sprintf(`"%s":%d`, name, value)))
	}
	cases := map[string]func(*graphpublication.Root){
		"missing-token":         func(r *graphpublication.Root) { r.Token = "" },
		"invalid-token":         func(r *graphpublication.Root) { r.Token = "invalid token" },
		"offset-outside-count":  func(r *graphpublication.Root) { setField(r, "retained_from", cursor.Count+1) },
		"offset-after-request":  func(r *graphpublication.Root) { setField(r, "retained_from", cursor.RetainedFrom+1) },
		"offset-before-archive": func(r *graphpublication.Root) { setField(r, "retained_from", cursor.RetainedFrom-1) },
		"missing-pointer": func(r *graphpublication.Root) {
			at := bytes.Index(r.Application, []byte(`,"checkpoint":`))
			if at < 0 {
				t.Fatal("pointer absent")
			}
			r.Application = append(bytes.Clone(r.Application[:at]), '}')
		},
		"wrong-schema": func(r *graphpublication.Root) {
			r.Application = bytes.Replace(r.Application, []byte("cursor-v6"), []byte("cursor-v5"), 1)
		},
		"missing-archive":  func(r *graphpublication.Root) { r.Streams = r.Streams[1:] },
		"archive-count":    func(r *graphpublication.Root) { r.Streams[0].Graph = r.Graph },
		"live-count":       func(r *graphpublication.Root) { r.Graph = r.Streams[0].Graph },
		"archive-frontier": func(r *graphpublication.Root) { r.Streams[0].Graph.Frontier = nil },
		"archive-hash":     func(r *graphpublication.Root) { r.Streams[0].Graph.Frontier[0].Link.Hash = "malformed" },
		"duplicate-stream": func(r *graphpublication.Root) { r.Streams = append(r.Streams, r.Streams[0]) },
		"unknown-stream":   func(r *graphpublication.Root) { r.Streams[0].Name = "unrecognized" },
		"duplicate-reader": func(r *graphpublication.Root) { r.Readers = append(r.Readers, r.Readers[0]) },
		"reader-expiry":    func(r *graphpublication.Root) { r.Readers[0].Expires = time.Time{} },
		"reader-snapshot":  func(r *graphpublication.Root) { r.Readers[0].Graph.Frontier = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			for _, op := range []string{"inspect", "begin", "open", "append", "compact"} {
				t.Run(op, func(t *testing.T) {
					probe, store := newProbe()
					mutate(&probe.root)
					var err error
					switch op {
					case "inspect":
						_, err = store.InspectStart(ctx, "flow", "archive")
					case "begin":
						_, err = store.Begin(ctx, "flow", "archive", cursor.Invocation)
					case "open":
						_, err = store.Open(ctx, "flow", "archive", cursor.Invocation)
					case "append":
						_, err = store.Append(ctx, "flow", "archive", cursor.Invocation, journal.Entry{Kind: journal.StepRequested, Index: cursor.Count, Epoch: cursor.Epoch, Payload: []byte(`{"kind":"run","name":"after"}`)}, cursor.Base+cursor.Count, nil, nil)
					case "compact":
						err = store.CompactCheckpoint(ctx, "flow", "archive", *status.Checkpoint, cursor.Base+cursor.Count)
					}
					if !errors.Is(err, journal.ErrGap) || probe.cas != 0 || probe.gets != 0 || probe.blobs != 0 {
						t.Fatal("malformed authority admitted", op, err, probe.cas, probe.gets, probe.blobs)
					}
				})
			}
		})
	}
}
