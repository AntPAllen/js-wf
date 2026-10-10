package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

func TestGraphPrefixCompactionPreparationFailures(t *testing.T) {
	for _, fault := range []string{"bound", "digest", "missing", "upload-drop", "upload-lost-ack", "cancel", "zero-cut", "outside-cut"} {
		t.Run(fault, func(t *testing.T) {
			m, p := newModel("copy-" + fault)
			root := appendOne(t, m, p, "owner", []byte("record"), [][]byte{[]byte("payload")})
			original := cloneRoot(root)
			record, err := retainedgraph.Read(ctx, stageStore{protocol: p}, root.Graph, 0)
			if err != nil {
				t.Fatal(err)
			}
			limit, cut, runCtx := 1024, uint64(1), ctx
			switch fault {
			case "bound":
				limit = 1
			case "digest":
				m.objects[record.Blobs[0].Reference.Object] = []byte("corrupt")
			case "missing":
				delete(m.objects, record.Blobs[0].Reference.Object)
			case "upload-drop":
				m.putHook = func(string, []byte) error { return lostReply }
			case "upload-lost-ack":
				m.putHook = func(name string, data []byte) error { m.objects[name] = bytes.Clone(data); return lostReply }
			case "cancel":
				var cancel context.CancelFunc
				runCtx, cancel = context.WithCancel(ctx)
				cancel()
			case "zero-cut":
				cut = 0
			case "outside-cut":
				cut = 2
			}
			if _, err = p.PreparePrefixCompaction(runCtx, "owner", root.Head, cut, limit, epoch.Add(time.Second), nil); err == nil {
				t.Fatal("invalid copy accepted")
			}
			if !reflect.DeepEqual(original, m.roots["owner"]) {
				t.Fatal("failed preparation published")
			}
			if fault == "upload-drop" || fault == "upload-lost-ack" {
				if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				for name := range m.objects {
					if name != record.Blobs[0].Reference.Object && name != root.Graph.Frontier[0].Link.Reference.Object {
						t.Fatal("orphan upload survived", name)
					}
				}
			}
		})
	}
}

func TestGraphPrefixCompactionPreservesStreamsAndOwnedReuse(t *testing.T) {
	m, p := newModel("compact-streams")
	root := appendOne(t, m, p, "owner", []byte("first"), [][]byte{[]byte("shared")})
	source, err := retainedgraph.Read(ctx, stageStore{protocol: p}, root.Graph, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		plan, err := p.PrepareAppendWithOwned(ctx, "owner", root.Head, []byte(fmt.Sprint(i)), nil, []OwnedPayload{{Index: 0, Link: source.Blobs[0]}}, epoch.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		root, err = p.Commit(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"start", "input", "queue"} {
		plan, err := p.PrepareStreamAppendWithApplication(ctx, "owner", root.Head, name, []byte(name), [][]byte{[]byte(name + " payload")}, nil, epoch.Add(time.Second), nil)
		if err != nil {
			t.Fatal(err)
		}
		root, err = p.Commit(ctx, plan)
		if err != nil {
			t.Fatal(err)
		}
	}
	before := cloneRoot(root)
	plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 6, 1024, epoch.Add(time.Second), []byte("cursor"))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.CommitPrefixCompaction(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	root = m.roots["owner"]
	for _, name := range []string{"start", "input", "queue"} {
		graph := selectGraph(root.Graph, root.Streams, name)
		if !reflect.DeepEqual(graph, selectGraph(before.Graph, before.Streams, name)) {
			t.Fatal("other stream changed", name)
		}
		r, err := retainedgraph.Read(ctx, stageStore{protocol: p}, graph, 0)
		if err != nil || string(r.Data) != name {
			t.Fatal(name, err)
		}
		data, err := m.Get(ctx, r.Blobs[0], 1024)
		if err != nil || string(data) != name+" payload" {
			t.Fatal(name, err)
		}
	}
	for _, name := range []string{"", PrefixArchiveStream} {
		graph := selectGraph(root.Graph, root.Streams, name)
		for i := uint64(0); i < graph.Count; i++ {
			r, err := retainedgraph.Read(ctx, stageStore{protocol: p}, graph, i)
			if err != nil || len(r.Blobs) != 1 || r.Blobs[0].Reference == source.Blobs[0].Reference {
				t.Fatal("origin grant reused", name, i, err)
			}
			data, err := m.Get(ctx, r.Blobs[0], 1024)
			if err != nil || string(data) != "shared" {
				t.Fatal(name, i, err)
			}
		}
	}
	if _, exists := m.objects[source.Blobs[0].Reference.Object]; exists {
		t.Fatal("old shared receipt survived")
	}
}

// Check the relocated records against independently retained input bytes, and
// check every payload's physical bytes rather than just its graph membership.
func checkRelocated(t *testing.T, m *memoryPort, p Protocol, root Root, archived int, records, payloads [][]byte) {
	t.Helper()
	if root.Graph.Count != uint64(len(records)-archived) || selectGraph(root.Graph, root.Streams, PrefixArchiveStream).Count != uint64(archived) {
		t.Fatal("population changed", root)
	}
	for i := range records {
		stream, index := "", uint64(i-archived)
		if i < archived {
			stream, index = PrefixArchiveStream, uint64(i)
		}
		r, err := retainedgraph.Read(ctx, stageStore{protocol: p}, selectGraph(root.Graph, root.Streams, stream), index)
		if err != nil || !bytes.Equal(r.Data, records[i]) || len(r.Blobs) != 1 {
			t.Fatalf("record %d: %v %v", i, r, err)
		}
		data, err := m.Get(ctx, r.Blobs[0], 1024)
		if err != nil || !bytes.Equal(data, payloads[i]) {
			t.Fatalf("payload %d: %q %v", i, data, err)
		}
	}
}

func TestGraphPrefixCompactionSeededPinsAndCollection(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			m, p := newModel(fmt.Sprintf("compact%d", seed))
			rng := rand.New(rand.NewSource(seed))
			n := 3 + rng.Intn(14)
			var records, payloads [][]byte
			var root Root
			for i := 0; i < n; i++ {
				records = append(records, []byte(fmt.Sprintf("%d/%d", seed, i)))
				payloads = append(payloads, []byte(fmt.Sprintf("shared%d", rng.Intn(3))))
				root = appendOne(t, m, p, "owner", records[i], [][]byte{payloads[i]})
			}
			reader, root, err := p.AcquireReader(ctx, "owner", root.Head, epoch.Add(3*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			oldObjects := map[string]bool{}
			for object := range m.objects {
				oldObjects[object] = true
			}
			cut := 1 + rng.Intn(n-1)
			prepared, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, uint64(cut), 1024, epoch.Add(time.Second), []byte("logical cursor"))
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.CommitPrefixCompaction(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			for object := range oldObjects {
				if _, exists := m.objects[object]; !exists {
					t.Fatal("pinned original collected", object)
				}
			}
			for i := 0; i < n; i++ {
				r, err := p.ReadRetained(ctx, reader, uint64(i), epoch.Add(time.Hour))
				if err != nil || !bytes.Equal(r.Data, records[i]) {
					t.Fatal(i, err)
				}
			}
			checkRelocated(t, m, p, root, cut, records, payloads)
			root, err = p.ReleaseReader(ctx, reader, m.roots["owner"].Head)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			for object := range oldObjects {
				if _, exists := m.objects[object]; exists {
					t.Fatal("unpinned original was not collected", object)
				}
			}
			root = m.roots["owner"]
			checkRelocated(t, m, p, root, cut, records, payloads)
			// Append after relocation, then archive the entire remaining suffix.
			records = append(records, []byte("after compaction"))
			payloads = append(payloads, []byte("shared0"))
			root = appendOne(t, m, p, "owner", records[n], [][]byte{payloads[n]})
			prepared, err = p.PreparePrefixCompaction(ctx, "owner", root.Head, root.Graph.Count, 1024, epoch.Add(time.Second), nil)
			if err != nil {
				t.Fatal(err)
			}
			root, err = p.CommitPrefixCompaction(ctx, prepared)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			root = m.roots["owner"]
			checkRelocated(t, m, p, root, len(records), records, payloads)
			if err = p.RetireLive(ctx, "owner", root.Head); err != nil {
				t.Fatal(err)
			}
			if _, err = p.SweepWithReaders(ctx, epoch.Add(4*time.Hour)); err != nil || len(m.objects) != 0 {
				t.Fatal("retired archive leaked", len(m.objects), err)
			}
		})
	}
	t.Log("GRAPH_PREFIX_COMPACTION seeds=32 successive_compactions=64 original_receipts_collected=true pinned_originals_preserved=true complete_retirements=32")
}

func TestGraphPrefixCompactionPublicationFaults(t *testing.T) {
	for _, fault := range []string{"drop", "lost-ack", "unknown-readback", "append", "reader", "retire", "collector", "collector-at-cas", "revoked", "changed-readers"} {
		t.Run(fault, func(t *testing.T) {
			m, p := newModel("compact-" + fault)
			root := appendOne(t, m, p, "owner", []byte("first"), [][]byte{[]byte("payload")})
			root = appendOne(t, m, p, "owner", []byte("last"), [][]byte{[]byte("payload")})
			prepared, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 1, 1024, epoch.Add(time.Second), nil)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "drop":
				m.rootBefore = func(string, uint64, Root) error { return lostReply }
			case "lost-ack":
				m.rootAfter = func() error { return lostReply }
			case "unknown-readback":
				m.rootAfter = func() error { m.readRootHook = func(string) error { return lostReply }; return lostReply }
			case "append":
				appendOne(t, m, p, "owner", []byte("racer"), nil)
			case "reader":
				_, _, err = p.AcquireReader(ctx, "owner", root.Head, epoch.Add(time.Hour))
			case "retire":
				err = p.RetireLive(ctx, "owner", root.Head)
			case "collector":
				_, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour))
			case "collector-at-cas":
				m.rootBefore = func(string, uint64, Root) error { _, err := p.SweepWithReaders(ctx, epoch.Add(time.Hour)); return err }
			case "revoked":
				for key, record := range m.blobs {
					if record.Fence.Owner == prepared.publication.Token {
						record.Fence.Phase = "closed"
						record.Fence.Intents = map[string]Intent{}
						m.blobs[key] = record
					}
				}
			case "changed-readers":
				prepared.publication.Readers = []ReaderPin{{}}
			}
			if err != nil {
				t.Fatal(err)
			}
			before := cloneRoot(m.roots["owner"])
			result, err := p.CommitPrefixCompaction(ctx, prepared)
			if fault == "lost-ack" {
				if err != nil || result.Graph.Count != 1 {
					t.Fatal(result, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe publication accepted")
				}
				if fault != "unknown-readback" && fault != "collector-at-cas" && !reflect.DeepEqual(before, m.roots["owner"]) {
					t.Fatal("rejected publication changed root")
				}
			}
			m.readRootHook = nil
			if fault == "lost-ack" || fault == "unknown-readback" {
				// Retry can confirm only the exact already published relocation.
				if _, err = p.CommitPrefixCompaction(ctx, prepared); err != nil {
					t.Fatal(err)
				}
			} else if fault == "drop" {
				if _, err = p.CommitPrefixCompaction(ctx, prepared); err != nil {
					t.Fatal(err)
				}
			} else if fault == "append" || fault == "reader" || fault == "retire" || fault == "collector" || fault == "collector-at-cas" {
				if !errors.Is(err, ErrConflict) {
					t.Fatal("stale original head", err)
				}
			}
		})
	}
}

// An authenticated source edge never substitutes for its original receipt grant.
func TestGraphPrefixCompactionRechecksOriginalGrant(t *testing.T) {
	for _, phase := range []string{"prepare", "commit"} {
		for _, fault := range []string{"missing", "destination", "location"} {
			t.Run(phase+"/"+fault, func(t *testing.T) {
				m, p := newModel("revoked-" + phase + "-" + fault)
				root := appendOne(t, m, p, "owner", []byte("record"), [][]byte{[]byte("payload")})
				source, err := retainedgraph.Read(ctx, stageStore{protocol: p}, root.Graph, 0)
				if err != nil {
					t.Fatal(err)
				}
				var plan PreparedCompaction
				if phase == "commit" {
					plan, err = p.PreparePrefixCompaction(ctx, "owner", root.Head, 1, 1024, epoch.Add(time.Second), nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				scope, owner, err := objectAuthority(blobpublication.Object{Key: source.Blobs[0].Hash, Reference: source.Blobs[0].Reference})
				if err != nil {
					t.Fatal(err)
				}
				record := m.blobs[scope]
				fence := cloneFence(record.Fence)
				intent := fence.Intents[owner]
				switch fault {
				case "missing":
					delete(fence.Intents, owner)
				case "destination":
					intent.Destination = "foreign"
					fence.Intents[owner] = intent
				case "location":
					intent.Locations = []Location{{Kind: "payload", First: 1}}
					fence.Intents[owner] = intent
				}
				record.Fence = fence
				m.blobs[scope] = record
				if phase == "prepare" {
					_, err = p.PreparePrefixCompaction(ctx, "owner", root.Head, 1, 1024, epoch.Add(time.Second), nil)
				} else {
					_, err = p.CommitPrefixCompaction(ctx, plan)
				}
				if err == nil {
					t.Fatal("revoked source grant accepted", phase, fault)
				}
				if !reflect.DeepEqual(root, m.roots["owner"]) {
					t.Fatal("revoked compaction published")
				}
			})
		}
	}
}

func TestGraphPrefixCompactionRejectsRevokedNodeGrants(t *testing.T) {
	for _, stream := range []string{"", PrefixArchiveStream} {
		for _, height := range []uint8{0, 1} {
			for _, fault := range []string{"missing", "destination", "location", "height", "stream", "kind", "closed"} {
				t.Run(fmt.Sprintf("stream=%s/height=%d/%s", stream, height, fault), func(t *testing.T) {
					m, p := newModel("node-revoke")
					var root Root
					for i := 0; i < 4; i++ {
						root = appendOne(t, m, p, "owner", []byte(fmt.Sprint(i)), [][]byte{[]byte("shared")})
					}
					plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Second), nil)
					if err != nil {
						t.Fatal(err)
					}
					var selected retainedgraph.Link
					err = retainedgraph.Walk(ctx, stageStore{protocol: p}, selectGraph(plan.publication.Graph, plan.publication.Streams, stream), func(link retainedgraph.Link, isNode bool) error {
						if !isNode || selected.Hash != "" {
							return nil
						}
						raw, err := m.Get(ctx, link, retainedgraph.MaxNodeBytes)
						if err != nil {
							return err
						}
						var node struct {
							Height uint8 `json:"height"`
						}
						if err = json.Unmarshal(raw, &node); err != nil {
							return err
						}
						if node.Height == height {
							selected = link
						}
						return nil
					})
					if err != nil || selected.Hash == "" {
						t.Fatal(selected, err)
					}
					scope, owner, err := objectAuthority(blobpublication.Object{Key: selected.Hash, Reference: selected.Reference})
					if err != nil {
						t.Fatal(err)
					}
					if owner != plan.publication.Token {
						t.Fatal("not a relocated node", owner)
					}
					record := m.blobs[scope]
					fence := cloneFence(record.Fence)
					intent := fence.Intents[owner]
					switch fault {
					case "missing":
						delete(fence.Intents, owner)
					case "destination":
						intent.Destination = "foreign"
						fence.Intents[owner] = intent
					case "location":
						intent.Locations = []Location{{Kind: "node", First: 100, Height: height, Stream: stream}}
						fence.Intents[owner] = intent
					case "height":
						intent.Locations[0].Height = height + 1
						fence.Intents[owner] = intent
					case "stream":
						intent.Locations[0].Stream = "foreign"
						fence.Intents[owner] = intent
					case "kind":
						intent.Locations[0].Kind = "payload"
						fence.Intents[owner] = intent
					case "closed":
						fence.Phase = "closed"
						fence.Intents = map[string]Intent{}
					}
					record.Fence = fence
					m.blobs[scope] = record
					before := cloneRoot(m.roots["owner"])
					if _, err = p.CommitPrefixCompaction(ctx, plan); err == nil {
						t.Fatal("revoked relocated node grant accepted", selected)
					}
					if !reflect.DeepEqual(before, m.roots["owner"]) {
						t.Fatal("rejected node grant changed canonical root")
					}
				})
			}
		}
	}
}

// Fresh, correctly granted target bytes still have to equal the source records.
// Rebuild a complete target forest so failures cannot rely on a corrupt hash or
// missing grant to mask an omitted source/target comparison.
func TestGraphPrefixCompactionRejectsChangedRelocationRecords(t *testing.T) {
	for _, stream := range []string{"", PrefixArchiveStream} {
		for _, fault := range []string{"data", "payload"} {
			t.Run("stream="+stream+"/"+fault, func(t *testing.T) {
				m, p := newModel("changed-relocation")
				var root Root
				for i := 0; i < 4; i++ {
					root = appendOne(t, m, p, "owner", []byte(fmt.Sprint(i)), [][]byte{[]byte(fmt.Sprint("payload", i))})
				}
				plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Second), nil)
				if err != nil {
					t.Fatal(err)
				}
				old := selectGraph(plan.publication.Graph, plan.publication.Streams, stream)
				rebuilt := retainedgraph.Empty()
				for index := uint64(0); index < old.Count; index++ {
					record, err := retainedgraph.Read(ctx, stageStore{protocol: p}, old, index)
					if err != nil {
						t.Fatal(err)
					}
					if index == 0 {
						if fault == "data" {
							record.Data = []byte("replacement")
						} else {
							data := []byte("replacement payload")
							ref, err := p.acquire(ctx, key(data), data, plan.publication.Token, Intent{Destination: plan.destination, Expected: plan.expected, Expires: plan.expires, Locations: []Location{{Kind: "payload", First: index, Stream: stream}}})
							if err != nil {
								t.Fatal(err)
							}
							record.Blobs[0] = retainedgraph.Link{Hash: key(data), Reference: ref}
						}
					}
					rebuilt, err = retainedgraph.Append(ctx, stageStore{protocol: p, destination: plan.destination, expected: plan.expected, expires: plan.expires, token: plan.publication.Token, index: index, stream: stream}, rebuilt, record)
					if err != nil {
						t.Fatal(err)
					}
				}
				if stream == "" {
					plan.publication.Graph = rebuilt
				} else {
					setStream(&plan.publication, stream, rebuilt)
				}
				if _, err = p.CommitPrefixCompaction(ctx, plan); err == nil {
					t.Fatal("changed relocation accepted", fault)
				}
				if !reflect.DeepEqual(root, m.roots["owner"]) {
					t.Fatal("changed relocation published")
				}
			})
		}
	}
}
