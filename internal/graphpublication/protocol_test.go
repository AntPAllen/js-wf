package graphpublication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

var lostReply = errors.New("committed reply lost")
var epoch = time.Unix(1000, 0).UTC()
var ctx = context.Background()

type memoryPort struct {
	blobs         map[string]Record
	roots         map[string]Root
	observedRoots map[string]bool
	objects       map[string][]byte
	serial        int
	prefix        string
	rootBefore    func(string, uint64, Root) error
	rootAfter     func() error
	blobAfter     func(string, Fence) error
	putHook       func(string, []byte) error
	deleteHook    func(string) error
	readRootHook  func(string) error
	readBlobHook  func(string) error
	deletes       int
}

func cloneFence(f Fence) Fence {
	n := f
	n.Intents = map[string]Intent{}
	for k, v := range f.Intents {
		v.Locations = append([]Location{}, v.Locations...)
		n.Intents[k] = v
	}
	return n
}
func cloneRoot(r Root) Root {
	r.Graph.Frontier = append([]retainedgraph.Tree{}, r.Graph.Frontier...)
	r.Streams = copyStreams(r.Streams)
	r.Readers = copyReaders(r.Readers)
	r.Application = append([]byte(nil), r.Application...)
	return r
}
func newModel(prefix string) (*memoryPort, Protocol) {
	prefix = strings.ReplaceAll(prefix, "_", "-")
	m := &memoryPort{blobs: map[string]Record{}, roots: map[string]Root{}, objects: map[string][]byte{}, prefix: prefix}
	return m, Protocol{Port: m, NewID: func() (string, error) { m.serial++; return fmt.Sprintf("%s-%d", prefix, m.serial), nil }}
}
func (m *memoryPort) ReadRoot(c context.Context, k string) (Root, error) {
	if err := c.Err(); err != nil {
		return Root{}, err
	}
	if m.readRootHook != nil {
		if err := m.readRootHook(k); err != nil {
			return Root{}, err
		}
	}
	r, ok := m.roots[k]
	if m.observedRoots == nil {
		m.observedRoots = map[string]bool{}
	}
	m.observedRoots[k] = true
	if !ok {
		return EmptyRoot(), nil
	}
	return cloneRoot(r), nil
}
func (m *memoryPort) CASRoot(c context.Context, k string, head uint64, r Root) (Root, error) {
	if err := c.Err(); err != nil {
		return Root{}, err
	}
	if m.rootBefore != nil {
		hook := m.rootBefore
		m.rootBefore = nil
		if err := hook(k, head, r); err != nil {
			return Root{}, err
		}
	}
	if m.roots[k].Head != head {
		return Root{}, ErrConflict
	}
	if schemaRank(r.Schema) < schemaRank(m.roots[k].Schema) {
		return Root{}, errors.New("retention schema downgrade")
	}
	r = cloneRoot(r)
	r.Head = head + 1
	m.roots[k] = r
	if m.rootAfter != nil {
		hook := m.rootAfter
		m.rootAfter = nil
		if err := hook(); err != nil {
			return Root{}, err
		}
	}
	return cloneRoot(r), nil
}
func (m *memoryPort) ReadBlob(c context.Context, k string) (Record, error) {
	if err := c.Err(); err != nil {
		return Record{}, err
	}
	if m.readBlobHook != nil {
		if err := m.readBlobHook(k); err != nil {
			return Record{}, err
		}
	}
	r := m.blobs[k]
	r.Fence = cloneFence(r.Fence)
	return r, nil
}
func (m *memoryPort) CASBlob(c context.Context, k string, revision uint64, f Fence) (Record, error) {
	if err := c.Err(); err != nil {
		return Record{}, err
	}
	if m.blobs[k].Revision != revision {
		return Record{}, ErrConflict
	}
	r := Record{Revision: revision + 1, Fence: cloneFence(f)}
	m.blobs[k] = r
	if m.blobAfter != nil {
		if err := m.blobAfter(k, f); err != nil {
			return Record{}, err
		}
	}
	r.Fence = cloneFence(r.Fence)
	return r, nil
}
func (m *memoryPort) BlobKeys(c context.Context) ([]string, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	keys := []string{}
	for k := range m.blobs {
		keys = append(keys, k)
	}
	return keys, nil
}
func (m *memoryPort) RootKeys(c context.Context) ([]string, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for k := range m.roots {
		seen[k] = true
	}
	for k := range m.observedRoots {
		seen[k] = true
	}
	keys := []string{}
	for k := range seen {
		keys = append(keys, k)
	}
	return keys, nil
}
func (m *memoryPort) Put(c context.Context, n string, data []byte) error {
	if err := c.Err(); err != nil {
		return err
	}
	if m.putHook != nil {
		hook := m.putHook
		m.putHook = nil
		return hook(n, data)
	}
	if _, ok := m.objects[n]; ok {
		return errors.New("immutable physical name reused")
	}
	m.objects[n] = bytes.Clone(data)
	return nil
}
func (m *memoryPort) Get(c context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	data, ok := m.objects[link.Reference.Object]
	if !ok {
		return nil, errors.New("missing graph bytes")
	}
	if len(data) > limit {
		return nil, errors.New("bounded read exceeded")
	}
	return bytes.Clone(data), nil
}
func (m *memoryPort) Delete(c context.Context, n string) error {
	if err := c.Err(); err != nil {
		return err
	}
	if m.deleteHook != nil {
		hook := m.deleteHook
		m.deleteHook = nil
		if err := hook(n); err != nil {
			return err
		}
	}
	delete(m.objects, n)
	m.deletes++
	return nil
}
func (m *memoryPort) Objects(c context.Context) ([]blobpublication.Object, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	objects := []blobpublication.Object{}
	for name := range m.objects {
		parts := strings.Split(name, "/")
		if len(parts) != 3 {
			return nil, errors.New("bad physical name")
		}
		gen, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		objects = append(objects, blobpublication.Object{Key: parts[0], Reference: blobpublication.Reference{Generation: gen, Object: name}})
	}
	return objects, nil
}
func appendOne(t *testing.T, m *memoryPort, p Protocol, destination string, data []byte, payloads [][]byte) Root {
	t.Helper()
	base, err := m.ReadRoot(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := p.PrepareAppend(ctx, destination, base.Head, data, payloads, epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// Census consumes raw immutable objects, not production membership/Walk APIs.
// Every selected node and external edge must remain physically readable and
// match its content hash, even when its original root token is no longer current.
func census(t *testing.T, m *memoryPort, root Root, want [][]byte) map[string]bool {
	t.Helper()
	live := map[string]bool{}
	leaves := 0
	read := func(link retainedgraph.Link) []byte {
		data, ok := m.objects[link.Reference.Object]
		if !ok {
			t.Fatalf("live object deleted: %s", link.Reference.Object)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != link.Hash || !link.Reference.ValidFor(link.Hash) {
			t.Fatal("live physical identity changed")
		}
		live[link.Reference.Object] = true
		return data
	}
	var visit func(retainedgraph.Tree)
	visit = func(tree retainedgraph.Tree) {
		var n struct {
			Schema   string
			First    uint64
			Height   uint8
			Children []retainedgraph.Link
			Record   *retainedgraph.Record
		}
		if err := json.Unmarshal(read(tree.Link), &n); err != nil {
			t.Fatal(err)
		}
		if n.First != tree.First || n.Height != tree.Height || n.Schema != retainedgraph.Schema {
			t.Fatal("selected graph position changed")
		}
		if n.Record != nil {
			if n.Height != 0 || n.First >= uint64(len(want)) || !bytes.Equal(n.Record.Data, want[n.First]) {
				t.Fatal("selected leaf metadata changed")
			}
			for _, link := range n.Record.Blobs {
				read(link)
			}
			leaves++
			return
		}
		if len(n.Children) != 2 || n.Height == 0 {
			t.Fatal("selected branch malformed")
		}
		visit(retainedgraph.Tree{First: n.First, Height: n.Height - 1, Link: n.Children[0]})
		visit(retainedgraph.Tree{First: n.First + (uint64(1) << (n.Height - 1)), Height: n.Height - 1, Link: n.Children[1]})
	}
	for _, tree := range root.Graph.Frontier {
		visit(tree)
	}
	if leaves != len(want) || uint64(leaves) != root.Graph.Count {
		t.Fatal("selected graph lost population")
	}
	return live
}

func TestGraphPublicationSeededAppendsSweepAndRetirement(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		t.Run(strconv.FormatInt(seed, 10), func(t *testing.T) {
			m, p := newModel(fmt.Sprintf("seed%d", seed))
			random := rand.New(rand.NewSource(seed))
			want := [][]byte{}
			for i := 0; i < 32; i++ {
				payloads := [][]byte{[]byte(fmt.Sprintf("shared%d", random.Intn(4)))}
				if random.Intn(3) == 0 {
					old, _ := m.ReadRoot(ctx, "owner")
					fork, err := p.PrepareAppend(ctx, "owner", old.Head, []byte("abandoned"), payloads, epoch.Add(time.Second))
					if err != nil {
						t.Fatal(err)
					}
					_ = fork // Keep its registered objects; do not publish this competing fork.
				}
				data := []byte(fmt.Sprintf("%d/%d", seed, i))
				root := appendOne(t, m, p, "owner", data, payloads)
				want = append(want, data)
				census(t, m, root, want)
				if random.Intn(4) == 0 || i == 31 {
					if _, err := p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
						t.Fatal(err)
					}
					census(t, m, root, want)
				}
			}
			root, _ := m.ReadRoot(ctx, "owner")
			if err := p.Retire(ctx, "owner", root.Head); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if len(m.objects) != 0 {
				t.Fatal("retired graph left physical objects", len(m.objects))
			}
			for _, r := range m.blobs {
				if r.Fence.Phase != "closed" || len(r.Fence.Intents) != 0 {
					t.Fatal("retired graph lost closed high-water fences")
				}
			}
			final, _ := m.ReadRoot(ctx, "owner")
			if final.Head <= root.Head || final.Graph.Count != 0 {
				t.Fatal("retirement reset canonical head")
			}
		})
	}
	t.Log("GRAPH_PUBLICATION_SEEDED seeds=32 committed_records=1024 complete_retirements=32 live_census_before_after_sweeps=true")
}

func TestGraphPublicationCollectorAndCommitInterleavings(t *testing.T) {
	for _, winner := range []string{"commit", "collector"} {
		t.Run(winner, func(t *testing.T) {
			m, p := newModel(winner)
			live := appendOne(t, m, p, "owner", []byte("inherited"), [][]byte{[]byte("large")})
			prepared, err := p.PrepareAppend(ctx, "owner", live.Head, []byte("next"), [][]byte{[]byte("large")}, epoch.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if winner == "commit" {
				m.rootBefore = func(_ string, _ uint64, _ Root) error { _, err := p.Commit(ctx, prepared); return err }
				if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				root, _ := m.ReadRoot(ctx, "owner")
				census(t, m, root, [][]byte{[]byte("inherited"), []byte("next")})
			} else {
				m.rootBefore = func(_ string, _ uint64, _ Root) error { _, err := p.Sweep(ctx, epoch.Add(time.Hour)); return err }
				if _, err = p.Commit(ctx, prepared); !errors.Is(err, ErrConflict) {
					t.Fatal("fenced append adopted", err)
				}
				if _, err = p.Commit(ctx, prepared); !errors.Is(err, ErrConflict) {
					t.Fatal("late append resurrected", err)
				}
				root, _ := m.ReadRoot(ctx, "owner")
				if root.Head <= live.Head {
					t.Fatal("collector did not fence original head")
				}
				census(t, m, root, [][]byte{[]byte("inherited")})
				appendOne(t, m, p, "owner", []byte("fresh"), [][]byte{[]byte("large")})
				if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				root, _ = m.ReadRoot(ctx, "owner")
				census(t, m, root, [][]byte{[]byte("inherited"), []byte("fresh")})
			}
		})
	}
}

func TestGraphPublicationUnknownRepliesNeverAdoptUploads(t *testing.T) {
	for _, mode := range []string{"put_lost", "ready_lost", "root_lost", "root_lost_replaced"} {
		t.Run(mode, func(t *testing.T) {
			m, p := newModel(mode)
			if mode == "put_lost" {
				m.putHook = func(name string, data []byte) error { m.objects[name] = bytes.Clone(data); return lostReply }
			}
			if mode == "ready_lost" {
				m.blobAfter = func(_ string, f Fence) error {
					if f.Phase == "ready" {
						m.blobAfter = nil
						return lostReply
					}
					return nil
				}
			}
			prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("first"), [][]byte{[]byte("payload")}, epoch.Add(time.Second))
			if mode == "put_lost" || mode == "ready_lost" {
				if !errors.Is(err, lostReply) {
					t.Fatal("upload uncertainty accepted", err)
				}
				root, _ := m.ReadRoot(ctx, "owner")
				if root.Graph.Count != 0 {
					t.Fatal("upload bytes became authority")
				}
				if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				if len(m.objects) != 0 {
					t.Fatal("abandoned upload not reclaimed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			m.rootAfter = func() error {
				if mode == "root_lost_replaced" {
					appendOne(t, m, p, "owner", []byte("second"), nil)
				}
				return lostReply
			}
			root, err := p.Commit(ctx, prepared)
			if mode == "root_lost" {
				if err != nil || root.Graph.Count != 1 {
					t.Fatal("exact canonical readback failed", err)
				}
				census(t, m, root, [][]byte{[]byte("first")})
			} else {
				if !errors.Is(err, lostReply) {
					t.Fatal("different canonical publication accepted", err)
				}
				root, _ = m.ReadRoot(ctx, "owner")
				census(t, m, root, [][]byte{[]byte("first"), []byte("second")})
			}
		})
	}
}

func TestGraphPublicationForeignDestinationAndRevokedGrants(t *testing.T) {
	m, p := newModel("foreign")
	prepared, err := p.PrepareAppend(ctx, "a", 0, []byte("a"), nil, epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	forged := prepared
	forged.destination = "b"
	if _, err = p.Commit(ctx, forged); !errors.Is(err, ErrRevoked) {
		t.Fatal("foreign destination adopted", err)
	}
	other, otherP := newModel("other")
	if _, err = otherP.Commit(ctx, prepared); err == nil {
		t.Fatal("foreign authority adopted absent grants")
	}
	if len(other.objects) != 0 {
		t.Fatal("foreign commit created objects")
	}
	if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Commit(ctx, prepared); !errors.Is(err, ErrConflict) {
		t.Fatal("expired original head adopted", err)
	}
	for _, record := range m.blobs {
		if record.Fence.Phase != "closed" {
			t.Fatal("expired generation not closed")
		}
	}
}

func TestGraphPublicationUncertainCensusFailsClosed(t *testing.T) {
	for _, mode := range []string{"root_read", "blob_read", "missing_ancestor", "schema", "unknown_object"} {
		t.Run(mode, func(t *testing.T) {
			m, p := newModel(mode)
			want := [][]byte{[]byte("0"), []byte("1"), []byte("2"), []byte("3")}
			var root Root
			for _, data := range want {
				root = appendOne(t, m, p, "owner", data, [][]byte{[]byte("payload")})
			}
			if mode == "root_read" {
				m.readRootHook = func(string) error { return lostReply }
			}
			if mode == "blob_read" {
				m.readBlobHook = func(string) error { return lostReply }
			}
			if mode == "missing_ancestor" {
				delete(m.objects, root.Graph.Frontier[0].Link.Reference.Object)
			}
			if mode == "schema" {
				r := m.roots["owner"]
				r.Schema = "legacy"
				m.roots["owner"] = r
			}
			if mode == "unknown_object" {
				data := []byte("unknown")
				m.objects[objectName(key(data), 1, "unknown", "upload")] = data
			}
			before := len(m.objects)
			if _, err := p.Sweep(ctx, epoch.Add(time.Hour)); err == nil {
				t.Fatal("uncertain collection accepted")
			}
			if len(m.objects) != before || m.deletes != 0 {
				t.Fatal("uncertain census deleted physical objects")
			}
		})
	}
}

func TestGraphPublicationDelayedDeleteCannotTouchNewGeneration(t *testing.T) {
	m, p := newModel("delayed")
	old := appendOne(t, m, p, "owner", []byte("same"), [][]byte{[]byte("payload")})
	if err := p.Retire(ctx, "owner", old.Head); err != nil {
		t.Fatal(err)
	}
	m.deleteHook = func(string) error {
		appendOne(t, m, p, "owner", []byte("same"), [][]byte{[]byte("payload")})
		return nil
	}
	if _, err := p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	root, _ := m.ReadRoot(ctx, "owner")
	census(t, m, root, [][]byte{[]byte("same")})
	if _, err := p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	census(t, m, root, [][]byte{[]byte("same")})
	for _, tree := range root.Graph.Frontier {
		if tree.Link.Reference.Object == old.Graph.Frontier[0].Link.Reference.Object {
			t.Fatal("retirement reused an old physical identity")
		}
	}
	for _, record := range m.blobs {
		if record.Fence.Generation == 0 {
			t.Fatal("permanent authority lost its generation")
		}
	}
}

func TestGraphPublicationContextAndCopies(t *testing.T) {
	m, p := newModel("copies")
	root := appendOne(t, m, p, "owner", []byte("record"), [][]byte{[]byte("payload")})
	read, _ := m.ReadRoot(ctx, "owner")
	read.Graph.Frontier[0].Link.Reference.Generation++
	if reflect.DeepEqual(read, m.roots["owner"]) {
		t.Fatal("authority root aliases caller")
	}
	for k := range m.blobs {
		read, _ := m.ReadBlob(ctx, k)
		for token, intent := range read.Fence.Intents {
			intent.Locations[0].First++
			read.Fence.Intents[token] = intent
		}
		if reflect.DeepEqual(read, m.blobs[k]) {
			t.Fatal("authority grant locations alias caller")
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.PrepareAppend(canceled, "owner", root.Head, nil, nil, epoch.Add(time.Second)); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled append contacted authority", err)
	}
	if _, err := p.Sweep(canceled, epoch.Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled sweep ignored", err)
	}
}

func TestGraphPublicationMetadataBoundAndPermanentScopedGenerations(t *testing.T) {
	m, p := newModel("bounds")
	maxMetadata := 0
	for i := 0; i < 128; i++ {
		appendOne(t, m, p, "owner", []byte("record"), [][]byte{[]byte("reused-content")})
	}
	for _, record := range m.blobs {
		if len(record.Fence.Intents) != 1 {
			t.Fatal("scope combined publications")
		}
		data, err := json.Marshal(record.Fence)
		if err != nil {
			t.Fatal(err)
		}
		maxMetadata = max(maxMetadata, len(data))
		if len(data) > 2048 {
			t.Fatal("authority metadata grew with retained history")
		}
	}
	m, p = newModel("late")
	prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("record"), nil, epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	link := prepared.publication.Graph.Frontier[0].Link
	data := bytes.Clone(m.objects[link.Reference.Object])
	scope := authorityKey(link.Hash, prepared.publication.Token)
	original := m.blobs[scope].Fence.Intents[prepared.publication.Token]
	if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	generation := m.blobs[scope].Fence.Generation
	ref, err := p.acquire(ctx, link.Hash, data, prepared.publication.Token, original)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Generation != generation+1 || ref.Object == link.Reference.Object {
		t.Fatal("late attempt reset permanent scope or reused old object")
	}
	if _, err = p.Commit(ctx, prepared); !errors.Is(err, ErrConflict) {
		t.Fatal("late attempt escaped original head fence", err)
	}
	if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	closed := m.blobs[scope].Fence
	if closed.Generation != generation+1 || closed.Phase != "closed" || len(m.objects) != 0 {
		t.Fatal("late generation did not close permanently")
	}
	t.Logf("GRAPH_PUBLICATION_BOUNDS records=128 max_authority_bytes=%d max_grants_per_scope=1 late_generation=%d retained_closed=true", maxMetadata, closed.Generation)
}

func TestGraphPublicationRejectsForeignPhysicalOwner(t *testing.T) {
	m, p := newModel("misbound")
	prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("record"), nil, epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	link := prepared.publication.Graph.Frontier[0].Link
	scope := authorityKey(link.Hash, prepared.publication.Token)
	record := m.blobs[scope]
	record.Fence.Object = objectName(link.Hash, record.Fence.Generation, "foreign", "upload")
	m.blobs[scope] = record
	if _, err = p.Commit(ctx, prepared); err == nil {
		t.Fatal("metadata selected another owner's physical object")
	}
	if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err == nil {
		t.Fatal("misbound metadata allowed collection")
	}
	if m.deletes != 0 {
		t.Fatal("misbound collection deleted objects")
	}
}

type acknowledgmentFaultPort struct{ Port }

func (p acknowledgmentFaultPort) CASRoot(_ context.Context, _ string, head uint64, root Root) (Root, error) {
	// No write occurred, and the malformed acknowledgment claims a wrong head.
	root.Head = head + 2
	return root, nil
}
func (p acknowledgmentFaultPort) CASBlob(c context.Context, k string, rev uint64, f Fence) (Record, error) {
	record, err := p.Port.CASBlob(c, k, rev, f)
	if err != nil {
		return record, err
	}
	record.Revision++
	return record, nil
}

func TestGraphPublicationInvalidAcknowledgmentsAndInputBounds(t *testing.T) {
	m, p := newModel("ack")
	prepared, err := p.PrepareAppend(ctx, "owner", 0, []byte("record"), nil, epoch.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	bad := p
	bad.Port = acknowledgmentFaultPort{Port: m}
	if _, err = bad.Commit(ctx, prepared); err == nil {
		t.Fatal("invalid root acknowledgment adopted")
	}
	root, _ := m.ReadRoot(ctx, "owner")
	if root.Graph.Count != 0 {
		t.Fatal("false root acknowledgment became canonical")
	}
	m, p = newModel("bloback")
	bad = p
	bad.Port = acknowledgmentFaultPort{Port: m}
	if _, err = bad.PrepareAppend(ctx, "owner", 0, []byte("record"), nil, epoch.Add(time.Second)); err == nil {
		t.Fatal("invalid blob acknowledgment adopted")
	}
	if len(m.objects) != 0 {
		t.Fatal("invalid registering acknowledgment started upload")
	}
	m, p = newModel("bounds2")
	badExpires := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err = p.PrepareAppend(ctx, "owner", 0, nil, nil, badExpires); err == nil {
		t.Fatal("unencodable intent expiration accepted")
	}
	if len(m.blobs) != 0 || len(m.objects) != 0 {
		t.Fatal("invalid expiration mutated authority")
	}
	if _, err = p.PrepareAppend(ctx, "owner", 0, make([]byte, retainedgraph.MaxDataBytes+1), nil, epoch.Add(time.Second)); err == nil {
		t.Fatal("unbounded leaf metadata accepted")
	}
	if _, err = p.PrepareAppend(ctx, "owner", 0, nil, make([][]byte, retainedgraph.MaxBlobReferences+1), epoch.Add(time.Second)); err == nil {
		t.Fatal("unbounded payload edges accepted")
	}
	if len(m.blobs) != 0 || len(m.objects) != 0 {
		t.Fatal("invalid bounds started uploading")
	}
}
