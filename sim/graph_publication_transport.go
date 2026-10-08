package sim

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

// GraphPublicationTransport models graph ownership durable CAS
// authorities and immutable object attempts. It makes no NATS conformance claim.
type GraphPublicationTransport struct {
	mu              sync.Mutex
	schedule        *Scheduler
	blobs           map[string]graphpublication.Record
	roots           map[string]graphpublication.Root
	observedRoots   map[string]bool
	objects         map[string][]byte
	faults          map[string][]AppendFault
	serial          uint64
	beforeOperation string
	before          func() error
}

var _ graphpublication.Port = (*GraphPublicationTransport)(nil)
var _ graphpublication.RootCatalogPort = (*GraphPublicationTransport)(nil)

func NewGraphPublicationTransport(s *Scheduler) *GraphPublicationTransport {
	return &GraphPublicationTransport{schedule: s, blobs: map[string]graphpublication.Record{}, roots: map[string]graphpublication.Root{}, objects: map[string][]byte{}, faults: map[string][]AppendFault{}}
}
func (m *GraphPublicationTransport) Protocol() graphpublication.Protocol {
	return graphpublication.Protocol{Port: m, NewID: func() (string, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.serial++
		id := fmt.Sprint(m.serial)
		m.event("id", id, 0, m.serial, nil, "ok")
		return id, nil
	}}
}
func graphFenceCopy(f graphpublication.Fence) graphpublication.Fence {
	n := f
	n.Intents = map[string]graphpublication.Intent{}
	for k, v := range f.Intents {
		v.Locations = append([]graphpublication.Location{}, v.Locations...)
		n.Intents[k] = v
	}
	return n
}
func graphRootCopy(r graphpublication.Root) graphpublication.Root {
	r.Graph.Frontier = append([]retainedgraph.Tree{}, r.Graph.Frontier...)
	r.Application = append([]byte(nil), r.Application...)
	r.Readers = append([]graphpublication.ReaderPin(nil), r.Readers...)
	for i := range r.Readers {
		r.Readers[i].Graph.Frontier = append([]retainedgraph.Tree{}, r.Readers[i].Graph.Frontier...)
	}
	return r
}
func (m *GraphPublicationTransport) event(op, subject string, expected, seq uint64, value any, outcome string) {
	data, _ := json.Marshal(value)
	m.schedule.RecordTransport(TransportEvent{Operation: "graph_publication_" + op, Subject: subject, Expected: expected, Sequence: seq, DataSHA256: digest(data), Outcome: outcome})
}
func (m *GraphPublicationTransport) QueueFault(op string, f AppendFault) error {
	switch op {
	case "cas_blob", "cas_root", "put", "delete":
	case "read_blob", "read_root", "get", "objects", "blob_keys", "root_keys":
		if f != DropBeforeCommit {
			return fmt.Errorf("read faults require drop-before-commit")
		}
	default:
		return fmt.Errorf("unsupported publication fault operation %q", op)
	}
	if f != DropBeforeCommit && f != LoseAckAfterCommit {
		return fmt.Errorf("unsupported publication fault %q", f)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults[op] = append(m.faults[op], f)
	return nil
}

// PauseBefore injects one chosen scheduler edge before a transport operation.
// The callback executes outside the transport lock and may run the collector.
func (m *GraphPublicationTransport) PauseBefore(op string, callback func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beforeOperation = op
	m.before = callback
}
func (m *GraphPublicationTransport) enter(ctx context.Context, op, subject string) (AppendFault, error) {
	m.mu.Lock()
	var before func() error
	if m.beforeOperation == op {
		before = m.before
		m.before = nil
		m.beforeOperation = ""
	}
	m.mu.Unlock()
	if before != nil {
		if err := before(); err != nil {
			return "", err
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	var f AppendFault
	if queue := m.faults[op]; len(queue) > 0 {
		f = queue[0]
		m.faults[op] = queue[1:]
	}
	if f == DropBeforeCommit {
		m.event(op, subject, 0, 0, nil, string(f))
		m.mu.Unlock()
		return f, ErrTransportLost
	}
	return f, nil // lock held; caller joins by unlocking
}
func (m *GraphPublicationTransport) finish(op, subject string, expected, seq uint64, value any, f AppendFault) error {
	outcome := "ok"
	if f == LoseAckAfterCommit {
		outcome = string(f)
	}
	m.event(op, subject, expected, seq, value, outcome)
	if f == LoseAckAfterCommit {
		return ErrTransportLost
	}
	return nil
}
func (m *GraphPublicationTransport) ReadBlob(ctx context.Context, k string) (graphpublication.Record, error) {
	_, err := m.enter(ctx, "read_blob", k)
	if err != nil {
		return graphpublication.Record{}, err
	}
	defer m.mu.Unlock()
	r := m.blobs[k]
	r.Fence = graphFenceCopy(r.Fence)
	m.event("read_blob", k, 0, r.Revision, r, "ok")
	return r, nil
}
func (m *GraphPublicationTransport) CASBlob(ctx context.Context, k string, revision uint64, f graphpublication.Fence) (graphpublication.Record, error) {
	fault, err := m.enter(ctx, "cas_blob", k)
	if err != nil {
		return graphpublication.Record{}, err
	}
	defer m.mu.Unlock()
	r := m.blobs[k]
	if r.Revision != revision {
		m.event("cas_blob", k, revision, r.Revision, f, "conflict")
		return graphpublication.Record{}, blobpublication.ErrConflict
	}
	if revision == ^uint64(0) {
		return graphpublication.Record{}, fmt.Errorf("revision exhausted")
	}
	r = graphpublication.Record{Revision: revision + 1, Fence: graphFenceCopy(f)}
	m.blobs[k] = r
	if err = m.finish("cas_blob", k, revision, r.Revision, r, fault); err != nil {
		return graphpublication.Record{}, err
	}
	r.Fence = graphFenceCopy(r.Fence)
	return r, nil
}
func (m *GraphPublicationTransport) ReadRoot(ctx context.Context, k string) (graphpublication.Root, error) {
	_, err := m.enter(ctx, "read_root", k)
	if err != nil {
		return graphpublication.Root{}, err
	}
	defer m.mu.Unlock()
	r, ok := m.roots[k]
	if m.observedRoots == nil {
		m.observedRoots = map[string]bool{}
	}
	m.observedRoots[k] = true
	if !ok {
		r = graphpublication.EmptyRoot()
	}
	r = graphRootCopy(r)
	m.event("read_root", k, 0, r.Head, r, "ok")
	return r, nil
}
func (m *GraphPublicationTransport) CASRoot(ctx context.Context, k string, head uint64, r graphpublication.Root) (graphpublication.Root, error) {
	fault, err := m.enter(ctx, "cas_root", k)
	if err != nil {
		return graphpublication.Root{}, err
	}
	defer m.mu.Unlock()
	current := m.roots[k]
	if current.Head != head {
		m.event("cas_root", k, head, current.Head, r, "conflict")
		return graphpublication.Root{}, blobpublication.ErrConflict
	}
	if head == ^uint64(0) {
		return graphpublication.Root{}, fmt.Errorf("head exhausted")
	}
	if (current.Schema == graphpublication.ApplicationSchema && r.Schema != graphpublication.ApplicationSchema) || (current.Schema == graphpublication.RetentionSchema && r.Schema != graphpublication.RetentionSchema && r.Schema != graphpublication.ApplicationSchema) {
		m.event("cas_root", k, head, current.Head, r, "schema_downgrade")
		return graphpublication.Root{}, fmt.Errorf("retention schema downgrade")
	}
	r = graphRootCopy(r)
	r.Head = head + 1
	m.roots[k] = r
	if err = m.finish("cas_root", k, head, r.Head, r, fault); err != nil {
		return graphpublication.Root{}, err
	}
	return graphRootCopy(r), nil
}
func (m *GraphPublicationTransport) Put(ctx context.Context, name string, b []byte) error {
	fault, err := m.enter(ctx, "put", name)
	if err != nil {
		return err
	}
	defer m.mu.Unlock()
	if _, ok := m.objects[name]; ok {
		m.event("put", name, 0, 0, b, "name_reused")
		return fmt.Errorf("physical name reused")
	}
	m.objects[name] = append([]byte(nil), b...)
	return m.finish("put", name, 0, 0, b, fault)
}
func (m *GraphPublicationTransport) Delete(ctx context.Context, name string) error {
	fault, err := m.enter(ctx, "delete", name)
	if err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.objects, name)
	return m.finish("delete", name, 0, 0, nil, fault)
}
func (m *GraphPublicationTransport) BlobKeys(ctx context.Context) ([]string, error) {
	_, err := m.enter(ctx, "blob_keys", "")
	if err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	keys := []string{}
	for k := range m.blobs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m.event("blob_keys", "", 0, uint64(len(keys)), keys, "ok")
	return keys, nil
}
func (m *GraphPublicationTransport) RootKeys(ctx context.Context) ([]string, error) {
	_, err := m.enter(ctx, "root_keys", "")
	if err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
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
	sort.Strings(keys)
	m.event("root_keys", "", 0, uint64(len(keys)), keys, "ok")
	return keys, nil
}
func (m *GraphPublicationTransport) Objects(ctx context.Context) ([]blobpublication.Object, error) {
	_, err := m.enter(ctx, "objects", "")
	if err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	objects := []blobpublication.Object{}
	for name := range m.objects {
		parts := strings.Split(name, "/")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid physical name")
		}
		gen, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		objects = append(objects, blobpublication.Object{Key: parts[0], Reference: blobpublication.Reference{Generation: gen, Object: name}})
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Reference.Object < objects[j].Reference.Object })
	m.event("objects", "", 0, uint64(len(objects)), objects, "ok")
	return objects, nil
}

func (m *GraphPublicationTransport) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	_, err := m.enter(ctx, "get", link.Reference.Object)
	if err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	data, ok := m.objects[link.Reference.Object]
	if !ok {
		m.event("get", link.Reference.Object, 0, 0, nil, "missing")
		return nil, fmt.Errorf("graph object missing")
	}
	if len(data) > limit {
		m.event("get", link.Reference.Object, 0, 0, nil, "oversized")
		return nil, fmt.Errorf("graph object exceeds limit")
	}
	m.event("get", link.Reference.Object, 0, uint64(len(data)), link, "ok")
	return append([]byte{}, data...), nil
}

// CheckReferences independently follows raw object receipts, without the
// production membership or walk helpers. It checks exact ready origin scopes.
func (m *GraphPublicationTransport) CheckReferences() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for destination, root := range m.roots {
		if (root.Schema != graphpublication.Schema && root.Schema != graphpublication.RetentionSchema && root.Schema != graphpublication.ApplicationSchema) || (root.Schema == graphpublication.Schema && len(root.Readers) != 0) || len(root.Readers) > graphpublication.MaxReaders || len(root.Application) > graphpublication.MaxApplicationBytes || (root.Schema == graphpublication.ApplicationSchema && root.Head == 0) || (root.Schema != graphpublication.ApplicationSchema && len(root.Application) != 0) || root.Graph.Validate() != nil {
			return fmt.Errorf("invalid graph authority")
		}
		graphs := []retainedgraph.Root{root.Graph}
		seenReaders := map[string]bool{}
		for _, pin := range root.Readers {
			if pin.ID == "" || pin.Expires.IsZero() || seenReaders[pin.ID] || pin.Graph.Validate() != nil {
				return fmt.Errorf("invalid reader pin")
			}
			seenReaders[pin.ID] = true
			graphs = append(graphs, pin.Graph)
		}
		for _, graph := range graphs {
			leaves := uint64(0)
			edges := map[uint64]map[retainedgraph.Link]bool{}
			origins := map[retainedgraph.Link][]graphpublication.Location{}
			check := func(link retainedgraph.Link, location graphpublication.Location) ([]byte, error) {
				data, ok := m.objects[link.Reference.Object]
				if !ok || digest(data) != link.Hash || !link.Reference.ValidFor(link.Hash) {
					return nil, fmt.Errorf("dangling graph receipt")
				}
				parts := strings.Split(link.Reference.Object, "/")
				ids := strings.Split(parts[2], "-")
				if len(ids) != 2 {
					return nil, fmt.Errorf("invalid graph physical scope")
				}
				ownerBytes, err := hex.DecodeString(ids[0])
				if err != nil {
					return nil, err
				}
				owner := string(ownerBytes)
				scope := digest([]byte("graph-authority/" + link.Hash + "/" + owner))
				f := m.blobs[scope].Fence
				intent, ok := f.Intents[owner]
				if f.Hash != link.Hash || f.Owner != owner || f.Phase != "ready" || f.Generation != link.Reference.Generation || f.Object != link.Reference.Object || !ok || intent.Destination != destination || intent.Expected >= root.Head {
					return nil, fmt.Errorf("missing graph ownership")
				}
				// Reused payload edges retain their original index; append preserves it.
				if location.Kind == "node" {
					found := false
					for _, registered := range intent.Locations {
						if registered == location {
							found = true
						}
					}
					if !found {
						return nil, fmt.Errorf("node lacks exact coordinate grant")
					}
				} else {
					origins[link] = append([]graphpublication.Location{}, intent.Locations...)
				}
				return data, nil
			}
			var visit func(retainedgraph.Tree) error
			visit = func(tree retainedgraph.Tree) error {
				data, err := check(tree.Link, graphpublication.Location{Kind: "node", First: tree.First, Height: tree.Height})
				if err != nil {
					return err
				}
				var node struct {
					Schema   string
					First    uint64
					Height   uint8
					Children []retainedgraph.Link
					Record   *retainedgraph.Record
				}
				if err = json.Unmarshal(data, &node); err != nil {
					return err
				}
				if node.Schema != retainedgraph.Schema || node.First != tree.First || node.Height != tree.Height {
					return fmt.Errorf("graph position changed")
				}
				if node.Height == 0 {
					if node.Record == nil || len(node.Children) != 0 {
						return fmt.Errorf("invalid graph leaf")
					}
					for _, link := range node.Record.Blobs {
						if _, err = check(link, graphpublication.Location{Kind: "payload", First: node.First}); err != nil {
							return err
						}
						if edges[node.First] == nil {
							edges[node.First] = map[retainedgraph.Link]bool{}
						}
						edges[node.First][link] = true
					}
					leaves++
					return nil
				}
				if node.Record != nil || len(node.Children) != 2 {
					return fmt.Errorf("invalid graph branch")
				}
				if err = visit(retainedgraph.Tree{First: tree.First, Height: tree.Height - 1, Link: node.Children[0]}); err != nil {
					return err
				}
				return visit(retainedgraph.Tree{First: tree.First + (uint64(1) << (tree.Height - 1)), Height: tree.Height - 1, Link: node.Children[1]})
			}
			for _, tree := range graph.Frontier {
				if err := visit(tree); err != nil {
					return err
				}
			}
			if leaves != graph.Count {
				return fmt.Errorf("graph census lost population")
			}
			for link, locations := range origins {
				found := false
				for _, location := range locations {
					if location.Kind == "payload" && edges[location.First][link] {
						found = true
					}
				}
				if !found {
					return fmt.Errorf("reused payload lost its canonical origin edge")
				}
			}
		}
	}
	m.event("check_references", "", 0, uint64(len(m.objects)), nil, "ok")
	return nil
}
