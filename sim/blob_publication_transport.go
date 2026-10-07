package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"js-wf/internal/blobpublication"
)

// BlobPublicationTransport models the experimental protocol's durable CAS
// authorities and immutable object attempts. It makes no NATS conformance claim.
type BlobPublicationTransport struct {
	mu              sync.Mutex
	schedule        *Scheduler
	blobs           map[string]blobpublication.Record
	roots           map[string]blobpublication.Root
	objects         map[string][]byte
	faults          map[string][]AppendFault
	serial          uint64
	beforeOperation string
	before          func() error
}

var _ blobpublication.Port = (*BlobPublicationTransport)(nil)

func NewBlobPublicationTransport(s *Scheduler) *BlobPublicationTransport {
	return &BlobPublicationTransport{schedule: s, blobs: map[string]blobpublication.Record{}, roots: map[string]blobpublication.Root{}, objects: map[string][]byte{}, faults: map[string][]AppendFault{}}
}
func (m *BlobPublicationTransport) Protocol() blobpublication.Protocol {
	return blobpublication.Protocol{Port: m, NewID: func() (string, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.serial++
		id := fmt.Sprint(m.serial)
		m.event("id", id, 0, m.serial, nil, "ok")
		return id, nil
	}}
}
func publicationFenceCopy(f blobpublication.Fence) blobpublication.Fence {
	n := f
	n.Intents = map[string]blobpublication.Intent{}
	for k, v := range f.Intents {
		n.Intents[k] = v
	}
	return n
}
func publicationRootCopy(r blobpublication.Root) blobpublication.Root {
	n := r
	n.Blobs = map[string]blobpublication.Reference{}
	for k, v := range r.Blobs {
		n.Blobs[k] = v
	}
	n.Data = append([]byte(nil), r.Data...)
	return n
}
func (m *BlobPublicationTransport) event(op, subject string, expected, seq uint64, value any, outcome string) {
	data, _ := json.Marshal(value)
	m.schedule.RecordTransport(TransportEvent{Operation: "publication_" + op, Subject: subject, Expected: expected, Sequence: seq, DataSHA256: digest(data), Outcome: outcome})
}
func (m *BlobPublicationTransport) QueueFault(op string, f AppendFault) error {
	switch op {
	case "cas_blob", "cas_root", "put", "delete":
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
func (m *BlobPublicationTransport) PauseBefore(op string, callback func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beforeOperation = op
	m.before = callback
}
func (m *BlobPublicationTransport) enter(ctx context.Context, op, subject string) (AppendFault, error) {
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
func (m *BlobPublicationTransport) finish(op, subject string, expected, seq uint64, value any, f AppendFault) error {
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
func (m *BlobPublicationTransport) ReadBlob(ctx context.Context, k string) (blobpublication.Record, error) {
	_, err := m.enter(ctx, "read_blob", k)
	if err != nil {
		return blobpublication.Record{}, err
	}
	defer m.mu.Unlock()
	r := m.blobs[k]
	r.Fence = publicationFenceCopy(r.Fence)
	m.event("read_blob", k, 0, r.Revision, r, "ok")
	return r, nil
}
func (m *BlobPublicationTransport) CASBlob(ctx context.Context, k string, revision uint64, f blobpublication.Fence) (blobpublication.Record, error) {
	fault, err := m.enter(ctx, "cas_blob", k)
	if err != nil {
		return blobpublication.Record{}, err
	}
	defer m.mu.Unlock()
	r := m.blobs[k]
	if r.Revision != revision {
		m.event("cas_blob", k, revision, r.Revision, f, "conflict")
		return blobpublication.Record{}, blobpublication.ErrConflict
	}
	if revision == ^uint64(0) {
		return blobpublication.Record{}, fmt.Errorf("revision exhausted")
	}
	r = blobpublication.Record{Revision: revision + 1, Fence: publicationFenceCopy(f)}
	m.blobs[k] = r
	if err = m.finish("cas_blob", k, revision, r.Revision, r, fault); err != nil {
		return blobpublication.Record{}, err
	}
	r.Fence = publicationFenceCopy(r.Fence)
	return r, nil
}
func (m *BlobPublicationTransport) ReadRoot(ctx context.Context, k string) (blobpublication.Root, error) {
	_, err := m.enter(ctx, "read_root", k)
	if err != nil {
		return blobpublication.Root{}, err
	}
	defer m.mu.Unlock()
	r := publicationRootCopy(m.roots[k])
	m.event("read_root", k, 0, r.Head, r, "ok")
	return r, nil
}
func (m *BlobPublicationTransport) CASRoot(ctx context.Context, k string, head uint64, r blobpublication.Root) (blobpublication.Root, error) {
	fault, err := m.enter(ctx, "cas_root", k)
	if err != nil {
		return blobpublication.Root{}, err
	}
	defer m.mu.Unlock()
	current := m.roots[k]
	if current.Head != head {
		m.event("cas_root", k, head, current.Head, r, "conflict")
		return blobpublication.Root{}, blobpublication.ErrConflict
	}
	if head == ^uint64(0) {
		return blobpublication.Root{}, fmt.Errorf("head exhausted")
	}
	r = publicationRootCopy(r)
	r.Head = head + 1
	m.roots[k] = r
	if err = m.finish("cas_root", k, head, r.Head, r, fault); err != nil {
		return blobpublication.Root{}, err
	}
	return publicationRootCopy(r), nil
}
func (m *BlobPublicationTransport) Put(ctx context.Context, name string, b []byte) error {
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
func (m *BlobPublicationTransport) Delete(ctx context.Context, name string) error {
	fault, err := m.enter(ctx, "delete", name)
	if err != nil {
		return err
	}
	defer m.mu.Unlock()
	delete(m.objects, name)
	return m.finish("delete", name, 0, 0, nil, fault)
}
func (m *BlobPublicationTransport) BlobKeys(ctx context.Context) ([]string, error) {
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
func (m *BlobPublicationTransport) Objects(ctx context.Context) ([]blobpublication.Object, error) {
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
func (m *BlobPublicationTransport) CheckReferences() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for destination, r := range m.roots {
		for k, ref := range r.Blobs {
			data, ok := m.objects[ref.Object]
			if !ok || digest(data) != k {
				return fmt.Errorf("dangling blob at %s: %s", destination, ref.Object)
			}
			f := m.blobs[k].Fence
			if f.Phase != "ready" || f.Generation != ref.Generation || f.Object != ref.Object {
				return fmt.Errorf("closed referenced generation")
			}
			intent, ok := f.Intents[r.Token]
			if !ok || intent.Root != destination || intent.Expected >= r.Head {
				return fmt.Errorf("missing committed protection")
			}
		}
	}
	m.event("check_references", "", 0, uint64(len(m.objects)), nil, "ok")
	return nil
}
