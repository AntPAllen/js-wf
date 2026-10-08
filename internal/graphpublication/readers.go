package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"time"
	"unicode/utf8"

	"js-wf/internal/retainedgraph"
)

// Reader is a destination-bound lease handle. Callers must finish reading before
// its expiry or renew it first. A copied snapshot alone does not keep bytes live.
type Reader struct {
	destination string
	id          string
	graph       retainedgraph.Root
}

const ReaderCheckpointSchema = "js-wf-graph-reader-handle-v1"
const MaxReaderCheckpointBytes = 2048

// readerCheckpoint carries identity and the exact snapshot fingerprint, never
// lease expiry or ownership. Resumption obtains both from canonical authority.
type readerCheckpoint struct {
	Schema         string
	Destination    string
	ID             string
	SnapshotSHA256 string
}

func validReaderCheckpoint(v readerCheckpoint) bool {
	return v.Schema == ReaderCheckpointSchema && v.Destination != "" && utf8.ValidString(v.Destination) && len(v.Destination) <= 256 && validID(v.ID) && validHash(v.SnapshotSHA256)
}

// Checkpoint encodes a bounded handle for durable client-side storage. These
// bytes cannot create a pin, extend expiry or adopt an arbitrary snapshot.
func (r Reader) Checkpoint() ([]byte, error) {
	graph, err := r.graph.Encode()
	if err != nil {
		return nil, err
	}
	v := readerCheckpoint{Schema: ReaderCheckpointSchema, Destination: r.destination, ID: r.id, SnapshotSHA256: key(graph)}
	if !validReaderCheckpoint(v) {
		return nil, errors.New("invalid reader checkpoint")
	}
	data, err := json.Marshal(v)
	if err != nil || len(data) > MaxReaderCheckpointBytes {
		return nil, errors.New("reader checkpoint byte limit")
	}
	return data, nil
}

// ResumeReader restores an unexpired exact pin using a quorum-witnessed root.
// It does not mutate logical authority. A released/expired/missing or changed
// snapshot fails closed even if all of its old bytes remain physically present.
// now must use the same collection-consistent clock as ReadRetained.
func (p Protocol) ResumeReader(ctx context.Context, data []byte, now time.Time) (Reader, Root, error) {
	if err := ctx.Err(); err != nil {
		return Reader{}, Root{}, err
	}
	if p.Port == nil || now.IsZero() || len(data) == 0 || len(data) > MaxReaderCheckpointBytes {
		return Reader{}, Root{}, errors.New("invalid reader resumption")
	}
	var v readerCheckpoint
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return Reader{}, Root{}, err
	}
	if d.Decode(new(any)) != io.EOF {
		return Reader{}, Root{}, errors.New("trailing reader checkpoint")
	}
	canonical, err := json.Marshal(v)
	if err != nil || !bytes.Equal(canonical, data) || !validReaderCheckpoint(v) {
		return Reader{}, Root{}, errors.New("noncanonical or invalid reader checkpoint")
	}
	root, err := p.readRoot(ctx, v.Destination)
	if err != nil {
		return Reader{}, Root{}, err
	}
	for _, pin := range root.Readers {
		if pin.ID != v.ID {
			continue
		}
		encoded, err := pin.Graph.Encode()
		if err != nil {
			return Reader{}, Root{}, err
		}
		if key(encoded) != v.SnapshotSHA256 || !now.Before(pin.Expires) {
			return Reader{}, Root{}, ErrRevoked
		}
		return Reader{destination: v.Destination, id: v.ID, graph: copyGraph(pin.Graph)}, root, nil
	}
	return Reader{}, Root{}, ErrRevoked
}

func copyGraph(g retainedgraph.Root) retainedgraph.Root {
	g.Frontier = append([]retainedgraph.Tree{}, g.Frontier...)
	return g
}
func copyReaders(pins []ReaderPin) []ReaderPin {
	if len(pins) == 0 {
		return nil
	}
	result := append([]ReaderPin(nil), pins...)
	for i := range result {
		result[i].Graph = copyGraph(result[i].Graph)
	}
	return result
}

// Snapshot returns an independent copy of the captured forest. It cannot be
// supplied to AcquireReader to adopt arbitrary historical objects.
func (r Reader) Snapshot() retainedgraph.Root { return copyGraph(r.graph) }

func (p Protocol) readerRoot(ctx context.Context, destination string, expected uint64) (Root, error) {
	if err := ctx.Err(); err != nil {
		return Root{}, err
	}
	if p.Port == nil || destination == "" || !utf8.ValidString(destination) || len(destination) > 256 || expected == math.MaxUint64 {
		return Root{}, errors.New("invalid reader destination/head")
	}
	root, err := p.readRoot(ctx, destination)
	if err != nil {
		return Root{}, err
	}
	if root.Head != expected {
		return Root{}, ErrConflict
	}
	return root, nil
}

// readerCAS never retries an uncertain mutation. Only the exact original-head
// image witnessed at expected+1 resolves a lost acknowledgement.
func (p Protocol) readerCAS(ctx context.Context, destination string, expected uint64, next Root) (Root, error) {
	ack, err := p.casRoot(ctx, destination, expected, next)
	if err == nil || ctx.Err() != nil || errors.Is(err, ErrConflict) {
		return ack, err
	}
	actual, readErr := p.readRoot(ctx, destination)
	if readErr == nil && actual.Head == expected+1 && samePublication(actual, next) && reflect.DeepEqual(actual.Readers, next.Readers) {
		return actual, nil
	}
	return Root{}, err
}

// AcquireReader captures only the current canonical live graph, at the caller's
// original head. The first acquisition permanently upgrades this destination's
// root schema; old graph adapters must reject it before collection.
func (p Protocol) AcquireReader(ctx context.Context, destination string, expected uint64, expires time.Time) (Reader, Root, error) {
	if expires.IsZero() {
		return Reader{}, Root{}, errors.New("reader expiry required")
	}
	root, err := p.readerRoot(ctx, destination, expected)
	if err != nil {
		return Reader{}, Root{}, err
	}
	if len(root.Readers) >= MaxReaders {
		return Reader{}, Root{}, errors.New("reader slot limit")
	}
	id, err := p.id()
	if err != nil {
		return Reader{}, Root{}, err
	}
	if !validID(id) {
		return Reader{}, Root{}, errors.New("invalid reader ID")
	}
	for _, pin := range root.Readers {
		if pin.ID == id {
			return Reader{}, Root{}, errors.New("reader ID reused")
		}
	}
	reader := Reader{destination: destination, id: id, graph: copyGraph(root.Graph)}
	root.Schema = RetentionSchema
	root.Readers = append(copyReaders(root.Readers), ReaderPin{ID: id, Expires: expires.UTC(), Graph: copyGraph(root.Graph)})
	ack, err := p.readerCAS(ctx, destination, expected, root)
	if err != nil {
		return Reader{}, Root{}, err
	}
	return reader, ack, nil
}

func readerIndex(root Root, reader Reader) (int, error) {
	if !validID(reader.id) {
		return -1, ErrRevoked
	}
	for i, pin := range root.Readers {
		if pin.ID == reader.id {
			if !reflect.DeepEqual(pin.Graph, reader.graph) {
				return -1, ErrRevoked
			}
			return i, nil
		}
	}
	return -1, ErrRevoked
}

// RenewReader extends an existing pin. Once an expiry/release CAS removes it,
// renewal cannot recreate it, even while its old objects still physically exist.
func (p Protocol) RenewReader(ctx context.Context, reader Reader, expected uint64, expires time.Time) (Root, error) {
	root, err := p.readerRoot(ctx, reader.destination, expected)
	if err != nil {
		return Root{}, err
	}
	i, err := readerIndex(root, reader)
	if err != nil {
		return Root{}, err
	}
	if expires.IsZero() || !expires.After(root.Readers[i].Expires) {
		return Root{}, errors.New("reader renewal must extend expiry")
	}
	root.Readers = copyReaders(root.Readers)
	root.Readers[i].Expires = expires.UTC()
	return p.readerCAS(ctx, reader.destination, expected, root)
}

// ReleaseReader removes exactly this pin, fencing pending renewal at its old
// head. Released handles cannot reattach their snapshots.
func (p Protocol) ReleaseReader(ctx context.Context, reader Reader, expected uint64) (Root, error) {
	root, err := p.readerRoot(ctx, reader.destination, expected)
	if err != nil {
		return Root{}, err
	}
	i, err := readerIndex(root, reader)
	if err != nil {
		return Root{}, err
	}
	root.Readers = copyReaders(root.Readers)
	root.Readers = append(root.Readers[:i], root.Readers[i+1:]...)
	if len(root.Readers) == 0 {
		root.Readers = nil
	}
	return p.readerCAS(ctx, reader.destination, expected, root)
}

// ReadRetained validates the live lease before reading a path. The caller's
// collection clock must be consistent with now and its deadline must fall before
// expiry. A lease expiring during this call can cause a read error; it never
// grants authority to extend the lease by observing bytes.
func (p Protocol) ReadRetained(ctx context.Context, reader Reader, index uint64, now time.Time) (retainedgraph.Record, error) {
	if p.Port == nil || now.IsZero() {
		return retainedgraph.Record{}, errors.New("port and reader time required")
	}
	root, err := p.readRoot(ctx, reader.destination)
	if err != nil {
		return retainedgraph.Record{}, err
	}
	i, err := readerIndex(root, reader)
	if err != nil {
		return retainedgraph.Record{}, err
	}
	if !now.Before(root.Readers[i].Expires) {
		return retainedgraph.Record{}, ErrRevoked
	}
	return retainedgraph.Read(ctx, stageStore{protocol: p}, reader.graph, index)
}

func (p Protocol) pruneReaders(ctx context.Context, destination string, root Root, now time.Time) (Root, error) {
	kept := make([]ReaderPin, 0, len(root.Readers))
	for _, reader := range root.Readers {
		if now.Before(reader.Expires) {
			kept = append(kept, reader)
		}
	}
	if len(kept) == len(root.Readers) {
		return root, nil
	}
	next := root
	next.Readers = copyReaders(kept)
	// A lost reply stops collection. Renewal and this CAS contend on the same
	// original head, so no expired pin can be ignored without first fencing it.
	return p.casRoot(ctx, destination, root.Head, next)
}

// ExpireReaders fences expired pins at a known destination, including empty
// snapshots with no object grants for Sweep to discover. The runtime must retain
// and enumerate its destination catalog; object enumeration is not that catalog.
func (p Protocol) ExpireReaders(ctx context.Context, destination string, expected uint64, now time.Time) (Root, error) {
	if now.IsZero() {
		return Root{}, errors.New("reader collection time required")
	}
	root, err := p.readerRoot(ctx, destination, expected)
	if err != nil {
		return Root{}, err
	}
	return p.pruneReaders(ctx, destination, root, now)
}
