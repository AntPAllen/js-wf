// Package retainedgraph stages immutable append trees for canonical runtime
// storage. It does not publish authority roots or enable online collection.
// Integrating it requires transitive ownership fencing and a fail-closed schema
// migration; the existing direct-reference collector cannot collect these trees.
package retainedgraph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"js-wf/internal/blobpublication"
)

const Schema = "js-wf-retained-append-graph-v1"
const MaxDataBytes = 64 << 10
const MaxBlobReferences = 128
const MaxNodeBytes = 192 << 10
const MaxRootBytes = 32 << 10

var ErrInvalid = errors.New("invalid retained append graph")
var ErrIndex = errors.New("retained graph index outside population")

// Link binds encoded node bytes to one immutable physical generation.
type Link struct {
	Hash      string                    `json:"hash"`
	Reference blobpublication.Reference `json:"reference"`
}
type Tree struct {
	First  uint64 `json:"first"`
	Height uint8  `json:"height"`
	Link   Link   `json:"link"`
}

// Root is a bounded frontier, not a flat inventory. Only a caller's authoritative
// root can establish ownership. A Root returned by Append is still unpublished.
type Root struct {
	Schema   string `json:"schema"`
	Count    uint64 `json:"count"`
	Frontier []Tree `json:"frontier"`
}

// Record contains bounded metadata and direct external payload references.
// Larger payloads must be uploaded separately by the owning publication protocol.
type Record struct {
	Data  []byte `json:"data"`
	Blobs []Link `json:"blobs"`
}
type node struct {
	Schema   string  `json:"schema"`
	First    uint64  `json:"first"`
	Height   uint8   `json:"height"`
	Children []Link  `json:"children,omitempty"`
	Record   *Record `json:"record,omitempty"`
}

// Store stages nodes under the caller's pending publication. Success means
// immutable bytes matching hash have been uploaded, and an exact physical
// receipt is returned. Errors, including lost replies, never mean success.
// Get must honor ctx and the byte limit before returning an unbounded object.
// This interface alone does not establish durable ownership or reclamation.
type Store interface {
	Put(context.Context, string, []byte) (blobpublication.Reference, error)
	Get(context.Context, Link, int) ([]byte, error)
}

func Empty() Root               { return Root{Schema: Schema, Frontier: []Tree{}} }
func invalid(what string) error { return fmt.Errorf("%w: %s", ErrInvalid, what) }
func digest(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func validLink(l Link) bool {
	b, err := hex.DecodeString(l.Hash)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == l.Hash && len(l.Reference.Object) <= 256 && l.Reference.ValidFor(l.Hash)
}
func width(height uint8) uint64 { return uint64(1) << height }

func (r Root) Validate() error {
	if r.Schema != Schema || r.Count > math.MaxInt64 || r.Frontier == nil || len(r.Frontier) > 63 {
		return invalid("root schema or population")
	}
	var first uint64
	previous := uint8(63)
	for _, t := range r.Frontier {
		if t.Height >= previous || t.Height > 62 || t.First != first || !validLink(t.Link) || width(t.Height) > r.Count-first {
			return invalid("frontier range, order or receipt")
		}
		first += width(t.Height)
		previous = t.Height
	}
	if first != r.Count {
		return invalid("frontier does not cover population")
	}
	return nil
}
func (r Root) Encode() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(r)
	if len(data) > MaxRootBytes {
		return nil, invalid("root byte limit")
	}
	return data, err
}
func decode(data []byte, limit int, value any) error {
	if len(data) > limit {
		return invalid("encoded byte limit")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return invalid("decode: " + err.Error())
	}
	if d.Decode(new(any)) != io.EOF {
		return invalid("trailing JSON")
	}
	// A single canonical representation prevents aliases, duplicate fields and
	// alternate numeric spellings from introducing different graph identities.
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(data, canonical) {
		return invalid("noncanonical encoding")
	}
	return nil
}
func DecodeRoot(data []byte) (Root, error) {
	var r Root
	if err := decode(data, MaxRootBytes, &r); err != nil {
		return Root{}, err
	}
	if err := r.Validate(); err != nil {
		return Root{}, err
	}
	return r, nil
}
func canonicalRecord(r Record) (Record, error) {
	if len(r.Data) > MaxDataBytes || len(r.Blobs) > MaxBlobReferences {
		return Record{}, invalid("record bounds")
	}
	copy := Record{Data: append([]byte{}, r.Data...), Blobs: append([]Link{}, r.Blobs...)}
	sort.Slice(copy.Blobs, func(i, j int) bool { return copy.Blobs[i].Hash < copy.Blobs[j].Hash })
	for i, l := range copy.Blobs {
		if !validLink(l) || i > 0 && copy.Blobs[i-1].Hash == l.Hash {
			return Record{}, invalid("payload receipt or duplicate hash")
		}
	}
	return copy, nil
}
func (n node) validate(t Tree) error {
	if n.Schema != Schema || n.First != t.First || n.Height != t.Height {
		return invalid("node position")
	}
	if n.Height == 0 {
		if n.Record == nil || len(n.Children) != 0 {
			return invalid("leaf shape")
		}
		r, err := canonicalRecord(*n.Record)
		if err != nil {
			return err
		}
		a, _ := json.Marshal(r)
		b, _ := json.Marshal(n.Record)
		if !bytes.Equal(a, b) {
			return invalid("record reference order")
		}
	} else {
		if n.Record != nil || len(n.Children) != 2 || !validLink(n.Children[0]) || !validLink(n.Children[1]) {
			return invalid("branch shape")
		}
	}
	return nil
}
func load(ctx context.Context, store Store, t Tree) (node, error) {
	if err := ctx.Err(); err != nil {
		return node{}, err
	}
	data, err := store.Get(ctx, t.Link, MaxNodeBytes)
	if err != nil {
		return node{}, err
	}
	if err = ctx.Err(); err != nil {
		return node{}, err
	}
	if digest(data) != t.Link.Hash {
		return node{}, invalid("node digest")
	}
	var n node
	if err = decode(data, MaxNodeBytes, &n); err != nil {
		return node{}, err
	}
	if err = n.validate(t); err != nil {
		return node{}, err
	}
	return n, nil
}
func upload(ctx context.Context, store Store, n node) (Tree, error) {
	if err := ctx.Err(); err != nil {
		return Tree{}, err
	}
	data, err := json.Marshal(n)
	if err != nil {
		return Tree{}, err
	}
	if len(data) > MaxNodeBytes {
		return Tree{}, invalid("node byte limit")
	}
	hash := digest(data)
	ref, err := store.Put(ctx, hash, data)
	if err != nil {
		return Tree{}, err
	}
	if err = ctx.Err(); err != nil {
		return Tree{}, err
	}
	t := Tree{First: n.First, Height: n.Height, Link: Link{Hash: hash, Reference: ref}}
	if !validLink(t.Link) {
		return Tree{}, invalid("upload receipt")
	}
	return t, nil
}

// Append stages one leaf and at most logarithmically many merge nodes. It reads
// no unchanged subtrees: callers must obtain root from their canonical authority,
// preserve inherited ownership and fence the eventual root CAS at that revision.
// Neither an upload acknowledgment nor this returned Root is a publication.
// A failed attempt may leave staged objects; its input root remains unchanged.
func Append(ctx context.Context, store Store, root Root, record Record) (Root, error) {
	if err := ctx.Err(); err != nil {
		return Root{}, err
	}
	if err := root.Validate(); err != nil {
		return Root{}, err
	}
	if store == nil || root.Count == math.MaxInt64 {
		return Root{}, invalid("store or exhausted population")
	}
	r, err := canonicalRecord(record)
	if err != nil {
		return Root{}, err
	}
	frontier := append([]Tree{}, root.Frontier...)
	carry, err := upload(ctx, store, node{Schema: Schema, First: root.Count, Record: &r})
	if err != nil {
		return Root{}, err
	}
	for len(frontier) > 0 && frontier[len(frontier)-1].Height == carry.Height {
		left := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		if _, err = load(ctx, store, left); err != nil {
			return Root{}, err
		}
		carry, err = upload(ctx, store, node{Schema: Schema, First: left.First, Height: left.Height + 1, Children: []Link{left.Link, carry.Link}})
		if err != nil {
			return Root{}, err
		}
	}
	result := Root{Schema: Schema, Count: root.Count + 1, Frontier: append(frontier, carry)}
	if err = result.Validate(); err != nil {
		return Root{}, err
	}
	return result, nil
}

func Read(ctx context.Context, store Store, root Root, index uint64) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if err := root.Validate(); err != nil {
		return Record{}, err
	}
	if index >= root.Count {
		return Record{}, ErrIndex
	}
	if store == nil {
		return Record{}, invalid("nil store")
	}
	for _, t := range root.Frontier {
		if index < t.First || index-t.First >= width(t.Height) {
			continue
		}
		for {
			n, err := load(ctx, store, t)
			if err != nil {
				return Record{}, err
			}
			if t.Height == 0 {
				return canonicalRecord(*n.Record)
			}
			height := t.Height - 1
			right := index-t.First >= width(height)
			child := 0
			first := t.First
			if right {
				child = 1
				first += width(height)
			}
			t = Tree{First: first, Height: height, Link: n.Children[child]}
		}
	}
	return Record{}, invalid("uncovered index")
}

// Walk streams every node and external payload edge using O(log n) traversal
// space. A caller must not treat a successful traversal as a collection fence.
// The callback may stop the walk; errors and missing/corrupt nodes fail closed.
func Walk(ctx context.Context, store Store, root Root, visit func(Link, bool) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Validate(); err != nil {
		return err
	}
	if store == nil || visit == nil {
		return invalid("store or visitor")
	}
	var walk func(Tree) error
	walk = func(t Tree) error {
		n, err := load(ctx, store, t)
		if err != nil {
			return err
		}
		if err = visit(t.Link, true); err != nil {
			return err
		}
		if t.Height == 0 {
			for _, l := range n.Record.Blobs {
				if err = ctx.Err(); err != nil {
					return err
				}
				if err = visit(l, false); err != nil {
					return err
				}
			}
			return nil
		}
		height := t.Height - 1
		if err = walk(Tree{First: t.First, Height: height, Link: n.Children[0]}); err != nil {
			return err
		}
		return walk(Tree{First: t.First + width(height), Height: height, Link: n.Children[1]})
	}
	for _, t := range root.Frontier {
		if err := walk(t); err != nil {
			return err
		}
	}
	return ctx.Err()
}
