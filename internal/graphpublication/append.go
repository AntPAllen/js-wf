package graphpublication

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

func (p Protocol) readRoot(ctx context.Context, destination string) (Root, error) {
	root, err := p.Port.ReadRoot(ctx, destination)
	if err != nil {
		return Root{}, err
	}
	return normalizeRoot(root)
}
func samePublication(a, b Root) bool {
	return a.Schema == b.Schema && a.Token == b.Token && reflect.DeepEqual(a.Graph, b.Graph)
}
func (p Protocol) casRoot(ctx context.Context, destination string, expected uint64, root Root) (Root, error) {
	if expected == math.MaxUint64 {
		return Root{}, errors.New("graph head exhausted")
	}
	root.Head = expected + 1
	var err error
	root, err = normalizeRoot(root)
	if err != nil {
		return Root{}, err
	}
	ack, err := p.Port.CASRoot(ctx, destination, expected, root)
	if err != nil {
		return Root{}, err
	}
	ack, err = normalizeRoot(ack)
	if err != nil {
		return Root{}, err
	}
	if ack.Head != expected+1 || !samePublication(ack, root) {
		return Root{}, errors.New("invalid graph root acknowledgment")
	}
	return ack, nil
}
func (p Protocol) casBlob(ctx context.Context, k string, revision uint64, f Fence) (Record, error) {
	if revision == math.MaxUint64 {
		return Record{}, errors.New("graph blob revision exhausted")
	}
	if err := validateFence(k, Record{Revision: revision + 1, Fence: f}); err != nil {
		return Record{}, err
	}
	ack, err := p.Port.CASBlob(ctx, k, revision, f)
	if err != nil {
		return Record{}, err
	}
	if err = validateFence(k, ack); err != nil {
		return Record{}, err
	}
	if ack.Revision != revision+1 || !reflect.DeepEqual(ack.Fence, f) {
		return Record{}, errors.New("invalid graph blob acknowledgment")
	}
	return ack, nil
}

// PrepareAppend acquires expiring pins before every payload/node upload. The
// caller supplies its original canonical head, never a refreshed replacement.
// Metadata belongs in the leaf; large payloads are separate immutable objects.
// Failed or ambiguous uploads are abandoned, never adopted by presence checks.
func (p Protocol) PrepareAppend(ctx context.Context, destination string, expected uint64, data []byte, payloads [][]byte, expires time.Time) (Prepared, error) {
	return p.PrepareAppendWithOwned(ctx, destination, expected, data, payloads, nil, expires)
}

// PrepareAppendWithOwned reuses exact payload edges already owned by this
// destination's canonical graph, without uploading their bytes or rewriting
// their origin grants. Only append and whole-graph retirement are supported:
// partial compaction must transfer ownership before dropping origin leaves.
func (p Protocol) PrepareAppendWithOwned(ctx context.Context, destination string, expected uint64, data []byte, payloads [][]byte, owned []OwnedPayload, expires time.Time) (Prepared, error) {
	if err := ctx.Err(); err != nil {
		return Prepared{}, err
	}
	if p.Port == nil || destination == "" || len(destination) > 256 || expected == math.MaxUint64 || expires.IsZero() || len(data) > retainedgraph.MaxDataBytes || len(payloads) > retainedgraph.MaxBlobReferences || len(owned) > retainedgraph.MaxBlobReferences-len(payloads) {
		return Prepared{}, errors.New("invalid graph append configuration")
	}
	base, err := p.readRoot(ctx, destination)
	if err != nil {
		return Prepared{}, err
	}
	if base.Head != expected {
		return Prepared{}, ErrConflict
	}
	if base.Graph.Count == math.MaxInt64 {
		return Prepared{}, errors.New("graph population exhausted")
	}
	token, err := p.id()
	if err != nil {
		return Prepared{}, err
	}
	if !validID(token) {
		return Prepared{}, errors.New("invalid publication ID")
	}
	expires = expires.UTC()
	result := Prepared{destination: destination, expected: expected, expires: expires, base: base, publication: Root{Schema: Schema, Token: token}, owned: map[retainedgraph.Link]uint64{}}
	selected := map[string]retainedgraph.Link{}
	for _, payload := range owned {
		if err = p.verifyOwned(ctx, destination, base, payload); err != nil {
			return Prepared{}, err
		}
		if previous, exists := selected[payload.Link.Hash]; exists && previous != payload.Link {
			return Prepared{}, errors.New("ambiguous owned payload generations")
		}
		selected[payload.Link.Hash] = payload.Link
		if index, exists := result.owned[payload.Link]; !exists || payload.Index < index {
			result.owned[payload.Link] = payload.Index
		}
	}
	unique := map[string][]byte{}
	for _, payload := range payloads {
		unique[key(payload)] = payload
	}
	keys := make([]string, 0, len(unique))
	for k := range unique {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	record := retainedgraph.Record{Data: append([]byte{}, data...), Blobs: []retainedgraph.Link{}}
	for _, k := range keys {
		if _, exists := selected[k]; exists {
			continue
		}
		intent := Intent{Destination: destination, Expected: expected, Expires: expires, Locations: []Location{{Kind: "payload", First: base.Graph.Count}}}
		ref, err := p.acquire(ctx, k, unique[k], token, intent)
		if err != nil {
			return Prepared{}, err
		}
		selected[k] = retainedgraph.Link{Hash: k, Reference: ref}
	}
	for _, link := range selected {
		record.Blobs = append(record.Blobs, link)
	}
	next, err := retainedgraph.Append(ctx, stageStore{protocol: p, destination: destination, expected: expected, expires: expires, token: token, index: base.Graph.Count}, base.Graph, record)
	if err != nil {
		return Prepared{}, err
	}
	result.publication.Graph = next
	return result, nil
}

type stageStore struct {
	protocol    Protocol
	destination string
	expected    uint64
	expires     time.Time
	token       string
	index       uint64
}

func (s stageStore) Put(ctx context.Context, k string, data []byte) (blobpublication.Reference, error) {
	var header struct {
		Schema string
		First  uint64
		Height uint8
	}
	if len(data) > retainedgraph.MaxNodeBytes || key(data) != k || json.Unmarshal(data, &header) != nil || header.Schema != retainedgraph.Schema || header.Height > 62 {
		return blobpublication.Reference{}, errors.New("invalid staged graph node")
	}
	span := uint64(1) << header.Height
	if span-1 > s.index || header.First != s.index-(span-1) {
		return blobpublication.Reference{}, errors.New("staged node outside append spine")
	}
	intent := Intent{Destination: s.destination, Expected: s.expected, Expires: s.expires, Locations: []Location{{Kind: "node", First: header.First, Height: header.Height}}}
	return s.protocol.acquire(ctx, k, data, s.token, intent)
}
func (s stageStore) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	return s.protocol.Port.Get(ctx, link, limit)
}

// Commit validates inheritance and each exact new grant before original-head
// CAS. Collection revokes a pending grant by fencing that head first. A lost
// CAS reply is accepted only by exact canonical quorum-witnessed publication
// readback, never by observing uploads or a matching content hash alone.
func (p Protocol) Commit(ctx context.Context, prepared Prepared) (Root, error) {
	if err := ctx.Err(); err != nil {
		return Root{}, err
	}
	if p.Port == nil || prepared.destination == "" || !validID(prepared.publication.Token) {
		return Root{}, errors.New("invalid prepared graph append")
	}
	current, err := p.readRoot(ctx, prepared.destination)
	if err != nil {
		return Root{}, err
	}
	if current.Head > prepared.expected && samePublication(current, prepared.publication) {
		return current, nil
	}
	if current.Head != prepared.expected || !samePublication(current, prepared.base) {
		return Root{}, ErrConflict
	}
	delta, err := retainedgraph.ValidateAppend(ctx, stageStore{protocol: p}, prepared.base.Graph, prepared.publication.Graph)
	if err != nil {
		return Root{}, err
	}
	check := func(link retainedgraph.Link, location Location) error {
		record, err := p.Port.ReadBlob(ctx, authorityKey(link.Hash, prepared.publication.Token))
		if err != nil {
			return err
		}
		if err = validateFence(authorityKey(link.Hash, prepared.publication.Token), record); err != nil {
			return err
		}
		f := record.Fence
		intent, ok := f.Intents[prepared.publication.Token]
		if f.Phase != "ready" || f.Generation != link.Reference.Generation || f.Object != link.Reference.Object || !ok || intent.Destination != prepared.destination || intent.Expected != prepared.expected || !intent.Expires.Equal(prepared.expires) {
			return ErrRevoked
		}
		for _, registered := range intent.Locations {
			if registered == location {
				return nil
			}
		}
		return ErrRevoked
	}
	for _, tree := range delta.Nodes {
		if err = check(tree.Link, Location{Kind: "node", First: tree.First, Height: tree.Height}); err != nil {
			return Root{}, err
		}
	}
	for _, link := range delta.Record.Blobs {
		if index, exists := prepared.owned[link]; exists {
			if err = p.verifyOwned(ctx, prepared.destination, prepared.base, OwnedPayload{Index: index, Link: link}); err != nil {
				return Root{}, err
			}
			continue
		}
		if err = check(link, Location{Kind: "payload", First: prepared.base.Graph.Count}); err != nil {
			return Root{}, err
		}
	}
	root, err := p.casRoot(ctx, prepared.destination, prepared.expected, prepared.publication)
	if err == nil {
		return root, nil
	}
	if ctx.Err() != nil {
		return Root{}, err
	}
	actual, readErr := p.readRoot(ctx, prepared.destination)
	if readErr == nil && actual.Head > prepared.expected && samePublication(actual, prepared.publication) {
		return actual, nil
	}
	return Root{}, err
}

// Retire clears the entire graph but preserves the destination high-water head.
// This is not partial history compaction, retention lease management or import.
func (p Protocol) Retire(ctx context.Context, destination string, expected uint64) error {
	if p.Port == nil || destination == "" {
		return errors.New("invalid graph retirement")
	}
	_, err := p.casRoot(ctx, destination, expected, EmptyRoot())
	return err
}
