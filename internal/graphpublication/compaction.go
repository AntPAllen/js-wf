package graphpublication

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"time"
	"unicode/utf8"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

const PrefixArchiveStream = "archive"

// PreparedCompaction is an original-head relocation plan. Its source, cut and
// resulting forests cannot be supplied or mutated by an external caller.
type PreparedCompaction struct {
	destination       string
	expected, first   uint64
	expires           time.Time
	base, publication Root
}

// PreparePrefixCompaction stages the live prefix in the reserved archive stream
// and reindexes the live suffix from zero. All relocated payloads get independent
// grants and physical receipts: origin locations cannot survive reindexing.
// Existing archives, other streams and reader snapshots retain their indices.
// Application bytes must encode the caller's matching logical cursor transition.
// This alone publishes nothing and does not authorize arbitrary replacement.
func (p Protocol) PreparePrefixCompaction(ctx context.Context, destination string, expected, first uint64, maxPayloadBytes int, expires time.Time, application []byte) (PreparedCompaction, error) {
	if err := ctx.Err(); err != nil {
		return PreparedCompaction{}, err
	}
	if p.Port == nil || destination == "" || !utf8.ValidString(destination) || len(destination) > 256 || expected == math.MaxUint64 || first == 0 || maxPayloadBytes < 1 || expires.IsZero() || len(application) > MaxApplicationBytes {
		return PreparedCompaction{}, errors.New("invalid prefix compaction")
	}
	base, err := p.readRoot(ctx, destination)
	if err != nil {
		return PreparedCompaction{}, err
	}
	if base.Head != expected {
		return PreparedCompaction{}, ErrConflict
	}
	if first > base.Graph.Count {
		return PreparedCompaction{}, errors.New("compaction cut outside live graph")
	}
	archive := selectGraph(base.Graph, base.Streams, PrefixArchiveStream)
	if archive.Count > math.MaxInt64-first {
		return PreparedCompaction{}, errors.New("archive population exhausted")
	}
	exists := false
	for _, stream := range base.Streams {
		exists = exists || stream.Name == PrefixArchiveStream
	}
	if !exists && len(base.Streams) == MaxStreams {
		return PreparedCompaction{}, errors.New("stream limit")
	}
	token, err := p.id()
	if err != nil {
		return PreparedCompaction{}, err
	}
	if !validID(token) {
		return PreparedCompaction{}, errors.New("invalid publication ID")
	}
	result := PreparedCompaction{destination: destination, expected: expected, first: first, expires: expires.UTC(), base: base, publication: Root{Schema: StreamsSchema, Token: token, Graph: retainedgraph.Empty(), Streams: copyStreams(base.Streams), Readers: copyReaders(base.Readers), Application: bytes.Clone(application)}}
	cache := map[string]retainedgraph.Link{}
	locations := map[string]map[string]bool{}
	err = retainedgraph.ReadRange(ctx, stageStore{protocol: p}, base.Graph, 0, base.Graph.Count, func(sourceIndex uint64, source retainedgraph.Record) error {
		target, stream := result.publication.Graph, ""
		if sourceIndex < first {
			target, stream = archive, PrefixArchiveStream
		}
		record := retainedgraph.Record{Data: bytes.Clone(source.Data), Blobs: []retainedgraph.Link{}}
		for _, link := range source.Blobs {
			if err := p.verifyOwnedGrant(ctx, destination, base, OwnedPayload{Index: sourceIndex, Link: link}); err != nil {
				return err
			}
			copied, known := cache[link.Hash]
			if !known || !locations[link.Hash][stream] {
				var data []byte
				if !known {
					data, err = p.Port.Get(ctx, link, maxPayloadBytes)
					if err != nil {
						return err
					}
					if len(data) > maxPayloadBytes || key(data) != link.Hash {
						return errors.New("relocation payload digest or bound")
					}
				}
				intent := Intent{Destination: destination, Expected: expected, Expires: result.expires, Locations: []Location{{Kind: "payload", First: target.Count, Stream: stream}}}
				ref, err := p.acquire(ctx, link.Hash, data, token, intent)
				if err != nil {
					return err
				}
				copied = retainedgraph.Link{Hash: link.Hash, Reference: ref}
				if known && copied != cache[link.Hash] {
					return errors.New("relocation receipt changed")
				}
				if !known {
					cache[link.Hash] = copied
					locations[link.Hash] = map[string]bool{}
				}
				locations[link.Hash][stream] = true
			}
			record.Blobs = append(record.Blobs, copied)
		}
		next, err := retainedgraph.Append(ctx, stageStore{protocol: p, destination: destination, expected: expected, expires: result.expires, token: token, index: target.Count, stream: stream}, target, record)
		if err != nil {
			return err
		}
		if stream == PrefixArchiveStream {
			archive = next
		} else {
			result.publication.Graph = next
		}
		return nil
	})
	if err != nil {
		return PreparedCompaction{}, err
	}
	setStream(&result.publication, PrefixArchiveStream, archive)
	result.publication.Head = expected + 1
	if _, err := normalizeRoot(result.publication); err != nil {
		return PreparedCompaction{}, err
	}
	return result, nil
}

func (p Protocol) checkCompactionGrant(ctx context.Context, prepared PreparedCompaction, link retainedgraph.Link, stream string) error {
	scope, owner, err := objectAuthority(blobpublication.Object{Key: link.Hash, Reference: link.Reference})
	if err != nil {
		return err
	}
	if owner != prepared.publication.Token {
		return ErrRevoked
	}
	record, err := p.Port.ReadBlob(ctx, scope)
	if err != nil {
		return err
	}
	if err := validateFence(scope, record); err != nil {
		return err
	}
	f := record.Fence
	intent, ok := f.Intents[owner]
	if f.Phase != "ready" || f.Generation != link.Reference.Generation || f.Object != link.Reference.Object || !ok || intent.Destination != prepared.destination || intent.Expected != prepared.expected || !intent.Expires.Equal(prepared.expires) {
		return ErrRevoked
	}
	graph := selectGraph(prepared.publication.Graph, prepared.publication.Streams, stream)
	for _, location := range intent.Locations {
		if location.Stream != stream {
			continue
		}
		var present bool
		if location.Kind == "payload" {
			present, err = retainedgraph.ContainsBlob(ctx, stageStore{protocol: p}, graph, location.First, link)
		} else {
			present, err = retainedgraph.ContainsNode(ctx, stageStore{protocol: p}, graph, retainedgraph.Tree{First: location.First, Height: location.Height, Link: link})
		}
		if err != nil {
			return err
		}
		if present {
			return nil
		}
	}
	return ErrRevoked
}

// CommitPrefixCompaction checks the exact relocation, every new grant and
// preserved snapshots, then publishes at the captured head. Lost acknowledgments
// require exact quorum-confirmed publication readback, as with ordinary append.
func (p Protocol) CommitPrefixCompaction(ctx context.Context, prepared PreparedCompaction) (Root, error) {
	if err := ctx.Err(); err != nil {
		return Root{}, err
	}
	if p.Port == nil || prepared.destination == "" || !validID(prepared.publication.Token) || prepared.first == 0 || prepared.first > prepared.base.Graph.Count {
		return Root{}, errors.New("invalid prepared compaction")
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
	if !reflect.DeepEqual(prepared.base.Readers, prepared.publication.Readers) {
		return Root{}, errors.New("compaction changed readers")
	}
	oldArchive := selectGraph(prepared.base.Graph, prepared.base.Streams, PrefixArchiveStream)
	nextArchive := selectGraph(prepared.publication.Graph, prepared.publication.Streams, PrefixArchiveStream)
	preserved := prepared.base
	preserved.Graph = prepared.publication.Graph
	setStream(&preserved, PrefixArchiveStream, nextArchive)
	if !reflect.DeepEqual(preserved.Streams, prepared.publication.Streams) || prepared.publication.Graph.Count != prepared.base.Graph.Count-prepared.first || nextArchive.Count != oldArchive.Count+prepared.first {
		return Root{}, errors.New("invalid compaction forest set")
	}
	for _, tree := range oldArchive.Frontier {
		present, err := retainedgraph.ContainsNode(ctx, stageStore{protocol: p}, nextArchive, tree)
		if err != nil {
			return Root{}, err
		}
		if !present {
			return Root{}, errors.New("compaction replaced inherited archive")
		}
	}
	for sourceIndex := uint64(0); sourceIndex < prepared.base.Graph.Count; sourceIndex++ {
		source, err := retainedgraph.Read(ctx, stageStore{protocol: p}, prepared.base.Graph, sourceIndex)
		if err != nil {
			return Root{}, err
		}
		stream, index := "", sourceIndex-prepared.first
		if sourceIndex < prepared.first {
			stream, index = PrefixArchiveStream, oldArchive.Count+sourceIndex
		}
		next, err := retainedgraph.Read(ctx, stageStore{protocol: p}, selectGraph(prepared.publication.Graph, prepared.publication.Streams, stream), index)
		if err != nil {
			return Root{}, err
		}
		if !bytes.Equal(source.Data, next.Data) || len(source.Blobs) != len(next.Blobs) {
			return Root{}, errors.New("compaction changed record")
		}
		for i, link := range next.Blobs {
			if link.Hash != source.Blobs[i].Hash {
				return Root{}, errors.New("compaction changed payload")
			}
			if err := p.verifyOwnedGrant(ctx, prepared.destination, prepared.base, OwnedPayload{Index: sourceIndex, Link: source.Blobs[i]}); err != nil {
				return Root{}, err
			}
			if err := p.checkCompactionGrant(ctx, prepared, link, stream); err != nil {
				return Root{}, err
			}
		}
	}
	for _, stream := range []string{"", PrefixArchiveStream} {
		graph := selectGraph(prepared.publication.Graph, prepared.publication.Streams, stream)
		if err := retainedgraph.Walk(ctx, stageStore{protocol: p}, graph, func(link retainedgraph.Link, payload bool) error {
			if payload {
				return nil
			}
			_, owner, err := objectAuthority(blobpublication.Object{Key: link.Hash, Reference: link.Reference})
			if err != nil {
				return err
			}
			if owner != prepared.publication.Token {
				if stream != PrefixArchiveStream {
					return ErrRevoked
				}
				return nil // Inherited archive frontiers were checked above.
			}
			return p.checkCompactionGrant(ctx, prepared, link, stream)
		}); err != nil {
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
