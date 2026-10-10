package graphpublication

import (
	"bytes"
	"context"
	"errors"
	"math"
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
	stage, err := p.BeginPrefixCompaction(ctx, destination, expected, first, maxPayloadBytes, expires, application)
	if err != nil {
		return PreparedCompaction{}, err
	}
	result, done, err := stage.Advance(ctx, stage.prepared.base.Graph.Count)
	if err != nil {
		return PreparedCompaction{}, err
	}
	if !done {
		return PreparedCompaction{}, errors.New("incomplete prefix compaction")
	}
	return result, nil
}

// BeginPrefixCompaction captures the original authority head without publishing.
// Advance stages record batches; Checkpoint can persist completed progress.
// Resumption is staging input, never a verification certificate for Commit.
func (p Protocol) BeginPrefixCompaction(ctx context.Context, destination string, expected, first uint64, maxPayloadBytes int, expires time.Time, application []byte) (*CompactionStage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.Port == nil || destination == "" || !utf8.ValidString(destination) || len(destination) > 256 || expected == math.MaxUint64 || first == 0 || maxPayloadBytes < 1 || expires.IsZero() || len(application) > MaxApplicationBytes {
		return nil, errors.New("invalid prefix compaction")
	}
	base, err := p.readRoot(ctx, destination)
	if err != nil {
		return nil, err
	}
	if base.Head != expected {
		return nil, ErrConflict
	}
	if first > base.Graph.Count {
		return nil, errors.New("compaction cut outside live graph")
	}
	archive := selectGraph(base.Graph, base.Streams, PrefixArchiveStream)
	if archive.Count > math.MaxInt64-first {
		return nil, errors.New("archive population exhausted")
	}
	exists := false
	for _, stream := range base.Streams {
		exists = exists || stream.Name == PrefixArchiveStream
	}
	if !exists && len(base.Streams) == MaxStreams {
		return nil, errors.New("stream limit")
	}
	token, err := p.id()
	if err != nil {
		return nil, err
	}
	if !validID(token) {
		return nil, errors.New("invalid publication ID")
	}
	result := PreparedCompaction{destination: destination, expected: expected, first: first, expires: expires.UTC(), base: base, publication: Root{Schema: StreamsSchema, Token: token, Graph: retainedgraph.Empty(), Streams: copyStreams(base.Streams), Readers: copyReaders(base.Readers), Application: bytes.Clone(application)}}
	return &CompactionStage{protocol: p, prepared: result, archive: archive, maxPayloadBytes: maxPayloadBytes, cache: map[string]retainedgraph.Link{}, locations: map[string]map[string]bool{}}, nil
}

// knownNode is supplied only by a fresh authenticated WalkNodes callback.
// It replaces repeated membership lookup, never the fresh grant authority read.
// Payload callers still prove the grant's recorded origin location explicitly.
func (p Protocol) checkCompactionGrant(ctx context.Context, prepared PreparedCompaction, link retainedgraph.Link, stream string, knownNode *retainedgraph.Tree) error {
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
		if knownNode != nil {
			if knownNode.Link == link && location.Kind == "node" && location.First == knownNode.First && location.Height == knownNode.Height {
				return nil
			}
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
	operation, err := p.BeginCompactionCommit(ctx, prepared)
	if err != nil {
		return Root{}, err
	}
	root, done, err := operation.Advance(ctx, max(uint64(1), prepared.base.Graph.Count), ^uint64(0))
	if err != nil {
		return Root{}, err
	}
	if !done {
		return Root{}, errors.New("incomplete compaction verification")
	}
	return root, nil
}
