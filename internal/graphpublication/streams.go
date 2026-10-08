package graphpublication

import (
	"context"
	"errors"
	"sort"
	"time"

	"js-wf/internal/retainedgraph"
)

func validStream(name string) bool {
	if name == "" || len(name) > 32 {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

func validateStreams(streams []StreamGraph) error {
	if len(streams) > MaxStreams {
		return errors.New("stream limit")
	}
	for i, stream := range streams {
		if !validStream(stream.Name) || i > 0 && streams[i-1].Name >= stream.Name {
			return errors.New("invalid stream name or order")
		}
		if _, err := stream.Graph.Encode(); err != nil {
			return err
		}
	}
	return nil
}

func copyStreams(streams []StreamGraph) []StreamGraph {
	if len(streams) == 0 {
		return nil
	}
	result := append([]StreamGraph(nil), streams...)
	for i := range result {
		result[i].Graph = copyGraph(result[i].Graph)
	}
	return result
}

func selectGraph(graph retainedgraph.Root, streams []StreamGraph, name string) retainedgraph.Root {
	if name == "" {
		return graph
	}
	for _, stream := range streams {
		if stream.Name == name {
			return stream.Graph
		}
	}
	return retainedgraph.Empty()
}

func setStream(root *Root, name string, graph retainedgraph.Root) {
	if name == "" {
		root.Graph = graph
		return
	}
	root.Streams = copyStreams(root.Streams)
	for i := range root.Streams {
		if root.Streams[i].Name == name {
			root.Streams[i].Graph = graph
			return
		}
	}
	root.Streams = append(root.Streams, StreamGraph{Name: name, Graph: graph})
	sort.Slice(root.Streams, func(i, j int) bool { return root.Streams[i].Name < root.Streams[j].Name })
}

func clearLive(root *Root) {
	root.Graph = retainedgraph.Empty()
	root.Streams = copyStreams(root.Streams)
	for i := range root.Streams {
		root.Streams[i].Graph = retainedgraph.Empty()
	}
	root.Token = ""
}

// StreamSnapshot returns a copy of one independently indexed forest. It is
// only a snapshot; object reads still require a destination-bound reader pin.
func (r Root) StreamSnapshot(name string) (retainedgraph.Root, error) {
	if name != "" && !validStream(name) {
		return retainedgraph.Root{}, errors.New("invalid stream")
	}
	var err error
	r, err = normalizeRoot(r)
	if err != nil {
		return retainedgraph.Root{}, err
	}
	return copyGraph(selectGraph(r.Graph, r.Streams, name)), nil
}

// StreamSnapshot returns an independent copy of a captured named forest. Like
// Snapshot, this copy does not replace the lease checks for retained reads.
func (r Reader) StreamSnapshot(name string) (retainedgraph.Root, error) {
	if !validStream(name) || validateStreams(r.streams) != nil {
		return retainedgraph.Root{}, errors.New("invalid reader stream")
	}
	return copyGraph(selectGraph(r.graph, r.streams, name)), nil
}

// PrepareStreamAppendWithApplication stages an append in one named forest,
// preserving every other forest and publishing the application descriptor in
// the same original-head CAS. All indexes and ownership locations are local to
// that forest. Owned edges may only be reused within the selected forest; cross
// forest transfers currently require copying payload bytes. No lifecycle rule
// is inferred from application bytes: the caller validates its observed state
// before preparing, and an intervening purge/state CAS invalidates this plan.
func (p Protocol) PrepareStreamAppendWithApplication(ctx context.Context, destination string, expected uint64, stream string, data []byte, payloads [][]byte, owned []OwnedPayload, expires time.Time, application []byte) (Prepared, error) {
	if !validStream(stream) || len(application) > MaxApplicationBytes {
		return Prepared{}, errors.New("invalid stream append")
	}
	return p.prepareStreamAppend(ctx, destination, expected, stream, data, payloads, owned, expires, application, true)
}

// ReadRetainedStream validates the pin for the complete captured graph set
// before reading one stream-local index. A copied root cannot create this pin.
func (p Protocol) ReadRetainedStream(ctx context.Context, reader Reader, stream string, index uint64, now time.Time) (retainedgraph.Record, error) {
	if !validStream(stream) {
		return retainedgraph.Record{}, errors.New("invalid stream")
	}
	return p.readRetainedStream(ctx, reader, stream, index, now)
}
