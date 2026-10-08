package graphpublication

import (
	"context"
	"errors"
	"sort"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

func (p Protocol) protects(ctx context.Context, root Root, k string, f Fence, intent Intent) (bool, error) {
	if f.Phase != "ready" {
		return false, nil
	}
	link := retainedgraph.Link{Hash: k, Reference: blobpublication.Reference{Generation: f.Generation, Object: f.Object}}
	graphs := []Root{{Graph: root.Graph, Streams: root.Streams}}
	for _, reader := range root.Readers {
		graphs = append(graphs, Root{Graph: reader.Graph, Streams: reader.Streams})
	}
	for _, snapshot := range graphs {
		for _, location := range intent.Locations {
			graph := selectGraph(snapshot.Graph, snapshot.Streams, location.Stream)
			var present bool
			var err error
			if location.Kind == "node" {
				present, err = retainedgraph.ContainsNode(ctx, stageStore{protocol: p}, graph, retainedgraph.Tree{First: location.First, Height: location.Height, Link: link})
			} else {
				present, err = retainedgraph.ContainsBlob(ctx, stageStore{protocol: p}, graph, location.First, link)
			}
			if err != nil {
				return false, err
			}
			if present {
				return true, nil
			}
		}
	}
	return false, nil
}
func (p Protocol) sweepKey(ctx context.Context, k string, now time.Time) error {
readScope:
	for attempts := 0; attempts < 32; attempts++ {
		record, err := p.Port.ReadBlob(ctx, k)
		if err != nil {
			return err
		}
		if err = validateFence(k, record); err != nil {
			return err
		}
		f := record.Fence
		if record.Revision == 0 || f.Phase == "closed" {
			return nil
		}
		changed := false
		tokens := make([]string, 0, len(f.Intents))
		for token := range f.Intents {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)
		for _, token := range tokens {
			intent := f.Intents[token]
			root, err := p.readRoot(ctx, intent.Destination)
			if err != nil {
				return err
			}
			if root.Head < intent.Expected {
				return errors.New("graph destination head regressed")
			}
			root, err = p.pruneReaders(ctx, intent.Destination, root, now)
			if errors.Is(err, ErrConflict) {
				continue readScope
			}
			if err != nil {
				return err
			}
			protected, err := p.protects(ctx, root, f.Hash, f, intent)
			if err != nil {
				return err
			}
			if protected {
				continue
			}
			if root.Head == intent.Expected {
				if now.Before(intent.Expires) {
					continue
				}
				// Advance the original head while preserving any inherited live graph.
				// Unknown outcomes leave the grant untouched until a later witnessed read.
				_, err = p.casRoot(ctx, intent.Destination, root.Head, root)
				if errors.Is(err, ErrConflict) {
					changed = true
					break
				}
				if err != nil {
					return err
				}
			}
			delete(f.Intents, token)
			changed = true
		}
		if len(f.Intents) == 0 {
			f.Phase = "closed"
			f.Object = ""
			changed = true
		}
		if !changed {
			return nil
		}
		_, err = p.casBlob(ctx, k, record.Revision, f)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}
