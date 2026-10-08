package graphpublication

import (
	"context"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

// verifyOwned checks both the selected source edge and its original grant.
// Append preserves that original leaf and its ancestors. If retirement or GC
// changes the destination during this check, the eventual original-head CAS
// fails. Receipt reuse therefore does not create an independent liveness pin.
func (p Protocol) verifyOwned(ctx context.Context, destination string, base Root, payload OwnedPayload) error {
	present, err := retainedgraph.ContainsBlob(ctx, stageStore{protocol: p}, selectGraph(base.Graph, base.Streams, payload.Stream), payload.Index, payload.Link)
	if err != nil {
		return err
	}
	if !present {
		return ErrRevoked
	}
	scope, owner, err := objectAuthority(blobpublication.Object{Key: payload.Link.Hash, Reference: payload.Link.Reference})
	if err != nil {
		return err
	}
	record, err := p.Port.ReadBlob(ctx, scope)
	if err != nil {
		return err
	}
	if err = validateFence(scope, record); err != nil {
		return err
	}
	f := record.Fence
	origin, exists := f.Intents[owner]
	if f.Phase != "ready" || f.Generation != payload.Link.Reference.Generation || f.Object != payload.Link.Reference.Object || !exists || origin.Destination != destination || origin.Expected >= base.Head {
		return ErrRevoked
	}
	for _, location := range origin.Locations {
		if location.Kind != "payload" || location.Stream != payload.Stream {
			continue
		}
		if location.First == payload.Index {
			return nil
		}
		present, err = retainedgraph.ContainsBlob(ctx, stageStore{protocol: p}, selectGraph(base.Graph, base.Streams, payload.Stream), location.First, payload.Link)
		if err != nil {
			return err
		}
		if present {
			return nil
		}
	}
	return ErrRevoked
}
