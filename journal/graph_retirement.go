package journal

import (
	"context"
	"encoding/json"
)

// GraphRetirement describes witnessed canonical cursor metadata, not terminal
// payload validation. An uninitialized identity has Invocation zero. Inspect
// does not acquire a reader, initialize history or prove application purge
// ordering. Callers validate terminal bytes before FencePurge.
type GraphRetirement struct {
	Invocation   uint64
	Tail         uint64
	Kind         Kind
	Purging      bool
	PendingStart bool `json:",omitempty"`
	Retired      bool
}

func (s *GraphStore) InspectRetirement(ctx context.Context, typ, id string) (GraphRetirement, error) {
	_, _, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return GraphRetirement{}, err
	}
	if c == nil {
		return GraphRetirement{}, nil
	}
	if s.cfg.CanonicalStarts && c.Invocation == 0 {
		return GraphRetirement{Invocation: c.PreviousInvocation, Tail: c.Base, Retired: c.PreviousInvocation != 0, PendingStart: true}, nil
	}
	return GraphRetirement{Invocation: c.Invocation, Tail: c.Base + c.Count, Kind: c.Kind, Purging: c.Purging, Retired: c.Retired}, nil
}

// FencePurge durably rejects new opens/appends for a verified terminal
// generation before its dependent stores are purged. Existing exact reader
// pins remain authoritative. Unknown replies are returned; a later operation
// can establish the committed fence through InspectRetirement. Old journal
// adapters reject this optional cursor field rather than ignoring its fence.
func (s *GraphStore) FencePurge(ctx context.Context, typ, id string, invocation, expected uint64) error {
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return err
	}
	if c == nil || c.Invocation != invocation || c.Base+c.Count != expected || c.Kind != Completed && c.Kind != Failed {
		return ErrStale
	}
	if c.Purging {
		return nil
	}
	if c.Retired {
		return ErrStale
	}
	next := *c
	next.Purging = true
	data, _ := json.Marshal(next)
	_, err = s.cfg.Protocol.UpdateApplication(ctx, destination, root.Head, data)
	return graphMutationError(err)
}
