package journal

import (
	"context"
	"encoding/json"
	"fmt"

	"js-wf/internal/blobpublication"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/internal/retainedindex"
)

// Observed metadata is an admission hint, never a payload ownership grant.
// Every mutating caller still publishes against the original witnessed head.
type observedGraphRead struct{ port graphpublication.Port }

func (s observedGraphRead) Put(context.Context, string, []byte) (blobpublication.Reference, error) {
	return blobpublication.Reference{}, ErrGap
}
func (s observedGraphRead) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	return s.port.Get(ctx, link, limit)
}
func (s *GraphStore) observedSignalRecord(ctx context.Context, root graphpublication.Root, forest string, index uint64) (retainedgraph.Record, error) {
	for _, stream := range root.Streams {
		if stream.Name == forest {
			return retainedgraph.Read(ctx, observedGraphRead{s.cfg.Protocol.Port}, stream.Graph, index)
		}
	}
	return retainedgraph.Record{}, ErrGap
}
func (s *GraphStore) observedBinding(ctx context.Context, root graphpublication.Root, c graphCursor, index uint64) (GraphSignalBinding, error) {
	raw, err := s.observedSignalRecord(ctx, root, graphSignalQueueForest, index)
	if err != nil {
		return GraphSignalBinding{}, err
	}
	var record graphSignalQueueRecord
	b := &record.Binding
	if graphDecode(raw.Data, &record) != nil || b.Schema != graphSignalBindingSchema || b.Index != index || b.Input.Index >= c.SignalInputs || !validSignalInput(b.Input, c, b.Input.Index, s.cfg.PayloadReadLimit) || b.Sequence == 0 || b.Sequence > c.SignalSource || len(record.IndexPacket) > retainedindex.MaxPacketBytes || len(raw.Blobs) != 1 || raw.Blobs[0].Hash != b.Input.InputSHA256 {
		return GraphSignalBinding{}, ErrGap
	}
	return *b, nil
}
func (s *GraphStore) validateSignalConsumption(ctx context.Context, root graphpublication.Root, c graphCursor, e Entry, payloads [][]byte, owned []graphpublication.OwnedPayload) error {
	var event struct {
		Sequence  uint64 `json:"sig_seq"`
		Name      string `json:"name"`
		Hash      string `json:"hash"`
		Ref       string `json:"ref"`
		Canonical *struct {
			Index uint64 `json:"index"`
			Token string `json:"token"`
		} `json:"canonical_signal"`
	}
	if json.Unmarshal(e.Payload, &event) != nil || event.Canonical == nil || event.Canonical.Index != c.SignalConsumed || c.SignalConsumed >= c.SignalBindings {
		return ErrGap
	}
	b, err := s.observedBinding(ctx, root, c, c.SignalConsumed)
	if err != nil {
		return fmt.Errorf("%w: consumption metadata: %w", ErrUnknown, err)
	}
	if event.Canonical.Token != b.Input.Token || event.Sequence != b.Sequence || event.Name != b.Input.Request.Name || event.Hash != b.Input.InputSHA256 || event.Ref != "graph-signal-"+event.Hash {
		return ErrGap
	}
	// Metadata progress cannot outrun actual journal ownership of the input.
	for _, body := range payloads {
		if len(body) == b.Input.InputSize && startHash(body) == event.Hash {
			return nil
		}
	}
	for _, receipt := range owned {
		if receipt.Link.Hash == event.Hash {
			return nil
		}
	} // Append validates exact ancestry/ownership.
	return ErrGap
}

// InspectSignalRepair returns one current immutable reservation descriptor and
// metadata-only bound presence. No reader is acquired and no body is returned.
// A matching body and publication still require the retained recovery API.
func (s *GraphStore) InspectSignalRepair(ctx context.Context, typ, id string, invocation, index uint64) (input GraphSignalInput, binding *GraphSignalBinding, err error) {
	if !s.cfg.CanonicalSignals {
		return input, nil, ErrGap
	}
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return input, nil, err
	}
	if c == nil || c.Invocation != invocation || c.Retired || c.Purging {
		return input, nil, ErrStale
	}
	raw, err := s.observedSignalRecord(ctx, root, graphSignalInputForest, index)
	if err != nil {
		return input, nil, err
	}
	var record graphSignalInputRecord
	if graphDecode(raw.Data, &record) != nil || !validSignalInput(record.Input, *c, index, s.cfg.PayloadReadLimit) || len(record.IndexPacket) > retainedindex.MaxPacketBytes || len(raw.Blobs) != 1 || raw.Blobs[0].Hash != record.Input.InputSHA256 {
		return input, nil, ErrGap
	}
	input = record.Input
	reader := observedQueueIndex{s, root, *c}
	bound, found, err := retainedindex.Lookup(ctx, reader, c.SignalBindings, signalIndexKey(input.Request))
	if err != nil {
		return input, nil, err
	}
	if found {
		b, e := s.observedBinding(ctx, root, *c, bound)
		if e != nil {
			return input, nil, e
		}
		if b.Input != input {
			return input, nil, ErrGap
		}
		binding = &b
	}
	fresh, err := s.cfg.Protocol.Port.ReadRoot(ctx, destination)
	if err != nil {
		return input, nil, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if fresh.Head != root.Head {
		return input, nil, ErrStale
	}
	return input, binding, nil
}

type observedQueueIndex struct {
	store  *GraphStore
	root   graphpublication.Root
	cursor graphCursor
}

func (r observedQueueIndex) ReadIndex(ctx context.Context, index uint64) ([]byte, error) {
	if _, err := r.store.observedBinding(ctx, r.root, r.cursor, index); err != nil {
		return nil, err
	}
	raw, err := r.store.observedSignalRecord(ctx, r.root, graphSignalQueueForest, index)
	if err != nil {
		return nil, err
	}
	var record graphSignalQueueRecord
	if graphDecode(raw.Data, &record) != nil {
		return nil, ErrGap
	}
	return record.IndexPacket, nil
}

// AdvanceSignalRepair saves a bounded per-generation scan position. Unknown
// writes remain unknown; callers reread metadata rather than skip candidates.
func (s *GraphStore) AdvanceSignalRepair(ctx context.Context, typ, id string, invocation uint64, token string, from, to uint64) error {
	if !s.cfg.CanonicalSignals {
		return ErrGap
	}
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return err
	}
	if c == nil || c.Invocation != invocation || c.Start.Token != token || c.Retired || c.Purging || c.SignalRepair != from || to > c.SignalInputs {
		return ErrStale
	}
	if to == c.SignalInputs {
		to = 0
	}
	if from == to {
		return nil
	}
	next := *c
	next.SignalRepair = to
	app, _ := json.Marshal(next)
	_, err = s.cfg.Protocol.UpdateApplication(ctx, destination, root.Head, app)
	return graphMutationError(err)
}

// InspectReadySignal supplies one unconsumed binding as a wakeup hint. This
// reads only bounded metadata; execution still acquires owned queue input.
func (s *GraphStore) InspectReadySignal(ctx context.Context, typ, id string, invocation uint64) (*GraphSignalBinding, error) {
	if !s.cfg.CanonicalSignals {
		return nil, ErrGap
	}
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return nil, err
	}
	if c == nil || c.Invocation != invocation || c.Purging || c.Retired {
		return nil, ErrStale
	}
	if c.Kind == Completed || c.Kind == Failed || c.SignalConsumed == c.SignalBindings {
		return nil, nil
	}
	binding, err := s.observedBinding(ctx, root, *c, c.SignalConsumed)
	if err != nil {
		return nil, err
	}
	fresh, err := s.cfg.Protocol.Port.ReadRoot(ctx, destination)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if fresh.Head != root.Head {
		return nil, ErrStale
	}
	return &binding, nil
}
