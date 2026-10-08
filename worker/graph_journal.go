package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
)

// WithGraphJournal selects experimental explicit-generation journal and payload
// ownership for deliveries. Every external terminal reader must also use graph
// results. Legacy start/state/signal publication, import, snapshots, continuation
// and retention migration remain separate; production online GC is not enabled.
// A child terminal must remain available until its parent publishes the owned
// SignalConsumed copy; production child retention coordination is still pending.
func WithGraphJournal(store *journal.GraphStore) Option {
	return func(w *Worker) error {
		if store == nil {
			return fmt.Errorf("nil graph journal")
		}
		w.graphJournal = store
		return nil
	}
}

func (w *Worker) validateGraphOptions() error {
	if w.graphJournal != nil && (w.legacyJournalOption || w.resultBlobPort != nil) {
		return fmt.Errorf("graph journal conflicts with legacy journal/result options")
	}
	if w.graphJournal != nil && len(w.continuations) != 0 {
		return fmt.Errorf("graph journal continuation/snapshot migration is incomplete")
	}
	return nil
}

type graphPayload struct {
	index uint64
	link  retainedgraph.Link
}
type graphDelivery struct {
	store          *journal.GraphStore
	typ, id        string
	invocation     uint64
	view           *journal.GraphView
	records        []journal.Record
	refs           map[string]graphPayload
	pending        map[string][]byte
	input          []byte
	invocationPort InvocationPort
}

func openGraphDelivery(ctx context.Context, s *journal.GraphStore, typ, id string, invocation uint64) (g *graphDelivery, err error) {
	if _, err = s.Begin(ctx, typ, id, invocation); err != nil {
		return nil, err
	}
	view, err := s.Open(ctx, typ, id, invocation)
	if err != nil {
		return nil, err
	}
	g = &graphDelivery{store: s, typ: typ, id: id, invocation: invocation, view: view, refs: map[string]graphPayload{}, pending: map[string][]byte{}}
	defer func() {
		if err != nil {
			_ = view.Close(ctx)
		}
	}()
	for i := uint64(0); i < view.Count(); i++ {
		record, e := view.Read(ctx, i)
		if e != nil {
			return nil, e
		}
		if len(g.records) > 0 {
			prev := g.records[len(g.records)-1]
			if record.Epoch < prev.Epoch || record.Sequence != prev.Sequence+1 {
				return nil, journal.ErrGap
			}
		}
		if e = g.register(record); e != nil {
			return nil, e
		}
		if e = g.validateChildSignal(ctx, record.Entry); e != nil {
			return nil, e
		}
		g.records = append(g.records, record.Record)
	}
	return g, nil
}

func graphHash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validGraphHash(hash string) bool {
	data, err := hex.DecodeString(hash)
	return err == nil && len(data) == sha256.Size && hex.EncodeToString(data) == hash
}

// Only runtime-declared references are ownership edges. User JSON strings are
// not interpreted as pointers. The failed journal-limit entry can retain a
// rejected signal declaration for offline replay.
func graphReferences(entry journal.Entry) (map[string]string, error) {
	refs := map[string]string{}
	add := func(name, hash string) error {
		if name == "" && hash == "" {
			return nil
		}
		if name == "" || !validGraphHash(hash) {
			return journal.ErrGap
		}
		if prev, ok := refs[name]; ok && prev != hash {
			return journal.ErrGap
		}
		refs[name] = hash
		return nil
	}
	switch entry.Kind {
	case journal.Started:
		var meta struct {
			InputSHA256 string `json:"input_sha256"`
		}
		if json.Unmarshal(entry.Payload, &meta) != nil || !validGraphHash(meta.InputSHA256) {
			return nil, journal.ErrGap
		}
		refs["input:"+meta.InputSHA256] = meta.InputSHA256
	case journal.StepCompleted, journal.Completed, journal.Failed:
		var meta struct {
			ResultRef  string `json:"result_ref"`
			ResultHash string `json:"result_hash"`
			LimitEntry *struct {
				Kind    string          `json:"kind"`
				Payload json.RawMessage `json:"payload"`
			} `json:"limit_entry"`
		}
		if json.Unmarshal(entry.Payload, &meta) != nil {
			return nil, journal.ErrGap
		}
		if err := add(meta.ResultRef, meta.ResultHash); err != nil {
			return nil, err
		}
		if meta.LimitEntry != nil && meta.LimitEntry.Kind == string(journal.SignalConsumed) {
			nested, err := graphReferences(journal.Entry{Kind: journal.SignalConsumed, Payload: meta.LimitEntry.Payload})
			if err != nil {
				return nil, err
			}
			for name, hash := range nested {
				if err = add(name, hash); err != nil {
					return nil, err
				}
			}
		}
	case journal.SignalConsumed:
		var meta signalRecord
		if json.Unmarshal(entry.Payload, &meta) != nil {
			return nil, journal.ErrGap
		}
		if meta.Ref != "" {
			if err := add(meta.Ref, meta.Hash); err != nil {
				return nil, err
			}
		}
		if meta.Child != nil {
			if err := meta.Child.validate(); err != nil {
				return nil, err
			}
			if err := add(meta.Child.Ref, meta.Child.Hash); err != nil {
				return nil, err
			}
		}
	}
	return refs, nil
}

func (g *graphDelivery) register(record journal.GraphRecord) error {
	refs, err := graphReferences(record.Entry)
	if err != nil {
		return err
	}
	for name, hash := range refs {
		found := false
		for _, link := range append(record.Blobs, record.EntryBlob) {
			if link.Hash == hash {
				g.refs[name] = graphPayload{index: record.Index, link: link}
				found = true
				break
			}
		}
		if !found {
			return journal.ErrGap
		}
	}
	return nil
}

func (g *graphDelivery) refresh(ctx context.Context) error {
	fresh, err := g.store.Open(ctx, g.typ, g.id, g.invocation)
	if err != nil {
		return err
	}
	old := g.view
	g.view = fresh
	if err = old.Close(ctx); err != nil && !errors.Is(err, graphpublication.ErrRevoked) {
		return err
	}
	return nil
}
func (g *graphDelivery) close(ctx context.Context) error { return g.view.Close(ctx) }
func (g *graphDelivery) PutBytes(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	hash := graphHash(data)
	if name != "step-result-"+hash && name != "terminal-result-"+hash {
		return journal.ErrGap
	}
	g.pending[name] = bytes.Clone(data)
	return nil
}
func (g *graphDelivery) GetBytes(ctx context.Context, name string) ([]byte, error) {
	if data, ok := g.pending[name]; ok {
		return bytes.Clone(data), nil
	}
	ref, ok := g.refs[name]
	if !ok {
		return nil, fmt.Errorf("%w: unowned graph result %s", ErrResultBlobUnavailable, name)
	}
	data, err := g.view.Payload(ctx, ref.index, ref.link, g.store.PayloadReadLimit())
	if errors.Is(err, graphpublication.ErrRevoked) {
		if err = g.refresh(ctx); err == nil {
			data, err = g.view.Payload(ctx, ref.index, ref.link, g.store.PayloadReadLimit())
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrResultBlobUnavailable, err)
	}
	return data, nil
}

func (g *graphDelivery) append(ctx context.Context, entry journal.Entry, tail uint64) (uint64, error) {
	cleanup, err := g.prepareChildSignal(ctx, &entry)
	if err != nil {
		return 0, err
	}
	if cleanup != nil {
		defer cleanup()
	}
	refs, err := graphReferences(entry)
	if err != nil {
		return 0, err
	}
	var payloads [][]byte
	var owned []graphpublication.OwnedPayload
	// Stable ordering keeps seeded transport traces reproducible.
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		hash := refs[name]
		if ref, ok := g.refs[name]; ok {
			if ref.link.Hash != hash {
				return 0, journal.ErrGap
			}
			owned = append(owned, graphpublication.OwnedPayload{Index: ref.index, Link: ref.link})
			continue
		}
		data, ok := g.pending[name]
		if entry.Kind == journal.Started {
			data = g.input
			ok = true
		}
		if !ok || graphHash(data) != hash {
			return 0, fmt.Errorf("%w: unstaged graph payload", ErrResultBlobUnavailable)
		}
		payloads = append(payloads, data)
	}
	seq, err := g.store.Append(ctx, g.typ, g.id, g.invocation, entry, tail, payloads, owned)
	if err != nil {
		return 0, err
	}
	if err = g.refresh(ctx); err != nil {
		return 0, fmt.Errorf("%w: committed append reader refresh: %w", journal.ErrUnknown, err)
	}
	record, err := g.view.Read(ctx, entry.Index)
	if err != nil {
		return 0, fmt.Errorf("%w: committed graph entry: %w", journal.ErrUnknown, err)
	}
	if err = g.register(record); err != nil {
		return 0, err
	}
	for _, name := range names {
		delete(g.pending, name)
	}
	g.records = append(g.records, record.Record)
	g.input = nil
	return seq, nil
}

type graphSignalDrain struct {
	SignalDrainPort
	graph *graphDelivery
}

func (p graphSignalDrain) SignalBlob(ctx context.Context, name string) ([]byte, error) {
	if _, ok := p.graph.refs[name]; ok {
		return p.graph.GetBytes(ctx, name)
	}
	data, err := p.SignalDrainPort.SignalBlob(ctx, name)
	if err != nil {
		return nil, err
	}
	p.graph.pending[name] = bytes.Clone(data)
	return data, nil
}
