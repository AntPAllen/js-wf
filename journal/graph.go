package journal

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
	"time"

	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
)

const graphCursorSchema = "js-wf-graph-journal-cursor-v1"
const graphEntrySchema = "js-wf-graph-journal-entry-v1"
const MaxGraphEntryBytes = 1 << 20
const DefaultGraphPayloadLimit = 64 << 20

// GraphConfig selects an isolated graph journal. Now must share the collector's
// clock. This does not import WF_JRN or enable runtime collection.
type GraphConfig struct {
	Protocol  graphpublication.Protocol
	Now       func() time.Time
	PinTTL    time.Duration
	IntentTTL time.Duration
	Encoding  Encoding
	// PayloadReadLimit bounds runtime input/result/signal reads. Direct GraphView
	// callers can supply their own explicit bound. Zero selects 64MiB.
	PayloadReadLimit int
}

type GraphStore struct{ cfg GraphConfig }

type graphCursor struct {
	Schema     string `json:"schema"`
	Invocation uint64 `json:"invocation"`
	Base       uint64 `json:"base"`
	Count      uint64 `json:"count"`
	Epoch      uint64 `json:"epoch"`
	Kind       Kind   `json:"kind"`
	Retired    bool   `json:"retired"`
}

type graphEntry struct {
	Schema      string `json:"schema"`
	Invocation  uint64 `json:"invocation"`
	Sequence    uint64 `json:"sequence"`
	EntrySHA256 string `json:"entry_sha256"`
}

func NewGraphStore(cfg GraphConfig) (*GraphStore, error) {
	if cfg.Protocol.Port == nil || validateEncoding(cfg.Encoding) != nil || cfg.PinTTL < 0 || cfg.IntentTTL < 0 || cfg.PayloadReadLimit < 0 || int64(cfg.PayloadReadLimit) == math.MaxInt64 {
		return nil, fmt.Errorf("invalid graph journal configuration")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.PinTTL == 0 {
		cfg.PinTTL = time.Minute
	}
	if cfg.IntentTTL == 0 {
		cfg.IntentTTL = time.Minute
	}
	if cfg.PayloadReadLimit == 0 {
		cfg.PayloadReadLimit = DefaultGraphPayloadLimit
	}
	return &GraphStore{cfg: cfg}, nil
}

// PayloadReadLimit is the explicit byte budget for configured runtime reads.
func (s *GraphStore) PayloadReadLimit() int { return s.cfg.PayloadReadLimit }

func graphDestination(typ, id string) (string, error) {
	if err := identity.Validate(typ, id); err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(identity.JournalSubject(typ, id)))
	return "journal/" + hex.EncodeToString(sum[:]), nil
}

// Strict canonical JSON rejects duplicate keys, aliases and unknown fields.
func graphDecode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return ErrGap
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrGap
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(data, canonical) {
		return ErrGap
	}
	return nil
}

func (s *GraphStore) observe(ctx context.Context, typ, id string) (string, graphpublication.Root, *graphCursor, error) {
	destination, err := graphDestination(typ, id)
	if err != nil {
		return "", graphpublication.Root{}, nil, err
	}
	if err = ctx.Err(); err != nil {
		return "", graphpublication.Root{}, nil, err
	}
	root, err := s.cfg.Protocol.Port.ReadRoot(ctx, destination)
	if err != nil {
		return destination, root, nil, fmt.Errorf("%w: %w", ErrUnknown, err)
	}
	if root.Graph.Validate() != nil {
		return destination, root, nil, ErrGap
	}
	if root.Head == 0 && root.Schema == graphpublication.Schema && root.Graph.Count == 0 && root.Token == "" && len(root.Readers) == 0 && len(root.Application) == 0 {
		return destination, root, nil, nil
	}
	if root.Schema != graphpublication.ApplicationSchema || root.Head == 0 || len(root.Application) == 0 || len(root.Application) > graphpublication.MaxApplicationBytes {
		return destination, root, nil, ErrGap
	}
	var c graphCursor
	if graphDecode(root.Application, &c) != nil || c.Schema != graphCursorSchema || c.Invocation == 0 || c.Count > MaxEntries || c.Base > math.MaxUint64-c.Count {
		return destination, root, nil, ErrGap
	}
	if c.Count == 0 {
		if c.Kind != "" || c.Epoch != 0 || c.Retired {
			return destination, root, nil, ErrGap
		}
	} else if _, ok := kinds[c.Kind]; !ok || c.Count > 1 && c.Kind == Started || c.Count == 1 && c.Kind != Started {
		return destination, root, nil, ErrGap
	}
	if c.Retired {
		if c.Kind != Completed && c.Kind != Failed || root.Graph.Count != 0 || root.Token != "" {
			return destination, root, nil, ErrGap
		}
	} else if root.Graph.Count != c.Count {
		return destination, root, nil, ErrGap
	}
	return destination, root, &c, nil
}

func graphMutationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, graphpublication.ErrConflict) || errors.Is(err, graphpublication.ErrRevoked) {
		return fmt.Errorf("%w: %w", ErrStale, err)
	}
	return fmt.Errorf("%w: %w", ErrUnknown, err)
}

// Begin binds the verified current invocation sequence. A new generation is
// admitted only after the old generation's terminal retirement. The caller must
// establish WF_INV identity and purge ordering; a sequence alone is no proof.
// Returned logical tail never resets across generations or reader metadata CAS.
func (s *GraphStore) Begin(ctx context.Context, typ, id string, invocation uint64) (uint64, error) {
	if invocation == 0 {
		return 0, ErrStale
	}
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return 0, err
	}
	next := graphCursor{Schema: graphCursorSchema, Invocation: invocation}
	if c != nil {
		if c.Invocation == invocation && !c.Retired {
			return c.Base + c.Count, nil
		}
		if invocation <= c.Invocation || !c.Retired {
			return 0, ErrStale
		}
		next.Base = c.Base + c.Count
	}
	data, _ := json.Marshal(next)
	_, err = s.cfg.Protocol.UpdateApplication(ctx, destination, root.Head, data)
	return next.Base, graphMutationError(err)
}

// Append owns encoded entry bytes as a graph payload, so large entries do not
// enlarge leaf metadata. Additional payloads and owned edges must include every
// external result/input reference the caller intends to retain. JSON pointers
// inside Entry.Payload do not automatically grant ownership.
func (s *GraphStore) Append(ctx context.Context, typ, id string, invocation uint64, e Entry, expected uint64, payloads [][]byte, owned []GraphOwnedPayload) (uint64, error) {
	if e.Index >= MaxEntries {
		return 0, ErrTooLong
	}
	if len(e.Payload) > MaxGraphEntryBytes || len(payloads)+len(owned) >= retainedgraph.MaxBlobReferences {
		return 0, ErrTooLong
	}
	if _, err := RecordToProto(Record{Entry: e}); err != nil {
		return 0, err
	}
	data, err := MarshalEntry(e, s.cfg.Encoding)
	if err != nil {
		return 0, err
	}
	if len(data) > MaxGraphEntryBytes {
		return 0, ErrTooLong
	}
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return 0, err
	}
	if c == nil || c.Invocation != invocation || c.Retired || c.Base+c.Count != expected || c.Count != e.Index || e.Epoch < c.Epoch || c.Count == 0 && e.Kind != Started || c.Count > 0 && (e.Kind == Started || c.Kind == Completed || c.Kind == Failed) {
		return 0, ErrStale
	}
	if expected == math.MaxUint64 {
		return 0, ErrTooLong
	}
	next := *c
	next.Count++
	next.Epoch = e.Epoch
	next.Kind = e.Kind
	app, _ := json.Marshal(next)
	sum := sha256.Sum256(data)
	envelope, _ := json.Marshal(graphEntry{Schema: graphEntrySchema, Invocation: invocation, Sequence: expected + 1, EntrySHA256: hex.EncodeToString(sum[:])})
	objects := make([][]byte, 0, len(payloads)+1)
	objects = append(objects, data)
	objects = append(objects, payloads...)
	prepared, err := s.cfg.Protocol.PrepareAppendWithApplication(ctx, destination, root.Head, envelope, objects, owned, s.cfg.Now().Add(s.cfg.IntentTTL), app)
	if err != nil {
		return 0, graphMutationError(err)
	}
	_, err = s.cfg.Protocol.Commit(ctx, prepared)
	if err != nil {
		return 0, graphMutationError(err)
	}
	return expected + 1, nil
}

// Retire atomically seals a terminal generation and removes only its live graph.
// Retained readers remain authoritative until release/expiry. The cursor and
// sequence high-water mark survive all physical object reclamation.
func (s *GraphStore) Retire(ctx context.Context, typ, id string, invocation, expected uint64) error {
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return err
	}
	if c == nil || c.Invocation != invocation || c.Base+c.Count != expected || c.Kind != Completed && c.Kind != Failed {
		return ErrStale
	}
	if c.Retired {
		return nil
	}
	next := *c
	next.Retired = true
	data, _ := json.Marshal(next)
	_, err = s.cfg.Protocol.RetireLiveWithApplication(ctx, destination, root.Head, data)
	return graphMutationError(err)
}

// GraphView retains an immutable generation snapshot. Keep this handle alive
// while consuming its payloads; receipts returned by Read alone are not pins.
// Views are not safe for concurrent mutation (Renew/Close).
type GraphView struct {
	store       *GraphStore
	destination string
	reader      graphpublication.Reader
	cursor      graphCursor
	expires     time.Time
	closed      bool
}

// GraphPayloadLink identifies exact immutable payload bytes in a retained view.
// Only a verified record supplies ownership; constructing a link does not.
type GraphPayloadLink = retainedgraph.Link

// GraphOwnedPayload reuses a payload edge owned by the current live graph.
// Append validates the index and exact receipt before accepting it.
type GraphOwnedPayload = graphpublication.OwnedPayload

type GraphRecord struct {
	Record
	Blobs []GraphPayloadLink
	// EntryBlob is the exact owned encoded entry edge. It can also be an
	// external payload when identical bytes were deduplicated in this leaf.
	EntryBlob GraphPayloadLink
}

// OpenTerminal pins a canonical terminal snapshot. A nil view means the
// matching invocation has not completed (including an uninitialized graph).
// A replaced or retired generation fails rather than reading another outcome.
func (s *GraphStore) OpenTerminal(ctx context.Context, typ, id string, invocation uint64) (*GraphView, error) {
	if invocation == 0 {
		return nil, ErrStale
	}
	return s.open(ctx, typ, id, invocation, true, true)
}

func (s *GraphStore) Open(ctx context.Context, typ, id string, invocation uint64) (*GraphView, error) {
	return s.open(ctx, typ, id, invocation, false, false)
}

// OpenExisting pins matching initialized history. A nil view means no graph
// has been initialized for this identity; retired or different generations are
// ErrStale, never absence. It does not initialize or import a journal.
func (s *GraphStore) OpenExisting(ctx context.Context, typ, id string, invocation uint64) (*GraphView, error) {
	return s.open(ctx, typ, id, invocation, false, true)
}

func (s *GraphStore) open(ctx context.Context, typ, id string, invocation uint64, terminalOnly, allowMissing bool) (*GraphView, error) {
	if invocation == 0 {
		return nil, ErrStale
	}
	// Each definite conflict requires a fresh generation observation: a
	// concurrent retirement/replacement must never attach an old cursor to a
	// newly acquired snapshot. Unknown acquisitions are not retried.
	for attempt := 0; attempt < 16; attempt++ {
		destination, root, c, err := s.observe(ctx, typ, id)
		if err != nil {
			return nil, err
		}
		if c == nil && allowMissing {
			return nil, nil
		}
		if c == nil || c.Invocation != invocation || c.Retired {
			return nil, ErrStale
		}
		if terminalOnly && c.Kind != Completed && c.Kind != Failed {
			return nil, nil
		}
		expires := s.cfg.Now().Add(s.cfg.PinTTL)
		reader, _, err := s.cfg.Protocol.AcquireReader(ctx, destination, root.Head, expires)
		if err == nil {
			return &GraphView{store: s, destination: destination, reader: reader, cursor: *c, expires: expires}, nil
		}
		if !errors.Is(err, graphpublication.ErrConflict) || ctx.Err() != nil {
			return nil, graphMutationError(err)
		}
	}
	return nil, graphMutationError(graphpublication.ErrConflict)
}
func (v *GraphView) Tail() uint64  { return v.cursor.Base + v.cursor.Count }
func (v *GraphView) Count() uint64 { return v.cursor.Count }
func (v *GraphView) alive() error {
	if v.closed || !v.store.cfg.Now().Before(v.expires) {
		return graphpublication.ErrRevoked
	}
	return nil
}

func (v *GraphView) Read(ctx context.Context, index uint64) (GraphRecord, error) {
	if err := v.alive(); err != nil {
		return GraphRecord{}, err
	}
	if index >= v.cursor.Count {
		return GraphRecord{}, ErrGap
	}
	raw, err := v.store.cfg.Protocol.ReadRetained(ctx, v.reader, index, v.store.cfg.Now())
	if err != nil {
		return GraphRecord{}, err
	}
	var envelope graphEntry
	if graphDecode(raw.Data, &envelope) != nil || envelope.Schema != graphEntrySchema || envelope.Invocation != v.cursor.Invocation || envelope.Sequence != v.cursor.Base+index+1 {
		return GraphRecord{}, ErrGap
	}
	result := GraphRecord{}
	found := false
	for _, link := range raw.Blobs {
		if link.Hash != envelope.EntrySHA256 {
			result.Blobs = append(result.Blobs, link)
			continue
		}
		if found {
			return GraphRecord{}, ErrGap
		}
		found = true
		result.EntryBlob = link
		data, e := v.store.cfg.Protocol.Port.Get(ctx, link, MaxGraphEntryBytes)
		if e != nil {
			return GraphRecord{}, e
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != envelope.EntrySHA256 || UnmarshalEntry(data, &result.Entry) != nil {
			return GraphRecord{}, ErrGap
		}
	}
	if !found || result.Index != index || result.Epoch > v.cursor.Epoch {
		return GraphRecord{}, ErrGap
	}
	if _, err = RecordToProto(Record{Entry: result.Entry}); err != nil {
		return GraphRecord{}, ErrGap
	}
	if index == 0 && result.Kind != Started || index > 0 && result.Kind == Started || index+1 < v.cursor.Count && (result.Kind == Completed || result.Kind == Failed) || index+1 == v.cursor.Count && (result.Epoch != v.cursor.Epoch || result.Kind != v.cursor.Kind) {
		return GraphRecord{}, ErrGap
	}
	result.Sequence = envelope.Sequence
	if err = v.alive(); err != nil {
		return GraphRecord{}, err
	}
	return result, nil
}

// Payload validates the exact edge in this view before reading its bytes.
func (v *GraphView) Payload(ctx context.Context, index uint64, link GraphPayloadLink, maxBytes int) ([]byte, error) {
	record, err := v.Read(ctx, index)
	if err != nil {
		return nil, err
	}
	found := record.EntryBlob == link
	for _, edge := range record.Blobs {
		if edge == link {
			found = true
			break
		}
	}
	if !found {
		return nil, ErrGap
	}
	data, err := v.store.cfg.Protocol.Port.Get(ctx, link, maxBytes)
	if err != nil {
		return nil, err
	}
	if err = v.alive(); err != nil {
		return nil, err
	}
	return data, nil
}

func (v *GraphView) Renew(ctx context.Context) error {
	for attempt := 0; attempt < 16; attempt++ {
		if err := v.alive(); err != nil {
			return err
		}
		root, err := v.store.cfg.Protocol.Port.ReadRoot(ctx, v.destination)
		if err != nil {
			return err
		}
		// Time may advance during the authority read; do not resurrect an expired
		// local lease even if its durable pin has not yet been pruned.
		if err = v.alive(); err != nil {
			return err
		}
		expires := v.store.cfg.Now().Add(v.store.cfg.PinTTL)
		_, err = v.store.cfg.Protocol.RenewReader(ctx, v.reader, root.Head, expires)
		if err == nil {
			v.expires = expires
			return nil
		}
		if !errors.Is(err, graphpublication.ErrConflict) || ctx.Err() != nil {
			return err
		}
	}
	return graphpublication.ErrConflict
}
func (v *GraphView) Close(ctx context.Context) error {
	if v.closed {
		return nil
	}
	// Other readers may advance this root while releasing their own pins.
	// Retry only a definite CAS rejection, with a fresh authority head. An
	// ambiguous release must retain its original-head readback semantics.
	for attempt := 0; attempt < 16; attempt++ {
		root, err := v.store.cfg.Protocol.Port.ReadRoot(ctx, v.destination)
		if err != nil {
			return err
		}
		_, err = v.store.cfg.Protocol.ReleaseReader(ctx, v.reader, root.Head)
		if err == nil {
			v.closed = true
			return nil
		}
		if !errors.Is(err, graphpublication.ErrConflict) || ctx.Err() != nil {
			return err
		}
	}
	return graphpublication.ErrConflict
}

// Read validates a complete retained snapshot and releases its pin afterward.
// For external payload consumption, hold Open's GraphView instead.
func (s *GraphStore) Read(ctx context.Context, typ, id string, invocation uint64) (records []Record, tail uint64, err error) {
	return s.read(ctx, typ, id, invocation, false)
}

// ReadExisting validates matching history without creating it. Only a witnessed
// uninitialized root returns empty history; stale/corrupt/unknown reads fail.
// External payload users must hold an OpenExisting view instead.
func (s *GraphStore) ReadExisting(ctx context.Context, typ, id string, invocation uint64) ([]Record, uint64, error) {
	return s.read(ctx, typ, id, invocation, true)
}

func (s *GraphStore) read(ctx context.Context, typ, id string, invocation uint64, allowMissing bool) (records []Record, tail uint64, err error) {
	view, err := s.open(ctx, typ, id, invocation, false, allowMissing)
	if err != nil || view == nil {
		return nil, 0, err
	}
	defer func() {
		closeErr := view.Close(ctx)
		if err == nil && closeErr != nil {
			records = nil
			tail = 0
			err = closeErr
		}
	}()
	for i := uint64(0); i < view.Count(); i++ {
		if !s.cfg.Now().Before(view.expires.Add(-s.cfg.PinTTL / 2)) {
			if err = view.Renew(ctx); err != nil {
				return nil, 0, err
			}
		}
		item, e := view.Read(ctx, i)
		if e != nil {
			return nil, 0, e
		}
		records, e = verifyNext(records, item.Record)
		if e != nil {
			return nil, 0, e
		}
	}
	return records, view.Tail(), nil
}
