package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"js-wf/identity"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrSnapshotTooShort = errors.New("journal has too few entries to snapshot")
var ErrSnapshotStale = errors.New("snapshot revision compare-and-swap lost")

type snapshotObjectGap struct{ cause error }

func (e *snapshotObjectGap) Error() string { return fmt.Sprintf("%v: %v", ErrGap, e.cause) }
func (e *snapshotObjectGap) Unwrap() error { return ErrGap }

type Snapshot struct {
	Version   int    `json:"version"`
	LastSeq   uint64 `json:"last_seq"`
	LastIndex uint64 `json:"last_index"`
	Epoch     uint64 `json:"epoch"`
	Object    string `json:"object"`
	SHA256    string `json:"sha256"`
}

// SnapshotReadPort contains the manifest and object reads needed to
// reconstruct a compacted journal. Missing data still fails closed.
type SnapshotReadPort interface {
	GetManifest(context.Context, string) ([]byte, error)
	GetObject(context.Context, string) ([]byte, error)
}

type SnapshotManifestValue struct {
	Value    []byte
	Revision uint64
}

// SnapshotWritePort extends compacted reads with the exact durable writes
// used by production snapshot creation and bounded prefix purge.
type SnapshotWritePort interface {
	SnapshotReadPort
	GetManifestRevision(context.Context, string) (SnapshotManifestValue, error)
	PutObject(context.Context, string, []byte) error
	CreateManifest(context.Context, string, []byte) error
	UpdateManifest(context.Context, string, []byte, uint64) error
	PurgeJournal(context.Context, string, uint64) error
	PurgeSignals(context.Context, string, uint64) error
}

type jetStreamSnapshotReadPort struct {
	js      jetstream.JetStream
	mu      sync.Mutex
	state   jetstream.KeyValue
	objects jetstream.ObjectStore
}

func NewSnapshotReadPort(js jetstream.JetStream) SnapshotReadPort {
	return &jetStreamSnapshotReadPort{js: js}
}

func NewSnapshotPort(js jetstream.JetStream) SnapshotWritePort {
	return &jetStreamSnapshotReadPort{js: js}
}

func (p *jetStreamSnapshotReadPort) GetManifest(ctx context.Context, key string) ([]byte, error) {
	value, err := p.GetManifestRevision(ctx, key)
	return value.Value, err
}

func (p *jetStreamSnapshotReadPort) GetManifestRevision(ctx context.Context, key string) (SnapshotManifestValue, error) {
	state, err := p.stateBucket(ctx)
	if err != nil {
		return SnapshotManifestValue{}, err
	}
	entry, err := state.Get(ctx, key)
	if err != nil {
		return SnapshotManifestValue{}, err
	}
	return SnapshotManifestValue{Value: entry.Value(), Revision: entry.Revision()}, nil
}

func (p *jetStreamSnapshotReadPort) PutObject(ctx context.Context, name string, data []byte) error {
	objects, err := p.objectStore(ctx)
	if err != nil {
		return err
	}
	_, err = objects.PutBytes(ctx, name, data)
	return err
}
func (p *jetStreamSnapshotReadPort) CreateManifest(ctx context.Context, key string, data []byte) error {
	state, err := p.stateBucket(ctx)
	if err != nil {
		return err
	}
	_, err = state.Create(ctx, key, data)
	return err
}
func (p *jetStreamSnapshotReadPort) UpdateManifest(ctx context.Context, key string, data []byte, revision uint64) error {
	state, err := p.stateBucket(ctx)
	if err != nil {
		return err
	}
	_, err = state.Update(ctx, key, data, revision)
	return err
}
func (p *jetStreamSnapshotReadPort) PurgeJournal(ctx context.Context, subject string, before uint64) error {
	stream, err := p.js.Stream(ctx, "WF_JRN")
	if err != nil {
		return err
	}
	return stream.Purge(ctx, jetstream.WithPurgeSubject(subject), jetstream.WithPurgeSequence(before))
}
func (p *jetStreamSnapshotReadPort) PurgeSignals(ctx context.Context, subject string, before uint64) error {
	stream, err := p.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return err
	}
	return stream.Purge(ctx, jetstream.WithPurgeSubject(subject), jetstream.WithPurgeSequence(before))
}

func (p *jetStreamSnapshotReadPort) GetObject(ctx context.Context, name string) ([]byte, error) {
	objects, err := p.objectStore(ctx)
	if err != nil {
		return nil, err
	}
	return objects.GetBytes(ctx, name)
}

func (p *jetStreamSnapshotReadPort) stateBucket(ctx context.Context) (jetstream.KeyValue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != nil {
		return p.state, nil
	}
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err == nil {
		p.state = state
	}
	return state, err
}

func (p *jetStreamSnapshotReadPort) objectStore(ctx context.Context) (jetstream.ObjectStore, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.objects != nil {
		return p.objects, nil
	}
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err == nil {
		p.objects = objects
	}
	return objects, err
}

func snapshotKey(typ, id string) string { return "snap." + identity.Key(typ, id) }

// MaybeSnapshot compacts after interval new entries have accumulated since the
// last snapshot. The newest keep entries always remain in the stream for CAS.
func (s *Store) MaybeSnapshot(ctx context.Context, typ, id string, interval, keep int) error {
	if interval < 1 || keep < 1 {
		return fmt.Errorf("interval and keep must be positive")
	}
	if err := identity.Validate(typ, id); err != nil {
		return err
	}
	if s.snapshotWritePort == nil {
		return fmt.Errorf("snapshot write transport unavailable")
	}
	var last AppendTail
	var err error
	if s.appendPort != nil {
		last, err = s.appendPort.Last(ctx, identity.JournalSubject(typ, id))
	} else {
		stream, streamErr := s.journalStream(ctx)
		if streamErr != nil {
			return streamErr
		}
		raw, lastErr := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
		err = lastErr
		if lastErr == nil {
			last = AppendTail{Sequence: raw.Sequence, Data: raw.Data}
		}
	}
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var entry Entry
	if err := json.Unmarshal(last.Data, &entry); err != nil {
		return ErrGap
	}
	var covered uint64
	var previous *Snapshot
	value, err := s.snapshotWritePort.GetManifestRevision(ctx, snapshotKey(typ, id))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if err == nil {
		previous = new(Snapshot)
		if json.Unmarshal(value.Value, previous) != nil || previous.Version != 1 {
			return ErrGap
		}
		covered = previous.LastIndex + 1
	}
	if entry.Index+1 < covered {
		return ErrGap
	}
	if entry.Index+1-covered < uint64(interval) || entry.Index+1 <= uint64(keep) {
		if previous != nil {
			return s.PurgeSnapshot(ctx, typ, id, *previous)
		}
		return nil
	}
	_, err = s.SnapshotPrefix(ctx, typ, id, keep)
	return err
}

// SnapshotPrefix writes a snapshot and then safely purges its covered prefix.
func (s *Store) SnapshotPrefix(ctx context.Context, typ, id string, keep int) (Snapshot, error) {
	snap, err := s.WriteSnapshot(ctx, typ, id, keep)
	if err != nil {
		return snap, err
	}
	return snap, s.PurgeSnapshot(ctx, typ, id, snap)
}

// WriteSnapshot stores all entries except the newest keep entries. It does
// not purge; a caller can recover a crash by calling PurgeSnapshot later.
func (s *Store) WriteSnapshot(ctx context.Context, typ, id string, keep int) (Snapshot, error) {
	var empty Snapshot
	if err := identity.Validate(typ, id); err != nil {
		return empty, err
	}
	if keep < 1 {
		return empty, fmt.Errorf("keep must be positive")
	}
	port := s.snapshotWritePort
	if port == nil {
		return empty, fmt.Errorf("snapshot write transport unavailable")
	}
	records, _, err := s.Read(ctx, typ, id)
	if err != nil {
		return empty, err
	}
	cut := len(records) - keep
	if cut < 1 {
		return empty, ErrSnapshotTooShort
	}
	prefix := records[:cut]
	last := prefix[len(prefix)-1]
	old, err := port.GetManifestRevision(ctx, snapshotKey(typ, id))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return empty, err
	}
	oldExists := err == nil
	if oldExists {
		var previous Snapshot
		if json.Unmarshal(old.Value, &previous) != nil {
			return empty, ErrGap
		}
		if previous.LastSeq >= last.Sequence {
			return previous, nil
		}
	}
	data, err := json.Marshal(prefix)
	if err != nil {
		return empty, err
	}
	digest := sha256.Sum256(data)
	keyDigest := sha256.Sum256([]byte(identity.Key(typ, id)))
	objectName := fmt.Sprintf("snapshot-%s-%d-%s", hex.EncodeToString(keyDigest[:8]), last.Sequence, hex.EncodeToString(digest[:8]))
	if err := port.PutObject(ctx, objectName, data); err != nil {
		return empty, err
	}
	get := port.GetObject
	if waiter, ok := port.(interface {
		Wait(context.Context, time.Duration) error
	}); ok {
		_, err = verifiedSnapshotObjectVirtual(ctx, get, waiter.Wait, objectName, hex.EncodeToString(digest[:]))
	} else {
		_, err = verifiedSnapshotObject(ctx, get, objectName, hex.EncodeToString(digest[:]))
	}
	if err != nil {
		return empty, err
	}
	snap := Snapshot{Version: 1, LastSeq: last.Sequence, LastIndex: last.Index, Epoch: last.Epoch, Object: objectName, SHA256: hex.EncodeToString(digest[:])}
	manifest, _ := json.Marshal(snap)
	if !oldExists {
		err = port.CreateManifest(ctx, snapshotKey(typ, id), manifest)
	} else {
		err = port.UpdateManifest(ctx, snapshotKey(typ, id), manifest, old.Revision)
	}
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return empty, ErrSnapshotStale
		}
		return empty, err
	}
	return snap, nil
}

// PurgeSnapshot removes only entries covered by an acknowledged snapshot.
// The fixed sequence bound preserves appends made after the snapshot read.
func (s *Store) PurgeSnapshot(ctx context.Context, typ, id string, snap Snapshot) error {
	if err := identity.Validate(typ, id); err != nil {
		return err
	}
	if snap.LastSeq == 0 {
		return ErrSnapshotStale
	}
	port := s.snapshotWritePort
	if port == nil {
		return fmt.Errorf("snapshot write transport unavailable")
	}
	confirmCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var prefix []Record
	confirmed := false
	for attempt := 0; attempt < 120; attempt++ {
		var current *Snapshot
		var err error
		prefix, current, err = s.loadSnapshot(confirmCtx, typ, id)
		if err != nil {
			return err
		}
		if current != nil && current.LastSeq >= snap.LastSeq {
			confirmed = true
			break
		}
		if waiter, ok := port.(interface {
			Wait(context.Context, time.Duration) error
		}); ok {
			if err := waiter.Wait(confirmCtx, 25*time.Millisecond); err != nil {
				return ErrSnapshotStale
			}
		} else {
			select {
			case <-confirmCtx.Done():
				return ErrSnapshotStale
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	if !confirmed {
		return ErrSnapshotStale
	}
	if err := port.PurgeJournal(ctx, identity.JournalSubject(typ, id), snap.LastSeq+1); err != nil {
		return err
	}
	var lastSignal uint64
	for _, record := range prefix {
		if record.Sequence > snap.LastSeq {
			break
		}
		if record.Kind != SignalConsumed {
			continue
		}
		var signal struct {
			Sequence uint64 `json:"sig_seq"`
		}
		if json.Unmarshal(record.Payload, &signal) != nil || signal.Sequence == 0 || signal.Sequence <= lastSignal {
			return ErrGap
		}
		lastSignal = signal.Sequence
	}
	if lastSignal == 0 {
		return nil
	}
	return port.PurgeSignals(ctx, "wf.sig."+typ+"."+id+".*", lastSignal+1)
}

func (s *Store) loadSnapshot(ctx context.Context, typ, id string) ([]Record, *Snapshot, error) {
	port := s.snapshotReadPort
	if port == nil {
		port = NewSnapshotReadPort(s.js)
	}
	wait := waitReadRequest
	if waiter, ok := port.(interface {
		Wait(context.Context, time.Duration) error
	}); ok {
		wait = waiter.Wait
	}
	key := snapshotKey(typ, id)
	value, err := boundedReadRequest(ctx, wait, "snapshot manifest "+key, func(request context.Context) ([]byte, error) { return port.GetManifest(request, key) })
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(value, &snap); err != nil {
		return nil, nil, ErrGap
	}
	if snap.Version != 1 || snap.Object == "" || snap.SHA256 == "" {
		return nil, nil, ErrGap
	}
	keyDigest := sha256.Sum256([]byte(identity.Key(typ, id)))
	if expectedPrefix := fmt.Sprintf("snapshot-%s-", hex.EncodeToString(keyDigest[:8])); len(snap.Object) < len(expectedPrefix) || snap.Object[:len(expectedPrefix)] != expectedPrefix {
		return nil, nil, ErrGap
	}
	var data []byte
	if waiter, ok := port.(interface {
		Wait(context.Context, time.Duration) error
	}); ok {
		data, err = verifiedSnapshotObjectVirtual(ctx, port.GetObject, waiter.Wait, snap.Object, snap.SHA256)
	} else {
		data, err = verifiedSnapshotObject(ctx, port.GetObject, snap.Object, snap.SHA256)
	}
	if err != nil {
		return nil, nil, err
	}
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, nil, ErrGap
	}
	if len(records) > MaxEntries {
		return nil, nil, ErrTooLong
	}
	if len(records) == 0 || records[len(records)-1].Sequence != snap.LastSeq || records[len(records)-1].Index != snap.LastIndex || records[len(records)-1].Epoch != snap.Epoch {
		return nil, nil, ErrGap
	}
	var checked []Record
	for _, r := range records {
		checked, err = verifyNext(checked, r)
		if err != nil {
			return nil, nil, err
		}
	}
	return checked, &snap, nil
}

// JetStream may briefly serve object metadata or chunks from a replica that
// has not caught up. Verify before acknowledging a manifest, and retry reads
// for a bounded period. Persistent absence or hash mismatch remains a gap.
func verifiedObject(ctx context.Context, objects jetstream.ObjectStore, name, wantHash string) ([]byte, error) {
	return verifiedSnapshotObject(ctx, func(ctx context.Context, name string) ([]byte, error) {
		return objects.GetBytes(ctx, name)
	}, name, wantHash)
}

func verifiedSnapshotObject(ctx context.Context, get func(context.Context, string) ([]byte, error), name, wantHash string) ([]byte, error) {
	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var lastErr error
	for {
		data, err := get(retryCtx, name)
		if err == nil {
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) == wantHash {
				return data, nil
			}
			lastErr = fmt.Errorf("snapshot SHA256 differs")
		} else if errors.Is(err, jetstream.ErrObjectNotFound) || errors.Is(err, jetstream.ErrDigestMismatch) {
			lastErr = err
		} else if retryCtx.Err() == nil {
			return nil, fmt.Errorf("%w: snapshot object: %v", ErrGap, err)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		select {
		case <-retryCtx.Done():
			return nil, &snapshotObjectGap{cause: fmt.Errorf("snapshot object %q: %v", name, lastErr)}
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// The simulation adapter advances its seeded clock rather than sleeping
// through the bounded replica-visibility retry window.
func verifiedSnapshotObjectVirtual(ctx context.Context, get func(context.Context, string) ([]byte, error), wait func(context.Context, time.Duration) error, name, wantHash string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 80; attempt++ {
		data, err := get(ctx, name)
		if err == nil {
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) == wantHash {
				return data, nil
			}
			lastErr = fmt.Errorf("snapshot SHA256 differs")
		} else {
			lastErr = err
		}
		if attempt < 79 {
			if err := wait(ctx, 25*time.Millisecond); err != nil {
				return nil, err
			}
		}
	}
	return nil, &snapshotObjectGap{cause: fmt.Errorf("snapshot object %q: %v", name, lastErr)}
}
