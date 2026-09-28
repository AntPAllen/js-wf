package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/identity"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrSnapshotTooShort = errors.New("journal has too few entries to snapshot")
var ErrSnapshotStale = errors.New("snapshot revision compare-and-swap lost")

type Snapshot struct {
	Version   int    `json:"version"`
	LastSeq   uint64 `json:"last_seq"`
	LastIndex uint64 `json:"last_index"`
	Epoch     uint64 `json:"epoch"`
	Object    string `json:"object"`
	SHA256    string `json:"sha256"`
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
	stream, err := s.journalStream(ctx)
	if err != nil {
		return err
	}
	last, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
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
	state, err := s.stateKV(ctx)
	if err != nil {
		return err
	}
	var covered uint64
	var previous *Snapshot
	value, err := state.Get(ctx, snapshotKey(typ, id))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if err == nil {
		previous = new(Snapshot)
		if json.Unmarshal(value.Value(), previous) != nil || previous.Version != 1 {
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
	state, err := s.stateKV(ctx)
	if err != nil {
		return empty, err
	}
	old, err := state.Get(ctx, snapshotKey(typ, id))
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return empty, err
	}
	if err == nil {
		var previous Snapshot
		if json.Unmarshal(old.Value(), &previous) != nil {
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
	objects, err := s.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return empty, err
	}
	if _, err := objects.PutBytes(ctx, objectName, data); err != nil {
		return empty, err
	}
	if _, err := verifiedObject(ctx, objects, objectName, hex.EncodeToString(digest[:])); err != nil {
		return empty, err
	}
	snap := Snapshot{Version: 1, LastSeq: last.Sequence, LastIndex: last.Index, Epoch: last.Epoch, Object: objectName, SHA256: hex.EncodeToString(digest[:])}
	manifest, _ := json.Marshal(snap)
	if old == nil {
		_, err = state.Create(ctx, snapshotKey(typ, id), manifest)
	} else {
		_, err = state.Update(ctx, snapshotKey(typ, id), manifest, old.Revision())
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
	confirmCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var prefix []Record
	for {
		var current *Snapshot
		var err error
		prefix, current, err = s.loadSnapshot(confirmCtx, typ, id)
		if err != nil {
			return err
		}
		if current != nil && current.LastSeq >= snap.LastSeq {
			break
		}
		select {
		case <-confirmCtx.Done():
			return ErrSnapshotStale
		case <-time.After(25 * time.Millisecond):
		}
	}
	stream, err := s.journalStream(ctx)
	if err != nil {
		return err
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSubject(identity.JournalSubject(typ, id)), jetstream.WithPurgeSequence(snap.LastSeq+1)); err != nil {
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
	signals, err := s.js.Stream(ctx, "WF_SIG")
	if err != nil {
		return err
	}
	return signals.Purge(ctx, jetstream.WithPurgeSubject("wf.sig."+typ+"."+id+".*"), jetstream.WithPurgeSequence(lastSignal+1))
}

func (s *Store) loadSnapshot(ctx context.Context, typ, id string) ([]Record, *Snapshot, error) {
	state, err := s.stateKV(ctx)
	if err != nil {
		return nil, nil, err
	}
	value, err := state.Get(ctx, snapshotKey(typ, id))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(value.Value(), &snap); err != nil {
		return nil, nil, ErrGap
	}
	if snap.Version != 1 || snap.Object == "" || snap.SHA256 == "" {
		return nil, nil, ErrGap
	}
	keyDigest := sha256.Sum256([]byte(identity.Key(typ, id)))
	if expectedPrefix := fmt.Sprintf("snapshot-%s-", hex.EncodeToString(keyDigest[:8])); len(snap.Object) < len(expectedPrefix) || snap.Object[:len(expectedPrefix)] != expectedPrefix {
		return nil, nil, ErrGap
	}
	objects, err := s.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return nil, nil, err
	}
	data, err := verifiedObject(ctx, objects, snap.Object, snap.SHA256)
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
	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var lastErr error
	for {
		data, err := objects.GetBytes(retryCtx, name)
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
			return nil, fmt.Errorf("%w: snapshot object %q: %v", ErrGap, name, lastErr)
		case <-time.After(25 * time.Millisecond):
		}
	}
}
