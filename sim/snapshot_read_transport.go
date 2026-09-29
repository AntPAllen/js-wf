package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// SnapshotReadTransport holds the manifest and object reads needed by
// production journal.Store.Read after a compacted prefix is purged.
type SnapshotReadTransport struct {
	mu              sync.Mutex
	schedule        *Scheduler
	manifests       map[string][]byte
	objects         map[string][]byte
	manifestMiss    bool
	objectTransient bool
}

var _ journal.SnapshotReadPort = (*SnapshotReadTransport)(nil)

func NewSnapshotReadTransport(schedule *Scheduler) *SnapshotReadTransport {
	return &SnapshotReadTransport{schedule: schedule, manifests: make(map[string][]byte), objects: make(map[string][]byte)}
}

// SetSnapshot installs a retained snapshot fixture using production's
// content-addressed name and manifest schema.
func (m *SnapshotReadTransport) SetSnapshot(typ, id string, prefix []journal.Record) (journal.Snapshot, error) {
	if len(prefix) == 0 {
		return journal.Snapshot{}, fmt.Errorf("empty snapshot prefix")
	}
	data, err := json.Marshal(prefix)
	if err != nil {
		return journal.Snapshot{}, err
	}
	checksum := sha256.Sum256(data)
	keyDigest := sha256.Sum256([]byte(identity.Key(typ, id)))
	last := prefix[len(prefix)-1]
	name := fmt.Sprintf("snapshot-%s-%d-%s", hex.EncodeToString(keyDigest[:8]), last.Sequence, hex.EncodeToString(checksum[:8]))
	snap := journal.Snapshot{Version: 1, LastSeq: last.Sequence, LastIndex: last.Index, Epoch: last.Epoch, Object: name, SHA256: hex.EncodeToString(checksum[:])}
	manifest, err := json.Marshal(snap)
	if err != nil {
		return journal.Snapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[name] = append([]byte(nil), data...)
	m.manifests["snap."+identity.Key(typ, id)] = manifest
	m.schedule.RecordTransport(TransportEvent{Operation: "set_snapshot", Subject: identity.JournalSubject(typ, id), Sequence: last.Sequence, DataSHA256: digest(manifest), Outcome: "ok", AtMillis: m.schedule.NowMillis()})
	return snap, nil
}

func (m *SnapshotReadTransport) QueueManifestMiss() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.manifestMiss = true
}
func (m *SnapshotReadTransport) QueueObjectTransient() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objectTransient = true
}

func (m *SnapshotReadTransport) Wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay < 0 || delay%time.Millisecond != 0 {
		return fmt.Errorf("invalid snapshot wait %s", delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.schedule.AdvanceMillis(delay.Milliseconds()); err != nil {
		return err
	}
	m.schedule.RecordTransport(TransportEvent{Operation: "snapshot_wait", Outcome: fmt.Sprintf("%dms", delay.Milliseconds()), AtMillis: m.schedule.NowMillis()})
	return nil
}

func (m *SnapshotReadTransport) CorruptObject(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[name]; !ok {
		return jetstream.ErrObjectNotFound
	}
	m.objects[name] = []byte(`"corrupt"`)
	m.schedule.RecordTransport(TransportEvent{Operation: "corrupt_snapshot_object", Subject: name, Outcome: "ok", AtMillis: m.schedule.NowMillis()})
	return nil
}

func (m *SnapshotReadTransport) GetManifest(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "get_snapshot_manifest", Subject: key, AtMillis: m.schedule.NowMillis()}
	if m.manifestMiss {
		m.manifestMiss = false
		event.Outcome = "stale_missing"
		m.schedule.RecordTransport(event)
		return nil, jetstream.ErrKeyNotFound
	}
	data, ok := m.manifests[key]
	if !ok {
		event.Outcome = "not_found"
		m.schedule.RecordTransport(event)
		return nil, jetstream.ErrKeyNotFound
	}
	event.Outcome = "ok"
	event.DataSHA256 = digest(data)
	m.schedule.RecordTransport(event)
	return append([]byte(nil), data...), nil
}

func (m *SnapshotReadTransport) GetObject(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	event := TransportEvent{Operation: "get_snapshot_object", Subject: name, AtMillis: m.schedule.NowMillis()}
	if m.objectTransient {
		m.objectTransient = false
		event.Outcome = "transient"
		m.schedule.RecordTransport(event)
		return nil, ErrTransportLost
	}
	data, ok := m.objects[name]
	if !ok {
		event.Outcome = "not_found"
		m.schedule.RecordTransport(event)
		return nil, jetstream.ErrObjectNotFound
	}
	event.Outcome = "ok"
	event.DataSHA256 = digest(data)
	m.schedule.RecordTransport(event)
	return append([]byte(nil), data...), nil
}
