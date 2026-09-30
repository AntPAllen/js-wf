package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SnapshotReadTransport holds the manifest and object reads needed by
// production journal.Store.Read after a compacted prefix is purged.
type SnapshotReadTransport struct {
	mu                 sync.Mutex
	schedule           *Scheduler
	manifests          map[string][]byte
	objects            map[string][]byte
	manifestRevisions  map[string]uint64
	revision           uint64
	live               *JournalTransport
	signals            *SignalTransport
	writeFaults        []SnapshotFault
	manifestMiss       bool
	manifestReadFaults []string
	objectTransient    bool
}

var _ journal.SnapshotReadPort = (*SnapshotReadTransport)(nil)
var _ journal.SnapshotWritePort = (*SnapshotReadTransport)(nil)

type SnapshotFault struct {
	Operation string
	Kind      AppendFault
}

func NewSnapshotReadTransport(schedule *Scheduler) *SnapshotReadTransport {
	return &SnapshotReadTransport{schedule: schedule, manifests: make(map[string][]byte), objects: make(map[string][]byte), manifestRevisions: make(map[string]uint64)}
}

func (m *SnapshotReadTransport) BindJournal(live *JournalTransport)   { m.live = live }
func (m *SnapshotReadTransport) BindSignals(signals *SignalTransport) { m.signals = signals }

func (m *SnapshotReadTransport) QueueWriteFault(f SnapshotFault) error {
	switch f.Operation {
	case "put_object", "create_manifest", "update_manifest", "purge_journal", "purge_signals":
	default:
		return fmt.Errorf("invalid snapshot fault operation %q", f.Operation)
	}
	if f.Kind != DropBeforeCommit && f.Kind != LoseAckAfterCommit {
		return fmt.Errorf("invalid snapshot fault kind %q", f.Kind)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeFaults = append(m.writeFaults, f)
	return nil
}

func (m *SnapshotReadTransport) takeWriteFault(operation string) AppendFault {
	for i, f := range m.writeFaults {
		if f.Operation == operation {
			m.writeFaults = append(m.writeFaults[:i], m.writeFaults[i+1:]...)
			return f.Kind
		}
	}
	return ""
}

func (m *SnapshotReadTransport) writeEvent(operation, key string, seq uint64, data []byte, outcome string) {
	m.schedule.RecordTransport(TransportEvent{Operation: operation, Subject: key, Sequence: seq, DataSHA256: digest(data), Outcome: outcome, AtMillis: m.schedule.NowMillis()})
}

func (m *SnapshotReadTransport) GetManifestRevision(ctx context.Context, key string) (journal.SnapshotManifestValue, error) {
	if err := ctx.Err(); err != nil {
		return journal.SnapshotManifestValue{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, revision, err := m.getManifestLocked(key)
	return journal.SnapshotManifestValue{Value: data, Revision: revision}, err
}

func (m *SnapshotReadTransport) PutObject(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fault := m.takeWriteFault("put_object")
	if fault == DropBeforeCommit {
		m.writeEvent("put_snapshot_object", name, 0, data, string(fault))
		return ErrTransportLost
	}
	m.objects[name] = append([]byte(nil), data...)
	if fault == LoseAckAfterCommit {
		m.writeEvent("put_snapshot_object", name, 0, data, string(fault))
		return ErrTransportLost
	}
	m.writeEvent("put_snapshot_object", name, 0, data, "ok")
	return nil
}

func (m *SnapshotReadTransport) CreateManifest(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fault := m.takeWriteFault("create_manifest")
	if fault == DropBeforeCommit {
		m.writeEvent("create_snapshot_manifest", key, 0, data, string(fault))
		return ErrTransportLost
	}
	if _, exists := m.manifests[key]; exists {
		m.writeEvent("create_snapshot_manifest", key, 0, data, "key_exists")
		return jetstream.ErrKeyExists
	}
	m.revision++
	m.manifests[key] = append([]byte(nil), data...)
	m.manifestRevisions[key] = m.revision
	if fault == LoseAckAfterCommit {
		m.writeEvent("create_snapshot_manifest", key, m.revision, data, string(fault))
		return ErrTransportLost
	}
	m.writeEvent("create_snapshot_manifest", key, m.revision, data, "ok")
	return nil
}

func (m *SnapshotReadTransport) UpdateManifest(ctx context.Context, key string, data []byte, revision uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fault := m.takeWriteFault("update_manifest")
	if fault == DropBeforeCommit {
		m.writeEvent("update_snapshot_manifest", key, revision, data, string(fault))
		return ErrTransportLost
	}
	if m.manifestRevisions[key] != revision || revision == 0 {
		m.writeEvent("update_snapshot_manifest", key, revision, data, "revision_mismatch")
		return jetstream.ErrKeyRevisionMismatch
	}
	m.revision++
	m.manifests[key] = append([]byte(nil), data...)
	m.manifestRevisions[key] = m.revision
	if fault == LoseAckAfterCommit {
		m.writeEvent("update_snapshot_manifest", key, m.revision, data, string(fault))
		return ErrTransportLost
	}
	m.writeEvent("update_snapshot_manifest", key, m.revision, data, "ok")
	return nil
}

func (m *SnapshotReadTransport) PurgeJournal(ctx context.Context, subject string, before uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	fault := m.takeWriteFault("purge_journal")
	m.mu.Unlock()
	if fault == DropBeforeCommit {
		m.mu.Lock()
		m.writeEvent("purge_snapshot_journal", subject, before, nil, string(fault))
		m.mu.Unlock()
		return ErrTransportLost
	}
	if m.live == nil {
		return fmt.Errorf("snapshot journal transport unbound")
	}
	m.live.PurgeBefore(subject, before)
	m.mu.Lock()
	defer m.mu.Unlock()
	outcome := "ok"
	if fault == LoseAckAfterCommit {
		outcome = string(fault)
	}
	m.writeEvent("purge_snapshot_journal", subject, before, nil, outcome)
	if fault == LoseAckAfterCommit {
		return ErrTransportLost
	}
	return nil
}

func (m *SnapshotReadTransport) PurgeSignals(ctx context.Context, subject string, before uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	fault := m.takeWriteFault("purge_signals")
	m.mu.Unlock()
	if fault == DropBeforeCommit {
		m.mu.Lock()
		m.writeEvent("purge_snapshot_signals", subject, before, nil, string(fault))
		m.mu.Unlock()
		return ErrTransportLost
	}
	if m.signals == nil {
		return fmt.Errorf("snapshot signal transport unbound")
	}
	m.signals.PurgeSignalPrefix(strings.TrimSuffix(subject, "*"), before)
	m.mu.Lock()
	defer m.mu.Unlock()
	outcome := "ok"
	if fault == LoseAckAfterCommit {
		outcome = string(fault)
	}
	m.writeEvent("purge_snapshot_signals", subject, before, nil, outcome)
	if fault == LoseAckAfterCommit {
		return ErrTransportLost
	}
	return nil
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
	m.revision++
	m.manifestRevisions["snap."+identity.Key(typ, id)] = m.revision
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
	if len(m.manifestReadFaults) > 0 {
		kind := m.manifestReadFaults[0]
		m.manifestReadFaults = m.manifestReadFaults[1:]
		if kind == "timeout" {
			if err := m.schedule.AdvanceMillis(2000); err != nil {
				return nil, err
			}
		}
		m.writeEvent("get_snapshot_manifest", key, 0, nil, kind)
		switch kind {
		case "timeout":
			return nil, context.DeadlineExceeded
		case "no_responders":
			return nil, nats.ErrNoResponders
		case "unavailable":
			return nil, &jetstream.APIError{ErrorCode: 10008}
		}
	}
	data, _, err := m.getManifestLocked(key)
	return data, err
}

func (m *SnapshotReadTransport) getManifestLocked(key string) ([]byte, uint64, error) {
	event := TransportEvent{Operation: "get_snapshot_manifest", Subject: key, AtMillis: m.schedule.NowMillis()}
	if m.manifestMiss {
		m.manifestMiss = false
		event.Outcome = "stale_missing"
		m.schedule.RecordTransport(event)
		return nil, 0, jetstream.ErrKeyNotFound
	}
	data, ok := m.manifests[key]
	if !ok {
		event.Outcome = "not_found"
		m.schedule.RecordTransport(event)
		return nil, 0, jetstream.ErrKeyNotFound
	}
	event.Outcome = "ok"
	event.DataSHA256 = digest(data)
	m.schedule.RecordTransport(event)
	return append([]byte(nil), data...), m.manifestRevisions[key], nil
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

func (m *SnapshotReadTransport) QueueManifestReadFault(kind string) error {
	switch kind {
	case "timeout", "no_responders", "unavailable":
	default:
		return fmt.Errorf("unknown manifest read fault %q", kind)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.manifestReadFaults = append(m.manifestReadFaults, kind)
	return nil
}
