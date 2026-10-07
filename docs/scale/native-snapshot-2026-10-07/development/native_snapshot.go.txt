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
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/blobpublication"
	"js-wf/internal/checkpoint"
)

const nativeSnapshotSchema = "js-wf-native-snapshot-v1"

// NativeSnapshotPort is experimental. Its canonical manifest and complete
// pinned object graph live in the native blob authority. Source provides live
// journal/signal purge and legacy object staging/import. All snapshot readers
// and writers for a workflow must use this port before prefix purge is allowed.
// It does not enable production online collection or migrate other destinations.
type NativeSnapshotPort struct {
	Native    *blobpublication.NativePort
	Source    SnapshotWritePort
	IntentTTL time.Duration
}

var _ SnapshotWritePort = (*NativeSnapshotPort)(nil)
var _ AtomicSnapshotPublisher = (*NativeSnapshotPort)(nil)
var _ OwnedSnapshotReadPort = (*NativeSnapshotPort)(nil)

type nativeSnapshotEnvelope struct {
	Schema   string            `json:"schema"`
	Key      string            `json:"key"`
	Manifest Snapshot          `json:"manifest"`
	Objects  map[string]string `json:"objects"` // logical name -> pinned content hash
}

func NewNativeSnapshotPort(native *blobpublication.NativePort, source SnapshotWritePort, intentTTL time.Duration) (*NativeSnapshotPort, error) {
	if native == nil || source == nil || intentTTL <= 0 {
		return nil, errors.New("native snapshot authority, source and positive intent TTL required")
	}
	return &NativeSnapshotPort{Native: native, Source: source, IntentTTL: intentTTL}, nil
}
func nativeSnapshotDestination(key string) string { return "snapshot." + key }
func nativeSnapshotIdentity(key string) (string, string, error) {
	parts := strings.Split(key, ".")
	if len(parts) != 3 || parts[0] != "snap" || identity.Validate(parts[1], parts[2]) != nil {
		return "", "", ErrGap
	}
	return parts[1], parts[2], nil
}
func snapshotHash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func validSnapshotHash(value string) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(b) == 32 && hex.EncodeToString(b) == value
}
func decodeNativeSnapshot(root blobpublication.Root, key string) (nativeSnapshotEnvelope, error) {
	var value nativeSnapshotEnvelope
	decoder := json.NewDecoder(bytes.NewReader(root.Data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("%w: native snapshot envelope: %v", ErrGap, err)
	}
	if decoder.Decode(new(any)) != io.EOF || value.Schema != nativeSnapshotSchema || value.Key != key || value.Manifest.Object == "" || !validSnapshotHash(value.Manifest.SHA256) || ValidateRuntimeSnapshot(value.Manifest) != nil || value.Manifest.Version != 1 && value.Manifest.Version != 2 {
		return value, ErrGap
	}
	if value.Objects[value.Manifest.Object] != value.Manifest.SHA256 {
		return value, ErrGap
	}
	hashes := map[string]bool{}
	for name, hash := range value.Objects {
		if name == "" || !validSnapshotHash(hash) {
			return value, ErrGap
		}
		if _, ok := root.Blobs[hash]; !ok {
			return value, ErrGap
		}
		hashes[hash] = true
	}
	if len(hashes) != len(root.Blobs) {
		return value, ErrGap
	}
	return value, nil
}
func (p *NativeSnapshotPort) manifestRoot(ctx context.Context, key string) (blobpublication.Root, nativeSnapshotEnvelope, error) {
	if _, _, err := nativeSnapshotIdentity(key); err != nil {
		return blobpublication.Root{}, nativeSnapshotEnvelope{}, err
	}
	root, err := p.Native.ReadRoot(ctx, nativeSnapshotDestination(key))
	if err != nil {
		return root, nativeSnapshotEnvelope{}, err
	}
	if root.Token == "" {
		return root, nativeSnapshotEnvelope{}, jetstream.ErrKeyNotFound
	}
	value, err := decodeNativeSnapshot(root, key)
	return root, value, err
}
func (p *NativeSnapshotPort) GetManifestRevision(ctx context.Context, key string) (SnapshotManifestValue, error) {
	root, value, err := p.manifestRoot(ctx, key)
	if err != nil {
		return SnapshotManifestValue{Revision: root.Head}, err
	}
	data, err := json.Marshal(value.Manifest)
	return SnapshotManifestValue{Value: data, Revision: root.Head}, err
}
func (p *NativeSnapshotPort) GetManifest(ctx context.Context, key string) ([]byte, error) {
	value, err := p.GetManifestRevision(ctx, key)
	return value.Value, err
}
func (p *NativeSnapshotPort) GetOwnedObject(ctx context.Context, key, name string) ([]byte, error) {
	root, value, err := p.manifestRoot(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil, ErrSnapshotSuperseded
	}
	if err != nil {
		return nil, err
	}
	hash, ok := value.Objects[name]
	if !ok {
		return nil, ErrSnapshotSuperseded
	}
	ref := root.Blobs[hash]
	data, err := p.Native.GetBytes(ctx, ref.Object)
	if err != nil {
		return nil, err
	}
	if snapshotHash(data) != hash {
		return nil, ErrGap
	}
	return data, nil
}

// Generic content reads support frame staging and imported result/signal names.
// Owned snapshot reads above never fall back to legacy bytes or a new generation.
func (p *NativeSnapshotPort) GetObject(ctx context.Context, name string) ([]byte, error) {
	hash := logicalObjectHash(name)
	if hash != "" {
		record, err := p.Native.ReadBlob(ctx, hash)
		if err != nil {
			return nil, err
		}
		if record.Fence.Phase == "ready" {
			data, err := p.Native.GetBytes(ctx, record.Fence.Object)
			if err != nil {
				return nil, err
			}
			if snapshotHash(data) != hash {
				return nil, ErrGap
			}
			return data, nil
		}
	}
	data, err := p.Source.GetObject(ctx, name)
	if err == nil && hash != "" && snapshotHash(data) != hash {
		return nil, ErrGap
	}
	return data, err
}
func logicalObjectHash(name string) string {
	for _, prefix := range []string{"step-result-", "terminal-result-", "signal-", "input-"} {
		if strings.HasPrefix(name, prefix) && validSnapshotHash(strings.TrimPrefix(name, prefix)) {
			return strings.TrimPrefix(name, prefix)
		}
	}
	parts := strings.Split(name, "/")
	if len(parts) == 3 && validSnapshotHash(parts[0]) {
		return parts[0]
	}
	return ""
}
func (p *NativeSnapshotPort) PutObject(ctx context.Context, name string, data []byte) error {
	return p.Source.PutObject(ctx, name, data)
}
func (p *NativeSnapshotPort) CreateManifest(context.Context, string, []byte) error {
	return errors.New("native snapshot requires atomic publication")
}
func (p *NativeSnapshotPort) UpdateManifest(context.Context, string, []byte, uint64) error {
	return errors.New("native snapshot requires atomic publication")
}
func (p *NativeSnapshotPort) PurgeJournal(ctx context.Context, subject string, before uint64) error {
	return p.Source.PurgeJournal(ctx, subject, before)
}
func (p *NativeSnapshotPort) PurgeSignals(ctx context.Context, subject string, before uint64) error {
	return p.Source.PurgeSignals(ctx, subject, before)
}

// These also satisfy the worker's explicit ResultBlobPort. Writes continue to
// stage in the legacy bucket; live journal/result destination migration is open.
func (p *NativeSnapshotPort) PutBytes(ctx context.Context, name string, data []byte) error {
	return p.PutObject(ctx, name, data)
}
func (p *NativeSnapshotPort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	return p.GetObject(ctx, name)
}

func retainedSnapshotReferences(records []Record) (map[string]string, error) {
	refs := map[string]string{}
	for _, record := range records {
		if len(record.Payload) == 0 {
			continue
		}
		switch record.Kind {
		case StepCompleted, SignalConsumed, Completed, Failed:
			var payload struct {
				ResultRef  string `json:"result_ref"`
				Ref        string `json:"ref"`
				ResultHash string `json:"result_hash"`
			}
			if json.Unmarshal(record.Payload, &payload) != nil {
				return nil, ErrGap
			}
			if payload.ResultRef != "" {
				if payload.ResultHash != "" && !validSnapshotHash(payload.ResultHash) {
					return nil, ErrGap
				}
				if prior := refs[payload.ResultRef]; prior != "" && payload.ResultHash != "" && prior != payload.ResultHash {
					return nil, ErrGap
				}
				if payload.ResultHash != "" || refs[payload.ResultRef] == "" {
					refs[payload.ResultRef] = payload.ResultHash
				}
			}
			if payload.Ref != "" {
				if _, ok := refs[payload.Ref]; !ok {
					refs[payload.Ref] = ""
				}
			}
		}
	}
	return refs, nil
}
func (p *NativeSnapshotPort) PublishSnapshot(ctx context.Context, key string, expected uint64, snap Snapshot, data []byte) (Snapshot, error) {
	typ, id, err := nativeSnapshotIdentity(key)
	if err != nil || snapshotHash(data) != snap.SHA256 || ValidateRuntimeSnapshot(snap) != nil || snap.Version != 1 && snap.Version != 2 {
		return Snapshot{}, ErrGap
	}
	keyDigest := sha256.Sum256([]byte(identity.Key(typ, id)))
	if !strings.HasPrefix(snap.Object, "snapshot-"+hex.EncodeToString(keyDigest[:8])+"-") {
		return Snapshot{}, ErrGap
	}
	current, err := p.Native.ReadRoot(ctx, nativeSnapshotDestination(key))
	if err != nil {
		return Snapshot{}, err
	}
	if current.Head != expected {
		return Snapshot{}, ErrSnapshotStale
	}
	if current.Token != "" {
		previous, err := decodeNativeSnapshot(current, key)
		if err != nil {
			return Snapshot{}, err
		}
		if snap.LastSeq <= previous.Manifest.LastSeq || snap.LastIndex <= previous.Manifest.LastIndex {
			return Snapshot{}, ErrSnapshotStale
		}
		if old := previous.Manifest.Runtime; old != nil {
			if snap.Runtime == nil {
				return Snapshot{}, ErrCheckpointCompaction
			}
			if snap.Runtime.InvSeq != old.InvSeq || snap.Runtime.Index <= old.Index || snap.Runtime.Sequence <= old.Sequence || snap.Runtime.Epoch < old.Epoch {
				return Snapshot{}, ErrSnapshotStale
			}
		}
	}
	var records []Record
	if json.Unmarshal(data, &records) != nil || len(records) == 0 || len(records) > MaxEntries {
		return Snapshot{}, ErrGap
	}
	var checked []Record
	for _, record := range records {
		checked, err = verifyNext(checked, record)
		if err != nil {
			return Snapshot{}, err
		}
	}
	last := records[len(records)-1]
	if last.Sequence != snap.LastSeq || last.Index != snap.LastIndex || last.Epoch != snap.Epoch {
		return Snapshot{}, ErrGap
	}
	refs, err := retainedSnapshotReferences(records)
	if err != nil {
		return Snapshot{}, err
	}
	cached := map[string][]byte{snap.Object: append([]byte(nil), data...)}
	// A retained archive can contain older checkpoint frames as ordinary step
	// results. Their promise outcomes remain reachable during full replay even
	// when the current runtime pointer refers to a newer frame.
	var request struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	var sdkPosition uint64
	for _, record := range records {
		if record.Kind == StepRequested {
			sdkPosition++
			request.Kind, request.Name, request.InputHash = "", "", ""
			if len(record.Payload) != 0 && json.Unmarshal(record.Payload, &request) != nil {
				return Snapshot{}, ErrGap
			}
		}
		if record.Kind != StepCompleted {
			continue
		}
		sdkPosition++
		if request.Kind != "checkpoint" {
			continue
		}
		var completion struct {
			ResultRef  string `json:"result_ref"`
			ResultHash string `json:"result_hash"`
		}
		if json.Unmarshal(record.Payload, &completion) != nil || completion.ResultRef != "step-result-"+completion.ResultHash || !validSnapshotHash(completion.ResultHash) {
			return Snapshot{}, ErrGap
		}
		frameBytes, err := p.GetObject(ctx, completion.ResultRef)
		if err != nil {
			return Snapshot{}, err
		}
		var declared checkpoint.Frame
		if json.Unmarshal(frameBytes, &declared) != nil || declared.Identity.InvSeq == 0 {
			return Snapshot{}, ErrGap
		}
		frame, err := checkpoint.Decode(frameBytes, completion.ResultHash, checkpoint.Identity{Type: typ, ID: id, InvSeq: declared.Identity.InvSeq}, checkpoint.Anchor{Index: record.Index, Epoch: record.Epoch})
		if err != nil || frame.Stage != request.Name || frame.StepPosition != sdkPosition || snapshotHash(frame.Data) != request.InputHash {
			return Snapshot{}, ErrGap
		}
		cached[completion.ResultRef] = frameBytes
		outcomes := make([]Record, 0, len(frame.PromiseOutcomes))
		for _, outcome := range frame.PromiseOutcomes {
			outcomes = append(outcomes, Record{Entry: Entry{Kind: Completed, Payload: outcome}})
		}
		dependencies, err := retainedSnapshotReferences(outcomes)
		if err != nil {
			return Snapshot{}, err
		}
		for name, hash := range dependencies {
			if prior := refs[name]; prior != "" && hash != "" && prior != hash {
				return Snapshot{}, ErrGap
			}
			if hash != "" || refs[name] == "" {
				refs[name] = hash
			}
		}
		request.Kind = ""
	}
	if snap.Runtime != nil {
		r := snap.Runtime
		frameBytes, err := p.GetObject(ctx, r.Object)
		if err != nil {
			return Snapshot{}, err
		}
		frame, err := checkpoint.Decode(frameBytes, r.SHA256, checkpoint.Identity{Type: typ, ID: id, InvSeq: r.InvSeq}, checkpoint.Anchor{Index: r.Index, Epoch: r.Epoch})
		if err != nil || frame.Stage != r.Stage || frame.StepPosition != r.StepPosition {
			return Snapshot{}, ErrGap
		}
		cached[r.Object] = frameBytes
		refs[r.Object] = r.SHA256
		var outcomes []Record
		for _, outcome := range frame.PromiseOutcomes {
			outcomes = append(outcomes, Record{Entry: Entry{Kind: Completed, Payload: outcome}})
		}
		outcomeRefs, err := retainedSnapshotReferences(outcomes)
		if err != nil {
			return Snapshot{}, err
		}
		for name, hash := range outcomeRefs {
			if prior := refs[name]; prior != "" && hash != "" && prior != hash {
				return Snapshot{}, ErrGap
			}
			if hash != "" || refs[name] == "" {
				refs[name] = hash
			}
		}
	}
	names := make([]string, 0, len(refs))
	for name := range refs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		bytes, ok := cached[name]
		if !ok {
			bytes, err = p.GetObject(ctx, name)
			if err != nil {
				return Snapshot{}, err
			}
			cached[name] = bytes
		}
		if refs[name] != "" && snapshotHash(bytes) != refs[name] {
			return Snapshot{}, ErrGap
		}
	}
	value := nativeSnapshotEnvelope{Schema: nativeSnapshotSchema, Key: key, Manifest: snap, Objects: map[string]string{}}
	var blobs [][]byte
	names = names[:0]
	for name := range cached {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value.Objects[name] = snapshotHash(cached[name])
		blobs = append(blobs, cached[name])
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return Snapshot{}, err
	}
	pinned, err := (blobpublication.Protocol{Port: p.Native}).PrepareAt(ctx, nativeSnapshotDestination(key), expected, payload, blobs, time.Now().Add(p.IntentTTL))
	if errors.Is(err, blobpublication.ErrConflict) {
		return Snapshot{}, ErrSnapshotStale
	}
	if err != nil {
		return Snapshot{}, err
	}
	if _, err = (blobpublication.Protocol{Port: p.Native}).Commit(ctx, pinned); err != nil {
		if errors.Is(err, blobpublication.ErrConflict) {
			return Snapshot{}, ErrSnapshotStale
		}
		return Snapshot{}, err
	}
	return snap, nil
}

// ImportSnapshot verifies the legacy Store, live checkpoint anchor when present,
// and manifest revision before copying its complete retained graph. source must
// read the legacy workflow journal using Source as its snapshot transport.
// The caller must stop legacy snapshot writers/readers before changing the
// canonical destination; this does not retire the old manifest or other roots.
func (p *NativeSnapshotPort) ImportSnapshot(ctx context.Context, key string, source *Store) (Snapshot, error) {
	typ, id, err := nativeSnapshotIdentity(key)
	if err != nil || source == nil {
		return Snapshot{}, ErrGap
	}
	legacy, err := p.Source.GetManifestRevision(ctx, key)
	if err != nil {
		return Snapshot{}, err
	}
	var snap Snapshot
	if json.Unmarshal(legacy.Value, &snap) != nil {
		return Snapshot{}, ErrGap
	}
	data, err := p.Source.GetObject(ctx, snap.Object)
	if err != nil {
		return Snapshot{}, err
	}
	// Read through the original Store so the archival prefix and live suffix,
	// including a continuation completion anchor, are independently checked.
	records, _, err := source.Read(ctx, typ, id)
	if err != nil {
		return Snapshot{}, err
	}
	var prefix []Record
	if json.Unmarshal(data, &prefix) != nil || len(prefix) == 0 || len(prefix) > len(records) {
		return Snapshot{}, ErrGap
	}
	archived, err := json.Marshal(prefix)
	if err != nil {
		return Snapshot{}, err
	}
	observed, err := json.Marshal(records[:len(prefix)])
	if err != nil || !bytes.Equal(archived, observed) {
		return Snapshot{}, ErrGap
	}
	if snap.Runtime != nil {
		if err := source.verifyRuntimeCheckpoint(ctx, typ, id, records, *snap.Runtime); err != nil {
			return Snapshot{}, err
		}
	}
	confirmed, err := p.Source.GetManifestRevision(ctx, key)
	if err != nil {
		return Snapshot{}, err
	}
	if confirmed.Revision != legacy.Revision || !bytes.Equal(confirmed.Value, legacy.Value) {
		return Snapshot{}, ErrSnapshotStale
	}
	root, err := p.Native.ReadRoot(ctx, nativeSnapshotDestination(key))
	if err != nil {
		return Snapshot{}, err
	}
	if root.Token != "" {
		return Snapshot{}, ErrSnapshotStale
	}
	return p.PublishSnapshot(ctx, key, root.Head, snap, data)
}

// RetireManifest advances the permanent head before making its object graph
// reclaimable. Workflow retention must also retire its other canonical roots.
func (p *NativeSnapshotPort) RetireManifest(ctx context.Context, key string, expected uint64) error {
	if _, _, err := nativeSnapshotIdentity(key); err != nil {
		return err
	}
	return (blobpublication.Protocol{Port: p.Native}).Retire(ctx, nativeSnapshotDestination(key), expected)
}
