package blobpublication

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"js-wf/internal/natsstream"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const nativeBlobFormat = "recoverable-v1"
const nativeChunkSize = 128 * 1024

var ErrUntrackedChunks = errors.New("untracked native blob chunks")

// NativePort is an experimental full protocol port. The bucket is exclusively
// written through this adapter; ordinary ObjectStore readers remain compatible.
// Chunk IDs encode the unique physical attempt so partial uploads are visible
// to collection even before metadata exists. Runtime workflow wiring is absent.
type NativePort struct {
	*NativeAuthority
	bucket       string
	objects      jetstream.ObjectStore
	objectStream jetstream.Stream
}

var _ Port = (*NativePort)(nil)

// NativeObjectStreamConfig provisions a NEW isolated protocol bucket. Metadata
// uses standard ObjectStore rollups; chunk subjects retain all their messages.
// Administrators must not relax retention or destroy/recreate its authority.
func NativeObjectStreamConfig(bucket string, replicas int) jetstream.StreamConfig {
	return jetstream.StreamConfig{Name: "OBJ_" + bucket, Subjects: []string{"$O." + bucket + ".C.>", "$O." + bucket + ".M.>"}, Storage: jetstream.FileStorage, Replicas: replicas, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld, AllowRollup: true, DenyDelete: true, Metadata: map[string]string{"js-wf-blob-format": nativeBlobFormat}}
}
func OpenNativePort(ctx context.Context, authority *NativeAuthority, bucket string) (*NativePort, error) {
	if authority == nil || !validID(strings.ReplaceAll(bucket, "_", "-")) {
		return nil, errors.New("invalid native blob bucket")
	}
	stream, err := authority.js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return nil, err
	}
	p := &NativePort{NativeAuthority: authority, bucket: bucket, objectStream: natsstream.Guard(stream)}
	if err = p.validateObjects(ctx); err != nil {
		return nil, err
	}
	p.objects, err = authority.js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return p, nil
}
func (p *NativePort) validateObjects(ctx context.Context) error {
	info, err := p.objectStream.Info(ctx)
	if err != nil {
		return err
	}
	c := info.Config
	if c.Name != "OBJ_"+p.bucket || !reflect.DeepEqual(c.Subjects, []string{"$O." + p.bucket + ".C.>", "$O." + p.bucket + ".M.>"}) || c.Metadata["js-wf-blob-format"] != nativeBlobFormat || c.Storage != jetstream.FileStorage || c.Replicas < 1 || c.Retention != jetstream.LimitsPolicy || c.Discard != jetstream.DiscardOld || c.MaxAge != 0 || c.MaxMsgs > 0 || c.MaxBytes > 0 || c.MaxMsgsPerSubject > 0 || c.MaxMsgSize > 0 || c.AllowDirect || c.AllowMsgTTL || c.SubjectDeleteMarkerTTL != 0 || !c.AllowRollup || !c.DenyDelete || c.DenyPurge || c.NoAck || c.Sealed || c.Mirror != nil || len(c.Sources) != 0 || c.SubjectTransform != nil || c.RePublish != nil || c.DiscardNewPerSubject || c.AllowAtomicPublish || c.AllowMsgSchedules || c.PersistMode != jetstream.DefaultPersistMode {
		return errors.New("unsafe recoverable ObjectStore configuration")
	}
	return nil
}
func parsePhysicalObject(name string) (Object, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 {
		return Object{}, errors.New("invalid physical object name")
	}
	gen, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return Object{}, err
	}
	o := Object{Key: parts[0], Reference: Reference{Generation: gen, Object: name}}
	if !validHash(o.Key) || !validObject(o) || parts[1] != strconv.FormatUint(gen, 10) {
		return Object{}, errors.New("invalid physical object identity")
	}
	return o, nil
}
func physicalEncoding(name string) string { return base64.URLEncoding.EncodeToString([]byte(name)) }
func (p *NativePort) metaSubject(name string) string {
	return "$O." + p.bucket + ".M." + physicalEncoding(name)
}
func (p *NativePort) chunkSubject(name string) string {
	return "$O." + p.bucket + ".C." + physicalEncoding(name)
}
func (p *NativePort) metadata(ctx context.Context, name string) (*jetstream.ObjectInfo, uint64, error) {
	message, err := p.objectStream.GetLastMsgForSubject(ctx, p.metaSubject(name))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var info jetstream.ObjectInfo
	if err = json.Unmarshal(message.Data, &info); err != nil {
		return nil, 0, err
	}
	if message.Subject != p.metaSubject(name) || info.Name != name || info.Bucket != p.bucket || info.NUID != physicalEncoding(name) || info.Opts != nil && info.Opts.Link != nil {
		return nil, 0, errors.New("foreign or invalid native object metadata")
	}
	if _, err = parsePhysicalObject(info.Name); err != nil {
		return nil, 0, err
	}
	return &info, message.Sequence, nil
}
func (p *NativePort) publishObject(ctx context.Context, subject string, data []byte, headers nats.Header) error {
	if headers == nil {
		headers = nats.Header{}
	}
	headers.Set(jetstream.ExpectedStreamHeader, "OBJ_"+p.bucket)
	ack, err := p.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Data: data, Header: headers})
	if err != nil {
		var api *jetstream.APIError
		if errors.As(err, &api) && (api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
			return ErrConflict
		}
		return err
	}
	if ack.Stream != "OBJ_"+p.bucket || ack.Sequence == 0 || ack.Duplicate {
		return errors.New("unexpected native object acknowledgment")
	}
	return nil
}
func (p *NativePort) publishMetadata(ctx context.Context, info jetstream.ObjectInfo, expected uint64) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return p.publishObject(ctx, p.metaSubject(info.Name), data, nats.Header{jetstream.ExpectedLastSubjSeqHeader: []string{fmt.Sprint(expected)}, jetstream.MsgRollup: []string{jetstream.MsgRollupSubject}})
}

// Put never overwrites an attempt, including a deleted one. Synchronous chunk
// acknowledgments precede one conditional metadata publication. A failed call
// does not promote by observing bytes; recovery treats all its chunks as orphans.
func (p *NativePort) Put(ctx context.Context, name string, data []byte) error {
	object, err := parsePhysicalObject(name)
	if err != nil {
		return err
	}
	if key(data) != object.Key {
		return errors.New("physical object content hash mismatch")
	}
	if err = p.validateObjects(ctx); err != nil {
		return err
	}
	record, err := p.ReadBlob(ctx, object.Key)
	if err != nil {
		return err
	}
	if record.Revision == 0 || record.Fence.Generation != object.Reference.Generation || record.Fence.Phase == "closed" {
		return ErrRevoked
	}
	existing, _, err := p.metadata(ctx, name)
	if err != nil {
		return err
	}
	if existing != nil {
		return errors.New("native physical attempt reused")
	}
	chunks := uint32(0)
	for offset := 0; offset < len(data); offset += nativeChunkSize {
		end := offset + nativeChunkSize
		if end > len(data) {
			end = len(data)
		}
		if err = p.publishObject(ctx, p.chunkSubject(name), data[offset:end], nil); err != nil {
			return err
		}
		chunks++
	}
	hash := sha256.New()
	_, _ = hash.Write(data)
	info := jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: name, Opts: &jetstream.ObjectMetaOptions{ChunkSize: nativeChunkSize}}, Bucket: p.bucket, NUID: physicalEncoding(name), Size: uint64(len(data)), Chunks: chunks, Digest: jetstream.GetObjectDigestValue(hash)}
	return p.publishMetadata(ctx, info, 0)
}

// Delete writes a durable attempt tombstone before purging its chunk subject.
// A delayed metadata publish expecting sequence zero then fails at the server.
// Later chunk writes cannot become referenced; Objects discovers them again.
func (p *NativePort) Delete(ctx context.Context, name string) error {
	if _, err := parsePhysicalObject(name); err != nil {
		return err
	}
	if err := p.validateObjects(ctx); err != nil {
		return err
	}
	for attempt := 0; attempt < 16; attempt++ {
		info, seq, err := p.metadata(ctx, name)
		if err != nil {
			return err
		}
		if info == nil {
			info = &jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: name}, Bucket: p.bucket, NUID: physicalEncoding(name)}
		}
		info.Deleted = true
		info.Size = 0
		info.Chunks = 0
		info.Digest = ""
		err = p.publishMetadata(ctx, *info, seq)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
		return p.objectStream.Purge(ctx, jetstream.WithPurgeSubject(p.chunkSubject(name)))
	}
	return ErrConflict
}

// Objects includes partial uploads and chunks recreated after a prior delete.
// Unknown subjects/opaque chunk IDs block collection before physical deletion.
func (p *NativePort) Objects(ctx context.Context) ([]Object, error) {
	if err := p.validateObjects(ctx); err != nil {
		return nil, err
	}
	prefix := "$O." + p.bucket + "."
	info, err := p.objectStream.Info(ctx, jetstream.WithSubjectFilter(prefix+">"))
	if err != nil {
		return nil, err
	}
	if uint64(len(info.State.Subjects)) != info.State.NumSubjects {
		return nil, errors.New("incomplete native object census")
	}
	names := map[string]bool{}
	for subject, count := range info.State.Subjects {
		if !strings.HasPrefix(subject, prefix) || count == 0 {
			return nil, ErrUntrackedChunks
		}
		suffix := strings.TrimPrefix(subject, prefix)
		parts := strings.Split(suffix, ".")
		if len(parts) != 2 || (parts[0] != "C" && parts[0] != "M") {
			return nil, ErrUntrackedChunks
		}
		decoded, err := base64.URLEncoding.DecodeString(parts[1])
		if err != nil || physicalEncoding(string(decoded)) != parts[1] {
			return nil, ErrUntrackedChunks
		}
		name := string(decoded)
		if _, err = parsePhysicalObject(name); err != nil {
			return nil, ErrUntrackedChunks
		}
		if parts[0] == "C" {
			names[name] = true
			continue
		}
		if count != 1 {
			return nil, errors.New("invalid native object metadata history")
		}
		meta, _, err := p.metadata(ctx, name)
		if err != nil {
			return nil, err
		}
		if meta == nil {
			return nil, errors.New("native object disappeared during census")
		}
		if !meta.Deleted {
			names[name] = true
		}
	}
	result := make([]Object, 0, len(names))
	for name := range names {
		o, err := parsePhysicalObject(name)
		if err != nil {
			return nil, err
		}
		result = append(result, o)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Reference.Object < result[j].Reference.Object })
	return result, nil
}

// GetBytes is a compatibility/read-integrity check through the normal client.
func (p *NativePort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	o, err := parsePhysicalObject(name)
	if err != nil {
		return nil, err
	}
	if err = p.validateObjects(ctx); err != nil {
		return nil, err
	}
	if _, _, err = p.metadata(ctx, name); err != nil {
		return nil, err
	}
	data, err := p.objects.GetBytes(ctx, name)
	if err != nil {
		return nil, err
	}
	if key(data) != o.Key {
		return nil, errors.New("native blob digest mismatch")
	}
	return data, nil
}
