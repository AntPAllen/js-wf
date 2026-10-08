package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"js-wf/internal/blobpublication"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const authoritySchema = "js-wf-graph-authority-v1"
const MaxAuthorityBytes = MaxFenceBytes + 4096

// NativeAuthority is experimental graph metadata storage. Provision a dedicated
// stream and subject prefix before opening; it never creates a missing stream.
// Its graph schema rejects legacy direct-reference records and the legacy
// adapter rejects this schema. There is no automatic import or stream upgrade.
// Administrators must preserve permanent logical heads/generations and cannot
// delete/recreate the stream. Object namespace isolation, runtime migration,
// reader pins and native fault qualification remain separate requirements.
type NativeAuthority struct {
	js           jetstream.JetStream
	stream       jetstream.Stream
	name, prefix string
}

// Authority is the metadata subset of Port. Reads conditionally reaffirm the
// exact same subject value with a quorum-acknowledged write. Administrative GET
// alone is tentative; CAS uses its sequence only as a conditional write fence.
type Authority interface {
	ReadRoot(context.Context, string) (Root, error)
	CASRoot(context.Context, string, uint64, Root) (Root, error)
	ReadBlob(context.Context, string) (Record, error)
	CASBlob(context.Context, string, uint64, Fence) (Record, error)
	BlobKeys(context.Context) ([]string, error)
}

var _ Authority = (*NativeAuthority)(nil)

type authorityValue struct {
	Schema   string `json:"schema"`
	Kind     string `json:"kind"`
	Identity string `json:"identity"`
	Revision uint64 `json:"revision"`
	Root     *Root  `json:"root,omitempty"`
	Fence    *Fence `json:"fence,omitempty"`
}

// AuthorityStreamConfig is for a new, explicitly provisioned authority. It
// keeps the latest record per subject, which carries the durable high-water
// revision. Global stream sequences are NOT logical destination heads.
func AuthorityStreamConfig(name, prefix string, replicas int) jetstream.StreamConfig {
	return jetstream.StreamConfig{Name: name, Subjects: []string{prefix + ".>"}, Storage: jetstream.FileStorage, Replicas: replicas, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld, MaxMsgsPerSubject: 1, DenyDelete: true, DenyPurge: true}
}
func OpenNativeAuthority(ctx context.Context, js jetstream.JetStream, name, prefix string) (*NativeAuthority, error) {
	if name == "" || prefix == "" || !utf8.ValidString(name) || !utf8.ValidString(prefix) || strings.ContainsAny(prefix, "*> /\\\t\r\n") || strings.HasPrefix(prefix, ".") || strings.HasSuffix(prefix, ".") || strings.Contains(prefix, "..") {
		return nil, errors.New("invalid authority namespace")
	}
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		lookup, stop := context.WithTimeout(ctx, 2*time.Second)
		stream, err := js.Stream(lookup, name)
		var p *NativeAuthority
		if err == nil {
			p = &NativeAuthority{js: js, stream: stream, name: name, prefix: prefix}
			err = p.validate(lookup)
		}
		stop()
		if err == nil {
			return p, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		last = err
		// Read-only admission can lose a reply during cold routing. Every attempt
		// has a fresh bounded context; missing/unsafe authorities are never retried
		// or recreated, and no publication is repeated by this constructor.
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, nats.ErrTimeout) {
			return nil, err
		}
	}
	return nil, last
}
func (p *NativeAuthority) validate(ctx context.Context) error {
	info, err := p.stream.Info(ctx)
	if err != nil {
		return err
	}
	c := info.Config
	if c.Name != p.name || !reflect.DeepEqual(c.Subjects, []string{p.prefix + ".>"}) || c.Storage != jetstream.FileStorage || c.Retention != jetstream.LimitsPolicy || c.Discard != jetstream.DiscardOld || c.MaxMsgsPerSubject != 1 || c.MaxAge != 0 || c.MaxMsgs > 0 || c.MaxBytes > 0 || !c.DenyDelete || !c.DenyPurge || c.AllowDirect || c.AllowMsgTTL || c.SubjectDeleteMarkerTTL != 0 || c.AllowRollup || c.NoAck || c.Sealed || c.Mirror != nil || len(c.Sources) != 0 || c.SubjectTransform != nil || c.RePublish != nil || c.Replicas < 1 || c.DiscardNewPerSubject || c.AllowAtomicPublish || c.AllowMsgSchedules || c.PersistMode != jetstream.DefaultPersistMode {
		return errors.New("unsafe graph authority stream configuration")
	}
	return nil
}
func (p *NativeAuthority) subject(kind, identity string) string {
	if kind == "root" {
		identity = key([]byte(identity))
	}
	return p.prefix + "." + kind + "." + identity
}
func (p *NativeAuthority) readSnapshot(ctx context.Context, kind, identity string) (authorityValue, uint64, error) {
	if identity == "" || (kind == "root" && (len(identity) > 256 || !utf8.ValidString(identity))) || (kind != "root" && kind != "blob") {
		return authorityValue{}, 0, errors.New("empty authority identity")
	}
	if kind == "blob" && !validHash(identity) {
		return authorityValue{}, 0, errors.New("invalid blob hash")
	}
	if err := p.validate(ctx); err != nil {
		return authorityValue{}, 0, err
	}
	subject := p.subject(kind, identity)
	message, err := p.stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return authorityValue{}, 0, nil
	}
	if err != nil {
		return authorityValue{}, 0, err
	}
	if message.Subject != subject || message.Sequence == 0 {
		return authorityValue{}, 0, errors.New("invalid graph authority message")
	}
	value, err := decodeAuthority(message.Data, kind, identity)
	if err != nil {
		return authorityValue{}, 0, err
	}

	return value, message.Sequence, nil
}

// read establishes a quorum-acknowledged witness at the same destination as
// the observed value. Administrative GET alone only checks current leadership;
// it does not establish the linearizable read contract required by Port. The
// conditional reaffirmation rejects stale value/absence snapshots, preserves
// logical heads and generations, and returns the acknowledged physical sequence.
// Reads require publish permission and perform a durable write, including for
// absent identities. All authority users must tolerate physical sequence churn.
func (p *NativeAuthority) read(ctx context.Context, kind, identity string) (authorityValue, uint64, error) {
	return blobpublication.ReadWithWitness(ctx, func(call context.Context) (authorityValue, uint64, error) {
		v, seq, err := p.readSnapshot(call, kind, identity)
		if err != nil {
			return v, 0, err
		}
		if seq == 0 {
			v = authorityValue{Schema: authoritySchema, Kind: kind, Identity: identity}
		}
		return v, seq, nil
	}, func(call context.Context, seq uint64, v authorityValue) (uint64, error) {
		return p.publishWithSequence(call, seq, v, true)
	})
}

func validHash(k string) bool {
	if len(k) != 64 {
		return false
	}
	for _, c := range k {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// validateAuthority never normalizes stored bytes. Zero logical revisions have
// no root/fence; their physical absence witnesses remain permanent subjects.
func validateAuthority(v authorityValue, kind, identity string) error {
	if (kind != "root" && kind != "blob") || v.Schema != authoritySchema || v.Kind != kind || v.Identity != identity || identity == "" || (kind == "root" && (len(identity) > 256 || !utf8.ValidString(identity))) || (kind == "blob" && !validHash(identity)) {
		return errors.New("invalid graph authority envelope")
	}
	if v.Revision == 0 {
		if v.Root != nil || v.Fence != nil {
			return errors.New("invalid graph absence witness")
		}
		return nil
	}
	switch kind {
	case "root":
		if v.Root == nil || v.Fence != nil || v.Root.Head != v.Revision || v.Root.Schema != Schema {
			return errors.New("invalid graph authority root")
		}
		_, err := normalizeRoot(*v.Root)
		return err
	case "blob":
		if v.Fence == nil || v.Root != nil {
			return errors.New("invalid graph authority fence")
		}
		return validateFence(identity, Record{Revision: v.Revision, Fence: *v.Fence})
	default:
		return errors.New("invalid graph authority kind")
	}
}
func decodeAuthority(data []byte, kind, identity string) (authorityValue, error) {
	var v authorityValue
	if len(data) == 0 || len(data) > MaxAuthorityBytes {
		return v, errors.New("graph authority byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&v); err != nil {
		return authorityValue{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return authorityValue{}, errors.New("trailing graph authority data")
	}
	encoded, err := json.Marshal(v)
	if err != nil || !bytes.Equal(data, encoded) {
		return authorityValue{}, errors.New("noncanonical graph authority encoding")
	}
	if err = validateAuthority(v, kind, identity); err != nil {
		return authorityValue{}, err
	}
	return v, nil
}
func (p *NativeAuthority) publishWithSequence(ctx context.Context, expected uint64, v authorityValue, witness bool) (uint64, error) {
	if err := p.validate(ctx); err != nil {
		return 0, err
	}
	if err := validateAuthority(v, v.Kind, v.Identity); err != nil {
		return 0, err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	if len(data) > MaxAuthorityBytes {
		return 0, errors.New("graph authority byte limit")
	}
	message := &nats.Msg{Subject: p.subject(v.Kind, v.Identity), Data: data, Header: nats.Header{}}
	message.Header.Set(jetstream.ExpectedStreamHeader, p.name)
	message.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprint(expected))
	if witness {
		message.Header.Set("Wf-Authority-Read-Witness", "1")
	}
	ack, err := p.js.PublishMsg(ctx, message)
	if err != nil {
		var api *jetstream.APIError
		if errors.As(err, &api) && (api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
			return 0, ErrConflict
		}
		return 0, err
	}
	if ack == nil || ack.Stream != p.name || ack.Sequence <= expected || ack.Duplicate {
		return 0, errors.New("unexpected authority acknowledgment")
	}
	return ack.Sequence, nil
}
func (p *NativeAuthority) publish(ctx context.Context, expected uint64, v authorityValue) error {
	_, err := p.publishWithSequence(ctx, expected, v, false)
	return err
}

func (p *NativeAuthority) ReadRoot(ctx context.Context, destination string) (Root, error) {
	v, _, err := p.read(ctx, "root", destination)
	if err != nil {
		return Root{}, err
	}
	if v.Root == nil {
		return EmptyRoot(), nil
	}
	return *v.Root, nil
}
func (p *NativeAuthority) CASRoot(ctx context.Context, destination string, head uint64, next Root) (Root, error) {
	v, seq, err := p.readSnapshot(ctx, "root", destination)
	if err != nil {
		return Root{}, err
	}
	if v.Revision != head {
		return Root{}, ErrConflict
	}
	if head == ^uint64(0) {
		return Root{}, errors.New("head exhausted")
	}
	next.Head = head + 1
	next, err = normalizeRoot(next)
	if err != nil {
		return Root{}, err
	}
	data, err := json.Marshal(next)
	if err != nil {
		return Root{}, err
	}
	var cloned Root
	if err = json.Unmarshal(data, &cloned); err != nil {
		return Root{}, err
	}
	next = cloned
	err = p.publish(ctx, seq, authorityValue{Schema: authoritySchema, Kind: "root", Identity: destination, Revision: next.Head, Root: &next})
	if err != nil {
		return Root{}, err
	}
	return next, nil
}
func (p *NativeAuthority) ReadBlob(ctx context.Context, k string) (Record, error) {
	v, _, err := p.read(ctx, "blob", k)
	if err != nil {
		return Record{}, err
	}
	if v.Fence == nil {
		return Record{}, nil
	}
	return Record{v.Revision, *v.Fence}, nil
}
func (p *NativeAuthority) CASBlob(ctx context.Context, k string, revision uint64, next Fence) (Record, error) {
	v, seq, err := p.readSnapshot(ctx, "blob", k)
	if err != nil {
		return Record{}, err
	}
	if v.Revision != revision {
		return Record{}, ErrConflict
	}
	if revision == ^uint64(0) {
		return Record{}, errors.New("revision exhausted")
	}
	if err = validateFence(k, Record{Revision: revision + 1, Fence: next}); err != nil {
		return Record{}, err
	}
	if v.Fence == nil {
		if next.Generation != 1 || next.Phase != "uploading" {
			return Record{}, errors.New("invalid first generation")
		}
	} else {
		old := *v.Fence
		if old.Hash != next.Hash || old.Owner != next.Owner {
			return Record{}, errors.New("graph scope identity changed")
		}
		if next.Generation != old.Generation {
			if old.Phase != "closed" || old.Generation == ^uint64(0) || next.Generation != old.Generation+1 || next.Phase != "uploading" {
				return Record{}, errors.New("invalid generation advance")
			}
		} else {
			if old.Phase == "closed" && next.Phase != "closed" || old.Phase == "ready" && next.Phase != "closed" && (next.Phase != "ready" || next.Object != old.Object) {
				return Record{}, errors.New("immutable generation selection changed")
			}
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return Record{}, err
	}
	var cloned Fence
	if err = json.Unmarshal(data, &cloned); err != nil {
		return Record{}, err
	}
	next = cloned
	err = p.publish(ctx, seq, authorityValue{Schema: authoritySchema, Kind: "blob", Identity: k, Revision: revision + 1, Fence: &next})
	if err != nil {
		return Record{}, err
	}
	return Record{revision + 1, next}, nil
}
func (p *NativeAuthority) BlobKeys(ctx context.Context) ([]string, error) {
	if err := p.validate(ctx); err != nil {
		return nil, err
	}
	prefix := p.prefix + ".blob."
	info, err := p.stream.Info(ctx, jetstream.WithSubjectFilter(p.prefix+".>"))
	if err != nil {
		return nil, err
	}
	if uint64(len(info.State.Subjects)) != info.State.NumSubjects {
		return nil, errors.New("incomplete authority census")
	}
	keys := []string{}
	for subject, count := range info.State.Subjects {
		if count != 1 {
			return nil, errors.New("invalid authority subject count")
		}
		if strings.HasPrefix(subject, prefix) {
			k := strings.TrimPrefix(subject, prefix)
			if !validHash(k) {
				return nil, errors.New("invalid authority blob subject")
			}
			keys = append(keys, k)
			continue
		}
		rootPrefix := p.prefix + ".root."
		if !strings.HasPrefix(subject, rootPrefix) || !validHash(strings.TrimPrefix(subject, rootPrefix)) {
			return nil, errors.New("unexpected authority subject")
		}
	}
	sort.Strings(keys)
	return keys, nil
}
