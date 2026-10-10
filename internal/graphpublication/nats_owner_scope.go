package graphpublication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/blobpublication"
)

const ownerIndexSchema = "js-wf-graph-owner-index-v1"
const ownerIndexMetadata = "wf_graph_owner_index"

// OwnerIndexedAuthorityStreamConfig is for a NEW isolated namespace. It is not
// an upgrade: never add its mode tag to an existing stream. Older graph adapters
// reject the tag before reading or writing, preventing unregistered scopes.
// Administrators must preserve the tag and permanent stream records. As with
// graph heads/generations, administrative deletion or import breaks authority.
func OwnerIndexedAuthorityStreamConfig(name, prefix string, replicas int) jetstream.StreamConfig {
	c := AuthorityStreamConfig(name, prefix, replicas)
	// Existing binaries demand exactly prefix+".>". A distinct subject shape
	// makes them reject this namespace even if they predate the metadata tag.
	c.Subjects = []string{prefix + ".root.>", prefix + ".blob.>", prefix + ".owner.>"}
	c.Metadata = map[string]string{ownerIndexMetadata: ownerIndexSchema}
	return c
}

func OpenOwnerIndexedNativeAuthority(ctx context.Context, js jetstream.JetStream, name, prefix string) (*NativeAuthority, error) {
	return openNativeAuthority(ctx, js, name, prefix, true)
}

// OwnerIndexedNativePort exposes complete owner discovery only for the explicit
// indexed mode. Legacy NativePort must not satisfy OwnerScopePort accidentally.
type OwnerIndexedNativePort struct{ *NativePort }

var _ OwnerScopePort = (*OwnerIndexedNativePort)(nil)

func OpenOwnerIndexedNativePort(ctx context.Context, a *NativeAuthority, bucket string) (*OwnerIndexedNativePort, error) {
	if a == nil || !a.ownerIndexed {
		return nil, errors.New("indexed graph authority required")
	}
	if err := a.validate(ctx); err != nil {
		return nil, err
	}
	p, err := OpenNativePort(ctx, a, bucket)
	if err != nil {
		return nil, err
	}
	return &OwnerIndexedNativePort{p}, nil
}

type ownerScopeValue struct {
	Schema string `json:"schema"`
	Owner  string `json:"owner"`
	Scope  string `json:"scope"`
}

func (p *NativeAuthority) ownerSubject(owner, scope string) string {
	return p.prefix + ".owner." + key([]byte(owner)) + "." + scope
}
func (p *NativeAuthority) validOwnerIndexSubject(subject string) bool {
	parts := strings.Split(strings.TrimPrefix(subject, p.prefix+".owner."), ".")
	return strings.HasPrefix(subject, p.prefix+".owner.") && len(parts) == 2 && validHash(parts[0]) && validHash(parts[1])
}

// ensureOwnerScope is called before the blob mutation. A lost marker reply
// stops that mutation even if registration committed. A later fresh invocation
// can witness the immutable registration. Scope reads remain separate authority.
func (p *NativeAuthority) ensureOwnerScope(ctx context.Context, owner, scope string) error {
	return p.witnessOwnerScope(ctx, owner, scope, true)
}
func (p *NativeAuthority) witnessOwnerScope(ctx context.Context, owner, scope string, create bool) error {
	if !p.ownerIndexed || !validID(owner) || !validHash(scope) {
		return errors.New("invalid indexed graph scope")
	}
	if err := p.validate(ctx); err != nil {
		return err
	}
	subject := p.ownerSubject(owner, scope)
	data, err := json.Marshal(ownerScopeValue{ownerIndexSchema, owner, scope})
	if err != nil {
		return err
	}
	release, err := p.reads.acquire(ctx, "owner:"+subject)
	if err != nil {
		return err
	}
	defer release()
	_, _, err = blobpublication.ReadWithWitness(ctx, func(c context.Context) ([]byte, uint64, error) {
		msg, e := p.stream.GetLastMsgForSubject(c, subject)
		if errors.Is(e, jetstream.ErrMsgNotFound) && create {
			return data, 0, nil
		}
		if e != nil {
			return nil, 0, e
		}
		if msg.Subject != subject || msg.Sequence == 0 || !bytes.Equal(msg.Data, data) {
			return nil, 0, errors.New("invalid owner scope registration")
		}
		return data, msg.Sequence, nil
	}, func(c context.Context, seq uint64, value []byte) (uint64, error) {
		if e := p.validate(c); e != nil {
			return 0, e
		}
		m := &nats.Msg{Subject: subject, Data: value, Header: nats.Header{}}
		m.Header.Set(jetstream.ExpectedStreamHeader, p.name)
		m.Header.Set(jetstream.ExpectedLastSubjSeqHeader, fmt.Sprint(seq))
		m.Header.Set("Wf-Owner-Scope-Witness", "1")
		ack, e := p.js.PublishMsg(c, m)
		if e != nil {
			var api *jetstream.APIError
			if errors.As(e, &api) && (api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant) {
				return 0, ErrConflict
			}
			return 0, e
		}
		if ack == nil || ack.Stream != p.name || ack.Sequence <= seq || ack.Duplicate {
			return 0, errors.New("unexpected owner scope acknowledgment")
		}
		return ack.Sequence, nil
	})
	return err
}

func (p *OwnerIndexedNativePort) BlobKeysForOwner(ctx context.Context, owner string) ([]string, error) {
	return p.NativeAuthority.ownerScopeKeys(ctx, owner)
}
func (p *NativeAuthority) ownerScopeKeys(ctx context.Context, owner string) ([]string, error) {
	if !p.ownerIndexed || !validID(owner) {
		return nil, errors.New("invalid owner scope discovery")
	}
	if err := p.validate(ctx); err != nil {
		return nil, err
	}
	prefix := p.prefix + ".owner." + key([]byte(owner)) + "."
	// The SDK pages the filtered subject census through its response total.
	// State.NumSubjects counts the WHOLE stream and cannot bound this subset.
	// No staging under this token is allowed during discovery or renewal.
	info, err := p.stream.Info(ctx, jetstream.WithSubjectFilter(prefix+">"))
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(info.State.Subjects))
	for subject, count := range info.State.Subjects {
		k := strings.TrimPrefix(subject, prefix)
		if !strings.HasPrefix(subject, prefix) || !validHash(k) || count != 1 {
			return nil, errors.New("invalid owner scope census")
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := p.witnessOwnerScope(ctx, owner, k, false); err != nil {
			return nil, err
		}
	}
	return keys, nil
}
