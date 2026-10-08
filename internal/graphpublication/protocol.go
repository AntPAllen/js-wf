// Package graphpublication implements an experimental append-only graph
// ownership protocol. Native metadata is experimental; graph object storage
// and canonical runtime adoption are pending. It does not enable runtime GC.
package graphpublication

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"js-wf/internal/blobpublication"
	"js-wf/internal/retainedgraph"
)

const Schema = "js-wf-graph-publication-v1"
const MaxIntentLocations = 63 + retainedgraph.MaxBlobReferences
const MaxFenceBytes = 32 << 10

var ErrConflict = blobpublication.ErrConflict
var ErrRevoked = blobpublication.ErrRevoked

type Location struct {
	Kind   string
	First  uint64
	Height uint8
}
type Intent struct {
	Destination string
	Expected    uint64
	Expires     time.Time
	Locations   []Location
}
type Fence struct {
	Hash       string
	Owner      string
	Generation uint64
	Phase      string
	Object     string
	Intents    map[string]Intent
}
type Record struct {
	Revision uint64
	Fence    Fence
}
type Root struct {
	Schema string
	Head   uint64
	Token  string
	Graph  retainedgraph.Root
}

// Port provides linearizable quorum-witnessed reads and acknowledged CAS.
// CASRoot advances Head exactly once; blob revisions and generations never
// reset, even after retirement. Returned maps/slices are independent copies.
// Put uses new immutable names, never acknowledges uncertain upload outcomes,
// and has no delayed cleanup that can remove an acknowledged object. Get is
// context-bound and honors the byte limit. Missing objects are successful Delete.
// Enumeration must cover only this protocol's isolated metadata/object namespace.
// Blob keys identify permanent (content hash, publication token) authority
// scopes, not global content hashes. Each scope has at most one bounded grant;
// generations never reset within that scope. Fresh scopes do not reuse objects.
// The legacy direct-reference collector MUST NOT enumerate these objects.
// A native adapter needs a separate fail-closed schema rollout and retained
// high-water marks; implementing this interface does not qualify that rollout.
type Port interface {
	ReadBlob(context.Context, string) (Record, error)
	CASBlob(context.Context, string, uint64, Fence) (Record, error)
	ReadRoot(context.Context, string) (Root, error)
	CASRoot(context.Context, string, uint64, Root) (Root, error)
	BlobKeys(context.Context) ([]string, error)
	Put(context.Context, string, []byte) error
	Get(context.Context, retainedgraph.Link, int) ([]byte, error)
	Delete(context.Context, string) error
	Objects(context.Context) ([]blobpublication.Object, error)
}

// NewID must be globally unique across producers, retries and restarts.
type Protocol struct {
	Port  Port
	NewID func() (string, error)
}
type Prepared struct {
	destination string
	expected    uint64
	expires     time.Time
	base        Root
	publication Root
	owned       map[retainedgraph.Link]uint64
}

// OwnedPayload locates an exact payload edge in the destination's original
// canonical graph. A receipt or content hash alone does not establish ownership.
type OwnedPayload struct {
	Index uint64
	Link  retainedgraph.Link
}

func EmptyRoot() Root { return Root{Schema: Schema, Graph: retainedgraph.Empty()} }
func normalizeRoot(root Root) (Root, error) {
	if root.Schema == "" && root.Head == 0 && root.Token == "" && root.Graph.Schema == "" && root.Graph.Count == 0 && root.Graph.Frontier == nil {
		return EmptyRoot(), nil
	}
	if root.Schema != Schema || (root.Graph.Count > 0 && (root.Head == 0 || !validID(root.Token))) || (root.Token != "" && !validID(root.Token)) {
		return Root{}, errors.New("invalid graph authority root")
	}
	if err := root.Graph.Validate(); err != nil {
		return Root{}, err
	}
	if _, err := root.Graph.Encode(); err != nil {
		return Root{}, err
	}
	return root, nil
}
func validLocation(location Location) bool {
	if location.First >= math.MaxInt64 {
		return false
	}
	switch location.Kind {
	case "node":
		return location.Height <= 62 && location.First%(uint64(1)<<location.Height) == 0
	case "payload":
		return location.Height == 0
	default:
		return false
	}
}
func validateFence(k string, record Record) error {
	raw, err := hex.DecodeString(k)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != k {
		return errors.New("invalid graph object key")
	}
	f := record.Fence
	if record.Revision == 0 {
		if f.Hash != "" || f.Owner != "" || f.Generation != 0 || f.Phase != "" || f.Object != "" || len(f.Intents) != 0 {
			return errors.New("invalid absent fence")
		}
		return nil
	}
	hashBytes, hashErr := hex.DecodeString(f.Hash)
	if hashErr != nil || len(hashBytes) != 32 || hex.EncodeToString(hashBytes) != f.Hash || !validID(f.Owner) || authorityKey(f.Hash, f.Owner) != k || len(f.Intents) > 1 {
		return errors.New("invalid scoped graph authority")
	}
	if f.Generation == 0 {
		return errors.New("missing graph generation")
	}
	switch f.Phase {
	case "closed":
		if f.Object != "" || len(f.Intents) != 0 {
			return errors.New("invalid closed fence")
		}
	case "uploading":
		if f.Object != "" || len(f.Intents) == 0 {
			return errors.New("invalid uploading fence")
		}
	case "ready":
		object := blobpublication.Object{Key: f.Hash, Reference: blobpublication.Reference{Generation: f.Generation, Object: f.Object}}
		_, owner, identityErr := objectAuthority(object)
		if identityErr != nil || owner != f.Owner || !validObject(object) || len(f.Intents) == 0 {
			return errors.New("invalid ready fence")
		}
	default:
		return errors.New("invalid graph fence phase")
	}
	for token, intent := range f.Intents {
		if token != f.Owner {
			return errors.New("foreign graph intent owner")
		}
		if !validID(token) || intent.Destination == "" || len(intent.Destination) > 256 || intent.Expected == math.MaxUint64 || intent.Expires.IsZero() || len(intent.Locations) == 0 || len(intent.Locations) > MaxIntentLocations {
			return errors.New("invalid graph intent")
		}
		seen := map[Location]bool{}
		for _, location := range intent.Locations {
			if !validLocation(location) || seen[location] {
				return errors.New("invalid intent location")
			}
			seen[location] = true
		}
	}
	encoded, err := json.Marshal(f)
	if err != nil || len(encoded) > MaxFenceBytes {
		return errors.New("graph authority encoding or byte limit")
	}
	return nil
}

func (p Protocol) id() (string, error) {
	if p.NewID != nil {
		return p.NewID()
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func key(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func authorityKey(hash, token string) string {
	return key([]byte("graph-authority/" + hash + "/" + token))
}
func objectName(k string, gen uint64, token, id string) string {
	return k + "/" + strconv.FormatUint(gen, 10) + "/" + hex.EncodeToString([]byte(token)) + "-" + hex.EncodeToString([]byte(id))
}
func objectAuthority(o blobpublication.Object) (string, string, error) {
	if !o.Reference.ValidFor(o.Key) || len(o.Reference.Object) > 256 {
		return "", "", errors.New("invalid graph physical identity")
	}
	parts := strings.Split(o.Reference.Object, "/")
	suffix := strings.Split(parts[2], "-")
	if len(suffix) != 2 {
		return "", "", errors.New("invalid graph attempt name")
	}
	owner, err := hex.DecodeString(suffix[0])
	if err != nil || !validID(string(owner)) || hex.EncodeToString(owner) != suffix[0] {
		return "", "", errors.New("invalid graph owner identity")
	}
	upload, err := hex.DecodeString(suffix[1])
	if err != nil || !validID(string(upload)) || hex.EncodeToString(upload) != suffix[1] {
		return "", "", errors.New("invalid graph upload identity")
	}
	return authorityKey(o.Key, string(owner)), string(owner), nil
}
func validID(id string) bool {
	if id == "" || len(id) > 32 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func validObject(o blobpublication.Object) bool {
	raw, err := hex.DecodeString(o.Key)
	if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != o.Key {
		return false
	}
	_, _, err = objectAuthority(o)
	return err == nil
}

func (p Protocol) acquire(ctx context.Context, hash string, data []byte, token string, intent Intent) (blobpublication.Reference, error) {
	k := authorityKey(hash, token)
	for attempts := 0; attempts < 32; attempts++ {
		record, err := p.Port.ReadBlob(ctx, k)
		if err != nil {
			return blobpublication.Reference{}, err
		}
		if err = validateFence(k, record); err != nil {
			return blobpublication.Reference{}, err
		}
		f := record.Fence
		if record.Revision == 0 || f.Phase == "closed" {
			if f.Generation == ^uint64(0) {
				return blobpublication.Reference{}, errors.New("generation exhausted")
			}
			f = Fence{Hash: hash, Owner: token, Generation: f.Generation + 1, Phase: "uploading", Intents: map[string]Intent{}}
		}
		if f.Phase != "uploading" && f.Phase != "ready" {
			return blobpublication.Reference{}, errors.New("invalid blob phase")
		}
		if f.Intents == nil {
			f.Intents = map[string]Intent{}
		}
		if prior, exists := f.Intents[token]; exists {
			if prior.Destination != intent.Destination || prior.Expected != intent.Expected || !prior.Expires.Equal(intent.Expires) {
				return blobpublication.Reference{}, errors.New("publication token reused")
			}
			merged := append([]Location{}, prior.Locations...)
			for _, location := range intent.Locations {
				found := false
				for _, old := range merged {
					if old == location {
						found = true
						break
					}
				}
				if !found {
					merged = append(merged, location)
				}
			}
			if len(merged) > MaxIntentLocations {
				return blobpublication.Reference{}, errors.New("intent location limit")
			}
			intent.Locations = merged
		}
		f.Intents[token] = intent
		record, err = p.casBlob(ctx, k, record.Revision, f)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return blobpublication.Reference{}, err
		}
		if record.Fence.Phase == "ready" {
			return blobpublication.Reference{record.Fence.Generation, record.Fence.Object}, nil
		}
		id, err := p.id()
		if err != nil {
			return blobpublication.Reference{}, err
		}
		if !validID(id) {
			return blobpublication.Reference{}, errors.New("invalid upload ID")
		}
		object := objectName(hash, record.Fence.Generation, token, id)
		if err = p.Port.Put(ctx, object, data); err != nil {
			return blobpublication.Reference{}, err
		}
		// A concurrent uploader may have chosen another physical attempt. Never
		// overwrite its selection, and never resurrect a closed generation.
		for retries := 0; retries < 32; retries++ {
			current, err := p.Port.ReadBlob(ctx, k)
			if err != nil {
				return blobpublication.Reference{}, err
			}
			if err = validateFence(k, current); err != nil {
				return blobpublication.Reference{}, err
			}
			if current.Fence.Generation != record.Fence.Generation {
				return blobpublication.Reference{}, ErrRevoked
			}
			if _, exists := current.Fence.Intents[token]; !exists {
				return blobpublication.Reference{}, ErrRevoked
			}
			if current.Fence.Phase == "ready" {
				return blobpublication.Reference{current.Fence.Generation, current.Fence.Object}, nil
			}
			if current.Fence.Phase != "uploading" {
				return blobpublication.Reference{}, ErrRevoked
			}
			next := current.Fence
			next.Phase = "ready"
			next.Object = object
			_, err = p.casBlob(ctx, k, current.Revision, next)
			if errors.Is(err, ErrConflict) {
				continue
			}
			if err != nil {
				return blobpublication.Reference{}, err
			}
			return blobpublication.Reference{next.Generation, object}, nil
		}
		return blobpublication.Reference{}, ErrConflict
	}
	return blobpublication.Reference{}, ErrConflict
}

// Sweep closes unreferenced generations before deleting physical objects. A
// late old upload can create an orphan, but cannot publish it; the next sweep
// removes it. A late delete cannot touch a later generation's unique names.
func (p Protocol) Sweep(ctx context.Context, now time.Time) (int, error) {
	if p.Port == nil || now.IsZero() {
		return 0, errors.New("port and collection time required")
	}
	keys, err := p.Port.BlobKeys(ctx)
	if err != nil {
		return 0, err
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err = p.sweepKey(ctx, k, now); err != nil {
			return 0, err
		}
	}
	objects, err := p.Port.Objects(ctx)
	if err != nil {
		return 0, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Reference.Object < objects[j].Reference.Object })
	deleted := 0
	for _, o := range objects {
		if !validObject(o) {
			return deleted, fmt.Errorf("invalid physical blob identity: %q", o.Reference.Object)
		}
		scope, _, err := objectAuthority(o)
		if err != nil {
			return deleted, err
		}
		record, err := p.Port.ReadBlob(ctx, scope)
		if err != nil {
			return deleted, err
		}
		if err = validateFence(scope, record); err != nil {
			return deleted, err
		}
		f := record.Fence
		// Fail closed for absent or future metadata. Uploading generations protect
		// all physical attempts until the winner is selected or the intents expire.
		if record.Revision == 0 || o.Reference.Generation > f.Generation {
			return deleted, errors.New("missing blob generation fence")
		}
		if o.Reference.Generation == f.Generation && f.Phase != "closed" {
			if f.Phase == "uploading" || (f.Phase == "ready" && o.Reference.Object == f.Object) {
				continue
			}
			if f.Phase != "ready" {
				return deleted, errors.New("invalid blob phase")
			}
		}
		if err = p.Port.Delete(ctx, o.Reference.Object); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
