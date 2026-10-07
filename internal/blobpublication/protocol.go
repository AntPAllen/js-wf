// Package blobpublication implements the experimental decision protocol for
// online blob reclamation. It is not wired into the runtime's NATS adapters.
package blobpublication

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

var ErrConflict = errors.New("blob publication CAS conflict")
var ErrRevoked = errors.New("blob publication revoked")

// Reference identifies one immutable physical upload, not a reusable hash key.
type Reference struct {
	Generation uint64
	Object     string
}
type Intent struct {
	Root     string
	Expected uint64
	Expires  time.Time
}
type Fence struct {
	Generation uint64
	Phase      string // uploading, ready, or closed; closed retains the generation forever
	Object     string
	Intents    map[string]Intent // publication token -> destination and original head
}
type Record struct {
	Revision uint64
	Fence    Fence
}
type Root struct {
	Head  uint64
	Token string
	Blobs map[string]Reference
	Data  []byte
}
type Object struct {
	Key       string
	Reference Reference
}

// Port requires linearizable reads and CAS. Every successful CASRoot advances
// Head by exactly one, including a fence that preserves the publication. Heads
// and blob generations must NEVER reset after deletion, purge, TTL or restart.
// Metadata is retained without TTL. Returned maps/bytes must be independent
// copies. ErrConflict means no write; all other errors may be ambiguous.
// Put must target a new, immutable physical name on each invocation; an adapter
// must not reuse that name for a later retry or report success before its own
// delayed cleanup can no longer delete that successful upload. List operations
// cover this protocol's objects only. Missing objects are a successful Delete.
type Port interface {
	ReadBlob(context.Context, string) (Record, error)
	CASBlob(context.Context, string, uint64, Fence) (Record, error)
	ReadRoot(context.Context, string) (Root, error)
	CASRoot(context.Context, string, uint64, Root) (Root, error)
	Put(context.Context, string, []byte) error
	Delete(context.Context, string) error
	BlobKeys(context.Context) ([]string, error)
	Objects(context.Context) ([]Object, error)
}

// NewID, when supplied, must issue a fresh globally unique ID for every call,
// including after crashes and across producers. The default uses crypto/rand.
type Protocol struct {
	Port  Port
	NewID func() (string, error)
}

// Prepared is opaque so callers cannot change destinations or references after
// their intentions have been registered.
type Prepared struct {
	destination string
	expected    uint64
	publication Root
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
func objectName(k string, gen uint64, id string) string {
	return k + "/" + strconv.FormatUint(gen, 10) + "/" + id
}
func validID(id string) bool {
	if id == "" {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func validObject(o Object) bool {
	b, err := hex.DecodeString(o.Key)
	prefix := o.Key + "/" + strconv.FormatUint(o.Reference.Generation, 10) + "/"
	return err == nil && len(b) == 32 && o.Reference.Generation > 0 && strings.HasPrefix(o.Reference.Object, prefix) && validID(strings.TrimPrefix(o.Reference.Object, prefix))
}

// Prepare registers expiring intentions before uploading. The returned value
// may be paused arbitrarily; Commit then either publishes atomically or fails.
// Partial preparations and ambiguous uploads are abandoned, never promoted by
// merely observing bytes. Their intents expire and later Sweep reclaims them.
func (p Protocol) Prepare(ctx context.Context, destination string, payload []byte, blobs [][]byte, expires time.Time) (Prepared, error) {
	if destination == "" || expires.IsZero() {
		return Prepared{}, errors.New("destination and expiration required")
	}
	root, err := p.Port.ReadRoot(ctx, destination)
	if err != nil {
		return Prepared{}, err
	}
	token, err := p.id()
	if err != nil {
		return Prepared{}, err
	}
	if !validID(token) {
		return Prepared{}, errors.New("invalid publication ID")
	}
	result := Prepared{destination: destination, expected: root.Head, publication: Root{Token: token, Blobs: map[string]Reference{}, Data: append([]byte(nil), payload...)}}
	for _, data := range blobs {
		k := key(data)
		if _, exists := result.publication.Blobs[k]; exists {
			continue
		}
		ref, err := p.acquire(ctx, k, data, token, Intent{Root: destination, Expected: root.Head, Expires: expires})
		if err != nil {
			return Prepared{}, err
		}
		result.publication.Blobs[k] = ref
	}
	return result, nil
}
func (p Protocol) acquire(ctx context.Context, k string, data []byte, token string, intent Intent) (Reference, error) {
	for attempts := 0; attempts < 32; attempts++ {
		record, err := p.Port.ReadBlob(ctx, k)
		if err != nil {
			return Reference{}, err
		}
		f := record.Fence
		if record.Revision == 0 || f.Phase == "closed" {
			if f.Generation == ^uint64(0) {
				return Reference{}, errors.New("generation exhausted")
			}
			f = Fence{Generation: f.Generation + 1, Phase: "uploading", Intents: map[string]Intent{}}
		}
		if f.Phase != "uploading" && f.Phase != "ready" {
			return Reference{}, errors.New("invalid blob phase")
		}
		if f.Intents == nil {
			f.Intents = map[string]Intent{}
		}
		if prior, exists := f.Intents[token]; exists && prior != intent {
			return Reference{}, errors.New("publication token reused")
		}
		f.Intents[token] = intent
		record, err = p.Port.CASBlob(ctx, k, record.Revision, f)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return Reference{}, err
		}
		if record.Fence.Phase == "ready" {
			return Reference{record.Fence.Generation, record.Fence.Object}, nil
		}
		id, err := p.id()
		if err != nil {
			return Reference{}, err
		}
		if !validID(id) {
			return Reference{}, errors.New("invalid upload ID")
		}
		object := objectName(k, record.Fence.Generation, id)
		if err = p.Port.Put(ctx, object, data); err != nil {
			return Reference{}, err
		}
		// A concurrent uploader may have chosen another physical attempt. Never
		// overwrite its selection, and never resurrect a closed generation.
		for retries := 0; retries < 32; retries++ {
			current, err := p.Port.ReadBlob(ctx, k)
			if err != nil {
				return Reference{}, err
			}
			if current.Fence.Generation != record.Fence.Generation {
				return Reference{}, ErrRevoked
			}
			if _, exists := current.Fence.Intents[token]; !exists {
				return Reference{}, ErrRevoked
			}
			if current.Fence.Phase == "ready" {
				return Reference{current.Fence.Generation, current.Fence.Object}, nil
			}
			if current.Fence.Phase != "uploading" {
				return Reference{}, ErrRevoked
			}
			next := current.Fence
			next.Phase = "ready"
			next.Object = object
			_, err = p.Port.CASBlob(ctx, k, current.Revision, next)
			if errors.Is(err, ErrConflict) {
				continue
			}
			if err != nil {
				return Reference{}, err
			}
			return Reference{next.Generation, object}, nil
		}
		return Reference{}, ErrConflict
	}
	return Reference{}, ErrConflict
}

// Commit cannot outrun GC's destination fence. On a lost reply it confirms only
// an exact immutable publication at the destination, not an object's presence.
func (p Protocol) Commit(ctx context.Context, prepared Prepared) (Root, error) {
	if prepared.destination == "" || !validID(prepared.publication.Token) {
		return Root{}, errors.New("invalid prepared publication")
	}
	root, err := p.Port.CASRoot(ctx, prepared.destination, prepared.expected, prepared.publication)
	if err == nil {
		return root, nil
	}
	current, readErr := p.Port.ReadRoot(ctx, prepared.destination)
	if readErr == nil && current.Token == prepared.publication.Token && reflect.DeepEqual(current.Blobs, prepared.publication.Blobs) && reflect.DeepEqual(current.Data, prepared.publication.Data) {
		return current, nil
	}
	return Root{}, err
}

// Retire clears a publication while retaining its monotonically advancing head.
func (p Protocol) Retire(ctx context.Context, destination string, expected uint64) error {
	_, err := p.Port.CASRoot(ctx, destination, expected, Root{})
	return err
}

func protects(root Root, token, k string, f Fence) bool {
	ref, ok := root.Blobs[k]
	return ok && root.Token == token && ref.Generation == f.Generation && ref.Object == f.Object && f.Phase == "ready"
}
func (p Protocol) sweepKey(ctx context.Context, k string, now time.Time) error {
	for attempts := 0; attempts < 32; attempts++ {
		record, err := p.Port.ReadBlob(ctx, k)
		if err != nil {
			return err
		}
		f := record.Fence
		if record.Revision == 0 || f.Phase == "closed" {
			return nil
		}
		changed := false
		tokens := make([]string, 0, len(f.Intents))
		for token := range f.Intents {
			tokens = append(tokens, token)
		}
		sort.Strings(tokens)
		for _, token := range tokens {
			intent := f.Intents[token]
			root, err := p.Port.ReadRoot(ctx, intent.Root)
			if err != nil {
				return err
			}
			if protects(root, token, k, f) {
				continue
			}
			if root.Head < intent.Expected {
				return errors.New("destination head regressed")
			}
			if root.Head == intent.Expected {
				if now.Before(intent.Expires) {
					continue
				}
				// Preserve an existing publication when fencing a pending replacement.
				// An ambiguous outcome leaves the intent in place for a later sweep.
				_, err = p.Port.CASRoot(ctx, intent.Root, root.Head, root)
				if errors.Is(err, ErrConflict) {
					changed = true
					break
				}
				if err != nil {
					return err
				}
			}
			delete(f.Intents, token)
			changed = true
		}
		if len(f.Intents) == 0 {
			f.Phase = "closed"
			f.Object = ""
			changed = true
		}
		if !changed {
			return nil
		}
		_, err = p.Port.CASBlob(ctx, k, record.Revision, f)
		if errors.Is(err, ErrConflict) {
			continue
		}
		return err
	}
	return ErrConflict
}

// Sweep closes unreferenced generations before deleting physical objects. A
// late old upload can create an orphan, but cannot publish it; the next sweep
// removes it. A late delete cannot touch a later generation's unique names.
func (p Protocol) Sweep(ctx context.Context, now time.Time) (int, error) {
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
		record, err := p.Port.ReadBlob(ctx, o.Key)
		if err != nil {
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
