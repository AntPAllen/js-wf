package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type mutationWitnessInterference struct {
	jetstream.JetStream
	subject   string
	remaining int
	attempts  int
	interfere func(context.Context) error
}

func (p *mutationWitnessInterference) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if m.Subject == p.subject && m.Header.Get("Wf-Authority-Read-Witness") != "1" {
		p.attempts++
		if p.remaining > 0 {
			p.remaining--
			if err := p.interfere(ctx); err != nil {
				return nil, err
			}
		}
	}
	return p.JetStream.PublishMsg(ctx, m, opts...)
}

// The intervening reads and writes execute on actual servers. A physical
// witness changes no logical revision; a peer mutation must still reject the
// originally prepared mutation rather than adopt its replacement authority.
func TestNativeGraphMutationAcrossReadWitnesses(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, kind := range []string{"root", "blob"} {
			for _, mode := range []string{"witness", "replacement"} {
				t.Run(fmt.Sprintf("R%d/%s/%s", replicas, kind, mode), func(t *testing.T) {
					_, authority, ctx := nativeGraphFixture(t, replicas)
					peer := *authority
					identity := "mutation"
					initial := EmptyRoot()
					initial.Schema = ApplicationSchema
					initial.Application = []byte("base")
					root, err := authority.CASRoot(ctx, identity, 0, initial)
					if err != nil {
						t.Fatal(err)
					}
					hash := key([]byte("payload"))
					owner := "owner"
					scope := authorityKey(hash, owner)
					fence := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]Intent{owner: {Destination: "mutation", Expected: 1, Expires: time.Now().UTC().Add(time.Hour), Locations: []Location{{Kind: "payload", First: 0}}}}}
					record, err := authority.CASBlob(ctx, scope, 0, fence)
					if err != nil {
						t.Fatal(err)
					}
					if kind == "blob" {
						identity = scope
					}
					fault := &mutationWitnessInterference{JetStream: authority.js, subject: authority.subject(kind, identity), remaining: 3}
					if mode == "replacement" {
						fault.remaining = 1
					}
					var replacement Root
					var replacementBlob Record
					fault.interfere = func(c context.Context) error {
						if kind == "root" {
							if mode == "witness" {
								_, e := peer.ReadRoot(c, identity)
								return e
							}
							next := root
							next.Application = []byte("peer")
							replacement, err = peer.CASRoot(c, identity, root.Head, next)
							return err
						}
						if mode == "witness" {
							_, e := peer.ReadBlob(c, identity)
							return e
						}
						next := record.Fence
						next.Phase = "closed"
						next.Intents = nil
						replacementBlob, err = peer.CASBlob(c, identity, record.Revision, next)
						return err
					}
					authority.js = fault
					if kind == "root" {
						next := root
						next.Application = []byte("ours")
						result, e := authority.CASRoot(ctx, identity, root.Head, next)
						if mode == "witness" {
							if e != nil || result.Head != root.Head+1 || string(result.Application) != "ours" {
								t.Fatalf("unchanged logical root rejected: result=%+v error=%v attempts=%d", result, e, fault.attempts)
							}
						} else {
							if !errors.Is(e, ErrConflict) || !reflect.DeepEqual(result, Root{}) || fault.attempts != 1 {
								t.Fatal("replacement root adopted/retried", result, e, fault.attempts)
							}
							current, e := peer.ReadRoot(ctx, identity)
							if e != nil || !reflect.DeepEqual(current, replacement) {
								t.Fatal(current, e)
							}
						}
					} else {
						next := record.Fence
						next.Phase = "closed"
						next.Intents = nil
						result, e := authority.CASBlob(ctx, identity, record.Revision, next)
						if mode == "witness" {
							if e != nil || result.Revision != record.Revision+1 || !reflect.DeepEqual(result.Fence, next) {
								t.Fatalf("unchanged logical blob rejected: result=%+v error=%v attempts=%d", result, e, fault.attempts)
							}
						} else {
							if !errors.Is(e, ErrConflict) || !reflect.DeepEqual(result, Record{}) || fault.attempts != 1 {
								t.Fatal("replacement blob adopted/retried", result, e, fault.attempts)
							}
							current, e := peer.ReadBlob(ctx, identity)
							if e != nil || !reflect.DeepEqual(current, replacementBlob) {
								t.Fatal(current, e)
							}
						}
					}
					if mode == "witness" && fault.attempts != 4 {
						t.Fatal("physical witness retry count", fault.attempts)
					}
					t.Logf("mutation kind=%s mode=%s publication_attempts=%d", kind, mode, fault.attempts)
				})
			}
		}
	}
}

func TestNativeGraphAbsentMutationAcrossReadWitnesses(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, kind := range []string{"root", "blob"} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, kind), func(t *testing.T) {
				_, authority, ctx := nativeGraphFixture(t, replicas)
				peer := *authority
				identity := "absent-mutation"
				hash := key([]byte("absent-payload"))
				owner := "owner"
				fence := Fence{Hash: hash, Owner: owner, Generation: 1, Phase: "uploading", Intents: map[string]Intent{owner: {Destination: identity, Expected: 0, Expires: time.Now().UTC().Add(time.Hour), Locations: []Location{{Kind: "payload", First: 0}}}}}
				if kind == "blob" {
					identity = authorityKey(hash, owner)
				}
				fault := &mutationWitnessInterference{JetStream: authority.js, subject: authority.subject(kind, identity), remaining: 3}
				fault.interfere = func(c context.Context) error {
					if kind == "root" {
						root, e := peer.ReadRoot(c, identity)
						if e == nil && root.Head != 0 {
							return fmt.Errorf("unexpected logical head %d", root.Head)
						}
						return e
					}
					record, e := peer.ReadBlob(c, identity)
					if e == nil && record.Revision != 0 {
						return fmt.Errorf("unexpected logical revision %d", record.Revision)
					}
					return e
				}
				authority.js = fault
				if kind == "root" {
					next := EmptyRoot()
					result, e := authority.CASRoot(ctx, identity, 0, next)
					if e != nil || result.Head != 1 {
						t.Fatal(result, e)
					}
				} else {
					result, e := authority.CASBlob(ctx, identity, 0, fence)
					if e != nil || result.Revision != 1 || !reflect.DeepEqual(result.Fence, fence) {
						t.Fatal(result, e)
					}
				}
				if fault.attempts != 4 {
					t.Fatal("absence witness attempts", fault.attempts)
				}
				t.Logf("absence kind=%s attempts=%d logical_revision=1", kind, fault.attempts)
			})
		}
	}
}

func TestNativeGraphMutationRetryStops(t *testing.T) {
	for _, mode := range []string{"exhaustion", "wrapped-conflict"} {
		t.Run(mode, func(t *testing.T) {
			_, authority, ctx := nativeGraphFixture(t, 1)
			peer := *authority
			identity := "retry-stop"
			uncertain := fmt.Errorf("uncertain transport outcome: %w", ErrConflict)
			fault := &mutationWitnessInterference{JetStream: authority.js, subject: authority.subject("root", identity), remaining: 17}
			fault.interfere = func(c context.Context) error {
				if mode == "wrapped-conflict" {
					return uncertain
				}
				_, err := peer.ReadRoot(c, identity)
				return err
			}
			authority.js = fault
			result, err := authority.CASRoot(ctx, identity, 0, EmptyRoot())
			wantAttempts := 16
			if mode == "wrapped-conflict" {
				wantAttempts = 1
				if err != uncertain {
					t.Fatal("uncertain result changed", err)
				}
			} else if err != ErrConflict {
				t.Fatal("exhaustion result", err)
			}
			if !reflect.DeepEqual(result, Root{}) || fault.attempts != wantAttempts {
				t.Fatal("retry bound or failed result", result, fault.attempts)
			}
			current, err := peer.ReadRoot(ctx, identity)
			if err != nil || current.Head != 0 {
				t.Fatal("failed mutation changed authority", current, err)
			}
			t.Logf("mode=%s attempts=%d logical_head=0", mode, fault.attempts)
		})
	}
}
