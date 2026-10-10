package graphpublication

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

type renewalDropPort struct {
	Port
	token string
	drop  bool
}

func (p *renewalDropPort) CASBlob(ctx context.Context, k string, rev uint64, f Fence) (Record, error) {
	if p.drop && f.Owner == p.token {
		p.drop = false
		return Record{}, lostReply
	}
	return p.Port.CASBlob(ctx, k, rev, f)
}

func TestGraphCompactionIntentRenewalScopesAndFences(t *testing.T) {
	for _, mode := range []string{"normal", "inherited", "drop", "lost", "cancel", "expired", "expiry-read", "expiry-after-cas", "append", "append-during-cas", "reader", "collector", "collector-during-cas", "revoked", "wrong-expiry", "input-ownership", "result-ownership"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "renew", 12)
			if mode == "inherited" {
				first, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Hour), nil)
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.CommitPrefixCompaction(ctx, first)
				if err != nil {
					t.Fatal(err)
				}
			}
			oldExpiry, nextExpiry := epoch.Add(time.Minute), epoch.Add(3*time.Minute)
			plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, oldExpiry, []byte("cursor"))
			if err != nil {
				t.Fatal(err)
			}
			// A discarded staging branch can leave a grant that is absent from
			// the final forests. Its expiry still fences this publication head.
			_, err = retainedgraph.Append(ctx, stageStore{protocol: p, destination: "owner", expected: root.Head, expires: oldExpiry, token: plan.publication.Token, stream: PrefixArchiveStream}, retainedgraph.Empty(), retainedgraph.Record{Data: []byte("discarded-stage-branch")})
			if err != nil {
				t.Fatal(err)
			}
			original := copyCompactionRoot(root)
			before := map[string]Record{}
			owned := 0
			for k, record := range m.blobs {
				before[k] = Record{record.Revision, cloneFence(record.Fence)}
				if record.Fence.Owner == plan.publication.Token {
					owned++
				}
			}
			now := epoch.Add(20 * time.Second)
			clock := func() time.Time { return now }
			if mode == "expired" {
				now = oldExpiry
				if _, err = p.BeginCompactionIntentRenewal(ctx, plan, clock, nextExpiry); !errors.Is(err, ErrRevoked) {
					t.Fatal("expired intent revived", err)
				}
				return
			}
			renewal, err := p.BeginCompactionIntentRenewal(ctx, plan, clock, nextExpiry)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "input-ownership" {
				plan.base.Application = []byte("forged")
				plan.publication.Application[0] = 'X'
			}
			if mode == "drop" {
				fault := &renewalDropPort{Port: m, token: renewal.prepared.publication.Token, drop: true}
				renewal.protocol.Port = fault
			}
			if mode == "lost" {
				m.blobAfter = func(_ string, f Fence) error {
					if f.Owner == plan.publication.Token && f.Intents[f.Owner].Expires.Equal(nextExpiry) {
						m.blobAfter = nil
						return lostReply
					}
					return nil
				}
			}
			if mode == "expiry-read" {
				m.readBlobHook = func(k string) error {
					if m.blobs[k].Fence.Owner == plan.publication.Token {
						now = oldExpiry
					}
					return nil
				}
			}
			if mode == "append-during-cas" || mode == "collector-during-cas" || mode == "expiry-after-cas" {
				m.blobAfter = func(_ string, f Fence) error {
					if f.Owner != plan.publication.Token {
						return nil
					}
					m.blobAfter = nil
					switch mode {
					case "append-during-cas":
						appendOne(t, m, p, "owner", []byte("raced"), nil)
					case "collector-during-cas":
						_, err := p.SweepWithReaders(ctx, oldExpiry)
						return err
					case "expiry-after-cas":
						now = oldExpiry
					}
					return nil
				}
			}
			if mode == "append" {
				appendOne(t, m, p, "owner", []byte("raced"), nil)
			}
			if mode == "reader" {
				if _, _, err = p.AcquireReader(ctx, "owner", root.Head, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "collector" {
				if _, err = p.SweepWithReaders(ctx, epoch.Add(2*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "revoked" || mode == "wrong-expiry" {
				for k, record := range m.blobs {
					if record.Fence.Owner != plan.publication.Token {
						continue
					}
					if mode == "revoked" {
						record.Fence.Phase = "closed"
						record.Fence.Object = ""
						record.Fence.Intents = nil
					} else {
						intent := record.Fence.Intents[record.Fence.Owner]
						intent.Expires = epoch.Add(2 * time.Minute)
						record.Fence.Intents[record.Fence.Owner] = intent
					}
					record.Revision++
					m.blobs[k] = record
					break
				}
			}
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if _, done, err := renewal.Advance(cancelled, 3); done || !errors.Is(err, context.Canceled) || renewal.ExaminedScopes() != 0 {
					t.Fatal(done, err)
				}
			}
			var renewed PreparedCompaction
			var complete bool
			for calls := 0; !complete && calls <= len(before)+1; calls++ {
				prior := renewal.ExaminedScopes()
				renewed, complete, err = renewal.Advance(ctx, 3)
				if renewal.ExaminedScopes()-prior > 3 {
					t.Fatal("scope budget exceeded")
				}
				if err != nil {
					break
				}
				if !complete && renewed.destination != "" {
					t.Fatal("partial renewed plan escaped")
				}
			}
			negative := mode == "expiry-read" || mode == "expiry-after-cas" || mode == "append" || mode == "append-during-cas" || mode == "reader" || mode == "collector" || mode == "collector-during-cas" || mode == "revoked" || mode == "wrong-expiry"
			if mode == "drop" || mode == "lost" || negative {
				if err == nil || complete || renewed.destination != "" {
					t.Fatal("unsafe renewal accepted", complete, err)
				}
				if _, done, again := renewal.Advance(ctx, 3); done || again != err {
					t.Fatal("failed renewal reused", done, again, err)
				}
				if negative {
					return
				}
				// Reconcile only the exact new expiry with a fresh operation.
				renewal, err = p.BeginCompactionIntentRenewal(ctx, plan, clock, nextExpiry)
				if err != nil {
					t.Fatal(err)
				}
				for !complete {
					renewed, complete, err = renewal.Advance(ctx, 3)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if err != nil || !complete || renewal.RenewedScopes() != uint64(owned) || !renewed.expires.Equal(nextExpiry) {
				t.Fatal("renewal incomplete", complete, err, owned, renewal.RenewedScopes())
			}
			for k, record := range m.blobs {
				old := before[k]
				if record.Fence.Owner != renewed.publication.Token {
					if !reflect.DeepEqual(record, old) {
						t.Fatal("foreign scope changed")
					}
					continue
				}
				if !record.Fence.Intents[record.Fence.Owner].Expires.Equal(nextExpiry) {
					t.Fatal("intermediate scope not renewed")
				}
				record.Fence = cloneFence(record.Fence)
				intent := record.Fence.Intents[record.Fence.Owner]
				intent.Expires = oldExpiry
				record.Fence.Intents[record.Fence.Owner] = intent
				if !reflect.DeepEqual(record.Fence, old.Fence) {
					t.Fatal("renewal changed ownership or locations")
				}
			}
			now = oldExpiry.Add(time.Second)
			if _, err = p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, m.roots["owner"]) {
				t.Fatal("expired intermediate intent fenced source")
			}
			if mode == "result-ownership" {
				renewed.publication.Application[0] = 'X'
				renewed, complete, err = renewal.Advance(ctx, 3)
				if err != nil || !complete || string(renewed.publication.Application) != "cursor" {
					t.Fatal("result aliases renewal state", err)
				}
			}
			if mode == "normal" {
				if _, err := p.CommitPrefixCompaction(ctx, plan); !errors.Is(err, ErrRevoked) {
					t.Fatal("old-expiry plan accepted", err)
				}
			}
			published, err := p.CommitPrefixCompaction(ctx, renewed)
			if err != nil || published.Graph.Count != root.Graph.Count-5 {
				t.Fatal("renewed independent commit failed", err)
			}
			t.Logf("COMPACTION_RENEWAL mode=%s examined=%d renewed=%d scope_budget=3 root_unchanged_after_old_expiry=true", mode, renewal.ExaminedScopes(), renewal.RenewedScopes())
		})
	}
}

func TestGraphCompactionUnrenewedIntermediateScopesFenceSource(t *testing.T) {
	m, p, root := stageFixture(t, "renew-baseline", 12)
	plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, epoch.Add(time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if m.roots["owner"].Head == root.Head {
		t.Fatal("expired publication failed to fence")
	}
	if _, err = p.CommitPrefixCompaction(ctx, plan); !errors.Is(err, ErrConflict) {
		t.Fatal("expired plan published", err)
	}
}
