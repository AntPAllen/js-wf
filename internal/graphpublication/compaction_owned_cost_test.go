package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

type ownedCostScan struct {
	OwnerScopeScan
	cost       *renewalLatencyPort
	discovered uint64
}

func (s *ownedCostScan) Advance(c context.Context, max uint64) ([]string, bool, error) {
	keys, done, err := s.OwnerScopeScan.Advance(c, max)
	if err != nil {
		return nil, false, err
	}
	for range keys {
		s.cost.charge()
		s.discovered++
	}
	return keys, done, nil
}

type ownedCostPort struct {
	*ownerScanModelPort
	cursor *ownedCostScan
}

func (p *ownedCostPort) BeginOwnerScopeScan(c context.Context, owner string) (OwnerScopeScan, error) {
	scan, err := p.ownerScanModelPort.BeginOwnerScopeScan(c, owner)
	if err != nil {
		return nil, err
	}
	// One barrier and one count operation. Native implementations may use
	// several wire requests per modeled port call; this model is optimistic.
	p.latency.charge()
	p.latency.charge()
	p.cursor = &ownedCostScan{OwnerScopeScan: scan, cost: p.latency}
	return p.cursor, nil
}

func TestGraphCompactionOwnedRenewalCost(t *testing.T) {
	for _, seed := range []int64{2, 5, 42} {
		for _, tc := range []struct {
			name    string
			extra   int
			latency time.Duration
			failure bool
		}{
			{"owned1000-1ms", 1000, time.Millisecond, false},
			{"owned10000-1ms", 10000, time.Millisecond, true},
			{"owned10000-10us", 10000, 10 * time.Microsecond, false},
			{"owned100000-1ms", 100000, time.Millisecond, true},
			{"owned100000-10us", 100000, 10 * time.Microsecond, false},
		} {
			if tc.extra == 100000 && seed != 42 {
				continue
			}
			t.Run(fmt.Sprintf("seed%d/%s", seed, tc.name), func(t *testing.T) {
				m, p, root := stageFixture(t, "owned-cost", 12)
				index := &ownerScopeModelPort{Port: m, registry: map[string]map[string]bool{}}
				p.Port = index
				expires, nextExpiry := epoch.Add(time.Minute), epoch.Add(2*time.Minute)
				plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, expires, nil)
				if err != nil {
					t.Fatal(err)
				}
				token := plan.publication.Token
				// Abandoned real uploading grants remain expiry authority even
				// though the final forests cannot enumerate them. Provision via
				// the index port before charging renewal latency.
				for i := 0; i < tc.extra; i++ {
					h := key([]byte(fmt.Sprint("owned-orphan", i)))
					f := Fence{Hash: h, Owner: token, Generation: 1, Phase: "uploading", Intents: map[string]Intent{token: {Destination: "owner", Expected: root.Head, Expires: expires, Locations: []Location{{Kind: "payload"}}}}}
					if _, err = index.CASBlob(ctx, authorityKey(h, token), 0, f); err != nil {
						t.Fatal(err)
					}
				}
				start := epoch.Add(40 * time.Second)
				cost := &renewalLatencyPort{Port: m, now: start, rng: rand.New(rand.NewSource(seed)), latency: tc.latency}
				index.Port, index.latency = cost, cost
				model := &ownerScanModelPort{ownerWitnessModel: &ownerWitnessModel{ownerScopeModelPort: index}}
				for k := range index.registry[token] {
					model.index = append(model.index, k)
				}
				sort.Strings(model.index)
				port := &ownedCostPort{ownerScanModelPort: model}
				p.Port = port
				r, err := p.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return cost.now }, nextExpiry)
				if err != nil || len(r.keys) != 0 || port.validated != 0 {
					t.Fatal("setup not incremental", err)
				}
				var done bool
				var result PreparedCompaction
				for calls := 0; calls <= len(model.index)/128+1; calls++ {
					before := r.ExaminedScopes()
					result, done, err = r.Advance(ctx, 128)
					if r.ExaminedScopes()-before > 128 {
						t.Fatal("scope budget exceeded")
					}
					if err != nil || done {
						break
					}
				}
				if tc.failure {
					if done || !errors.Is(err, ErrRevoked) || result.destination != "" || cost.now.Before(expires) || r.ExaminedScopes() >= uint64(len(model.index)) {
						t.Fatal("expected owned-cost expiry", done, err, r.ExaminedScopes(), cost.now)
					}
					before, validated, discovered := *cost, port.validated, port.cursor.discovered
					if _, done, e := r.Advance(ctx, 128); done || e != err || !reflect.DeepEqual(before, *cost) || port.validated != validated || port.cursor.discovered != discovered {
						t.Fatal("expired renewal retried", done, e)
					}
					// The same old plan cannot revive its grants in a fresh scan.
					if fresh, e := p.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return cost.now }, nextExpiry); fresh != nil || !errors.Is(e, ErrRevoked) {
						t.Fatal("expired input revived", fresh, e)
					}
				} else if err != nil || !done || r.RenewedScopes() != uint64(len(model.index)) || !result.expires.Equal(nextExpiry) {
					t.Fatal("healthy owned scan incomplete", done, err, r.RenewedScopes())
				}
				ownedOld, ownedNew := 0, 0
				for k := range index.registry[token] {
					f := m.blobs[k].Fence
					switch f.Intents[token].Expires {
					case expires:
						ownedOld++
					case nextExpiry:
						ownedNew++
					default:
						t.Fatal("unexpected owned expiry", k)
					}
				}
				if ownedNew != int(r.RenewedScopes()) || (!tc.failure && ownedOld != 0) || !reflect.DeepEqual(root, m.roots["owner"]) {
					t.Fatal("grant accounting or publication", ownedOld, ownedNew, r.RenewedScopes())
				}
				t.Logf("OWNED_RENEWAL seed=%d scopes=%d latency=%s elapsed=%s discovered=%d examined=%d renewed=%d old_expiry=%d new_expiry=%d roots=%d blobs=%d writes=%d complete=%t err=%v", seed, len(model.index), tc.latency, cost.now.Sub(start), port.cursor.discovered, r.ExaminedScopes(), r.RenewedScopes(), ownedOld, ownedNew, cost.roots, cost.blobs, cost.writes, done, err)
			})
		}
	}
}
