package graphpublication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

// This transport advances authority time without sleeping or contacting NATS.
// It charges even successful requests; faults are unnecessary to expose the
// namespace-wide renewal cost. Jitter is deterministic for a seed and trace.
type renewalLatencyPort struct {
	Port
	now                          time.Time
	rng                          *rand.Rand
	latency                      time.Duration
	roots, blobs, writes, census uint64
}

func (p *renewalLatencyPort) charge() {
	p.now = p.now.Add(p.latency/2 + time.Duration(p.rng.Int63n(int64(p.latency)+1)))
}
func (p *renewalLatencyPort) ReadRoot(c context.Context, k string) (Root, error) {
	p.roots++
	p.charge()
	return p.Port.ReadRoot(c, k)
}
func (p *renewalLatencyPort) ReadBlob(c context.Context, k string) (Record, error) {
	p.blobs++
	p.charge()
	return p.Port.ReadBlob(c, k)
}
func (p *renewalLatencyPort) CASBlob(c context.Context, k string, rev uint64, f Fence) (Record, error) {
	p.writes++
	p.charge()
	return p.Port.CASBlob(c, k, rev, f)
}
func (p *renewalLatencyPort) BlobKeys(c context.Context) ([]string, error) {
	p.census++
	p.charge()
	return p.Port.BlobKeys(c)
}

func TestGraphCompactionIntentRenewalNamespaceLatency(t *testing.T) {
	for _, seed := range []int64{2, 5, 42} {
		for _, tc := range []struct {
			name    string
			foreign int
			latency time.Duration
			failure bool
		}{
			{"small-1ms", 0, time.Millisecond, false},
			{"foreign100000-1ms", 100000, time.Millisecond, true},
			{"foreign100000-10us", 100000, 10 * time.Microsecond, false},
		} {
			t.Run(fmt.Sprintf("seed%d/%s", seed, tc.name), func(t *testing.T) {
				m, protocol, root := stageFixture(t, "latency", 12)
				oldExpiry, newExpiry := epoch.Add(time.Minute), epoch.Add(2*time.Minute)
				plan, err := protocol.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, oldExpiry, nil)
				if err != nil {
					t.Fatal(err)
				}
				// Permanent closed scopes are realistic retained high-water marks.
				// They require no object data, but renewal still reads each one.
				foreign := make(map[string]Record, tc.foreign)
				for i := 0; i < tc.foreign; i++ {
					hash := sha256.Sum256([]byte(fmt.Sprintf("foreign-%d", i)))
					h := hex.EncodeToString(hash[:])
					k := authorityKey(h, "foreign-owner")
					r := Record{Revision: 1, Fence: Fence{Hash: h, Owner: "foreign-owner", Generation: 1, Phase: "closed"}}
					if err := validateFence(k, r); err != nil {
						t.Fatal(err)
					}
					m.blobs[k], foreign[k] = r, r
				}
				start := epoch.Add(40 * time.Second) // default trigger: 20s remaining
				port := &renewalLatencyPort{Port: m, now: start, rng: rand.New(rand.NewSource(seed)), latency: tc.latency}
				protocol.Port = port
				renewal, err := protocol.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return port.now }, newExpiry)
				if err != nil {
					t.Fatal(err)
				}
				var result PreparedCompaction
				var done bool
				for calls := 0; calls <= len(m.blobs)/128+1; calls++ {
					prior := renewal.ExaminedScopes()
					result, done, err = renewal.Advance(ctx, 128)
					if renewal.ExaminedScopes()-prior > 128 {
						t.Fatal("scope budget exceeded")
					}
					if done || err != nil {
						break
					}
				}
				if tc.failure {
					if done || !errors.Is(err, ErrRevoked) || result.destination != "" || port.now.Before(oldExpiry) || renewal.ExaminedScopes() >= uint64(len(m.blobs)) {
						t.Fatal("expected namespace-cost expiry", done, err, port.now, renewal.ExaminedScopes())
					}
					before := *port
					if _, done, again := renewal.Advance(ctx, 128); done || again != err || !reflect.DeepEqual(before, *port) {
						t.Fatal("failed renewal retried transport", done, again)
					}
				} else if err != nil || !done || !result.expires.Equal(newExpiry) || renewal.ExaminedScopes() != uint64(len(m.blobs)) {
					t.Fatal("healthy renewal failed", done, err, renewal.ExaminedScopes())
				}
				if !reflect.DeepEqual(root, m.roots["owner"]) || port.census != 1 {
					t.Fatal("renewal published or repeated namespace census")
				}
				for k, r := range foreign {
					if !reflect.DeepEqual(r, m.blobs[k]) {
						t.Fatal("foreign scope changed", k)
					}
				}
				for _, r := range m.blobs {
					if r.Fence.Owner != plan.publication.Token {
						continue
					}
					expires := r.Fence.Intents[r.Fence.Owner].Expires
					if (!expires.Equal(oldExpiry) && !expires.Equal(newExpiry)) || (!tc.failure && !expires.Equal(newExpiry)) {
						t.Fatal("unexpected owned expiry", expires)
					}
				}
				t.Logf("RENEWAL_LATENCY seed=%d foreign=%d latency=%s elapsed=%s scopes=%d examined=%d renewed=%d roots=%d blobs=%d writes=%d census=%d complete=%t err=%v", seed, tc.foreign, tc.latency, port.now.Sub(start), len(m.blobs), renewal.ExaminedScopes(), renewal.RenewedScopes(), port.roots, port.blobs, port.writes, port.census, done, err)
			})
		}
	}
}
