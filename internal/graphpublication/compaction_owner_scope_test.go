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

// The registry models authoritative storage, shared by fresh adapter instances.
// Register-before-CAS preserves reservations when writes fail or lose replies.
// It is never reconstructed from a publication's final reachable forest.
type ownerScopeModelPort struct {
	Port
	registry map[string]map[string]bool
	listed   int
	fault    func([]string) ([]string, error)
	latency  *renewalLatencyPort
}

func (p *ownerScopeModelPort) CASBlob(c context.Context, k string, rev uint64, f Fence) (Record, error) {
	if err := c.Err(); err != nil {
		return Record{}, err
	}
	if p.registry[f.Owner] == nil {
		p.registry[f.Owner] = map[string]bool{}
	}
	p.registry[f.Owner][k] = true
	return p.Port.CASBlob(c, k, rev, f)
}
func (p *ownerScopeModelPort) BlobKeysForOwner(c context.Context, token string) ([]string, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	p.listed++
	if p.latency != nil {
		p.latency.census++
		p.latency.charge()
	}
	keys := []string{}
	for k := range p.registry[token] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if p.fault != nil {
		return p.fault(keys)
	}
	return keys, nil
}

func TestGraphCompactionOwnerScopeDiscovery(t *testing.T) {
	for _, mode := range []string{"normal", "foreign100000", "abandoned", "unknown-create", "absent-reservation", "fresh-adapter", "unknown-enumeration", "duplicate", "foreign-entry", "invalid-key", "source-change"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "owner-index", 12)
			index := &ownerScopeModelPort{Port: m, registry: map[string]map[string]bool{}}
			p.Port = index
			oldExpiry, newExpiry := epoch.Add(time.Minute), epoch.Add(2*time.Minute)
			plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, oldExpiry, nil)
			if err != nil {
				t.Fatal(err)
			}
			token := plan.publication.Token
			orphan := authorityKey(key([]byte("orphan")), token)
			if mode == "abandoned" || mode == "unknown-create" || mode == "absent-reservation" {
				f := Fence{Hash: key([]byte("orphan")), Owner: token, Generation: 1, Phase: "uploading", Intents: map[string]Intent{token: {Destination: "owner", Expected: root.Head, Expires: oldExpiry, Locations: []Location{{Kind: "payload", First: 0}}}}}
				if mode == "unknown-create" {
					m.blobAfter = func(string, Fence) error { return lostReply }
				}
				if mode == "absent-reservation" {
					// Force rejection before mutation, after index registration.
					_, err = index.CASBlob(ctx, orphan, 1, f)
				} else {
					_, err = index.CASBlob(ctx, orphan, 0, f)
				}
				m.blobAfter = nil
				if (mode == "abandoned" && err != nil) || (mode != "abandoned" && err == nil) || !index.registry[token][orphan] {
					t.Fatal("reservation/outcome fixture", err)
				}
			}
			if mode == "foreign100000" {
				for i := 0; i < 100000; i++ {
					h := key([]byte(fmt.Sprint("foreign", i)))
					k := authorityKey(h, "other")
					if _, err = index.CASBlob(ctx, k, 0, Fence{Hash: h, Owner: "other", Generation: 1, Phase: "closed"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "fresh-adapter" || mode == "unknown-create" {
				index = &ownerScopeModelPort{Port: m, registry: index.registry}
			}
			start := epoch.Add(40 * time.Second)
			cost := &renewalLatencyPort{Port: m, now: start, rng: rand.New(rand.NewSource(42)), latency: time.Millisecond}
			index.Port, index.latency = cost, cost
			p.Port = index
			switch mode {
			case "unknown-enumeration":
				index.fault = func(keys []string) ([]string, error) { return keys, lostReply }
			case "duplicate":
				index.fault = func(keys []string) ([]string, error) { return append(keys, keys[0]), nil }
			case "foreign-entry":
				index.fault = func(keys []string) ([]string, error) {
					for k, r := range m.blobs {
						if r.Fence.Owner != token {
							return append(keys, k), nil
						}
					}
					t.Fatal("no foreign fixture")
					return nil, nil
				}
			case "invalid-key":
				index.fault = func(keys []string) ([]string, error) { return append(keys, "invalid"), nil }
			case "source-change":
				index.fault = func(keys []string) ([]string, error) {
					changed := m.roots["owner"]
					changed.Head++
					m.roots["owner"] = changed
					return keys, nil
				}
			}
			renewal, err := p.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return cost.now }, newExpiry)
			var result PreparedCompaction
			var done bool
			if err == nil {
				for calls := 0; calls < 100 && !done && err == nil; calls++ {
					result, done, err = renewal.Advance(ctx, 3)
				}
			}
			negative := mode == "unknown-enumeration" || mode == "duplicate" || mode == "foreign-entry" || mode == "invalid-key" || mode == "source-change"
			if negative {
				if err == nil || done || result.destination != "" {
					t.Fatal("bad discovery accepted", done, err)
				}
				if mode == "unknown-enumeration" && (!errors.Is(err, lostReply) || cost.blobs != 0 || cost.writes != 0) {
					t.Fatal("unknown enumeration hidden", err, cost)
				}
			} else {
				if err != nil || !done || !result.expires.Equal(newExpiry) {
					t.Fatal("indexed renewal failed", done, err)
				}
				if cost.blobs != uint64(len(index.registry[token])) || cost.blobs > 21 || index.listed != 1 || !cost.now.Before(oldExpiry) {
					t.Fatal("unrelated history read", cost, index.listed)
				}
				for k := range index.registry[token] {
					r := m.blobs[k]
					if r.Revision != 0 && !r.Fence.Intents[token].Expires.Equal(newExpiry) {
						t.Fatal("owned scope missed", k)
					}
				}
			}
			if mode != "source-change" && !reflect.DeepEqual(root, m.roots["owner"]) {
				t.Fatal("renewal published")
			}
			if mode == "foreign100000" {
				for _, record := range m.blobs {
					if record.Fence.Owner == "other" && (record.Revision != 1 || record.Fence.Phase != "closed" || len(record.Fence.Intents) != 0) {
						t.Fatal("foreign scope changed")
					}
				}
			}
			t.Logf("OWNER_DISCOVERY mode=%s namespace=%d indexed=%d blobs=%d writes=%d elapsed=%s complete=%t err=%v", mode, len(m.blobs), len(index.registry[token]), cost.blobs, cost.writes, cost.now.Sub(start), done, err)
		})
	}
}
