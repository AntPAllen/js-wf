package graphpublication

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"testing"
	"time"
)

type ownerScanModel struct {
	keys        []string // authoritative backend index, built before opening a cursor
	next, calls int
	fault       func([]string, bool) ([]string, bool, error)
}

func (s *ownerScanModel) Advance(c context.Context, budget uint64) ([]string, bool, error) {
	if err := c.Err(); err != nil {
		return nil, false, err
	}
	s.calls++
	end := s.next + min(int(budget), len(s.keys)-s.next)
	keys := append([]string(nil), s.keys[s.next:end]...)
	s.next = end
	done := end == len(s.keys)
	if s.fault != nil {
		return s.fault(keys, done)
	}
	return keys, done, nil
}

type ownerScanModelPort struct {
	*ownerWitnessModel
	index   []string
	scan    *ownerScanModel
	begin   func() error
	nilScan bool
}

func (p *ownerScanModelPort) BeginOwnerScopeScan(c context.Context, _ string) (OwnerScopeScan, error) {
	if err := c.Err(); err != nil {
		return nil, err
	}
	if p.begin != nil {
		if err := p.begin(); err != nil {
			return nil, err
		}
	}
	if p.nilScan {
		return nil, nil
	}
	p.scan = &ownerScanModel{keys: p.index}
	return p.scan, nil
}

func TestGraphCompactionOwnerScanCompletenessAndBounds(t *testing.T) {
	for _, mode := range []string{"normal", "fresh-cursor", "unknown-begin", "nil-scan", "unknown-batch", "duplicate", "duplicate-later", "oversize", "no-progress", "invalid-key", "source-change", "expired"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "scan", 12)
			base := &ownerScopeModelPort{Port: m, registry: map[string]map[string]bool{}}
			p.Port = base
			expires := epoch.Add(time.Minute)
			plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, expires, nil)
			if err != nil {
				t.Fatal(err)
			}
			cost := &renewalLatencyPort{Port: m, now: epoch.Add(40 * time.Second), rng: rand.New(rand.NewSource(42)), latency: time.Millisecond}
			base.Port, base.latency = cost, cost
			port := &ownerScanModelPort{ownerWitnessModel: &ownerWitnessModel{ownerScopeModelPort: base}}
			for k := range base.registry[plan.publication.Token] {
				port.index = append(port.index, k)
			}
			sort.Strings(port.index)
			p.Port = port
			if mode == "unknown-begin" {
				port.begin = func() error { return lostReply }
			}
			if mode == "nil-scan" {
				port.nilScan = true
			}
			r, err := p.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return cost.now }, expires.Add(time.Minute))
			if mode == "unknown-begin" || mode == "nil-scan" {
				if err == nil || r != nil || port.listed != 0 || cost.blobs != 0 {
					t.Fatal("bad begin hidden", r, err)
				}
				return
			}
			if err != nil || len(r.keys) != 0 || port.validated != 0 || port.listed != 0 {
				t.Fatal("setup eagerly discovered", err, len(r.keys), port.listed)
			}
			port.scan.fault = func(keys []string, done bool) ([]string, bool, error) {
				switch mode {
				case "unknown-batch":
					return keys, done, lostReply
				case "duplicate":
					if port.scan.calls == 1 {
						return []string{keys[0], keys[0]}, false, nil
					}
				case "duplicate-later":
					if port.scan.calls == 2 {
						return []string{port.index[0]}, false, nil
					}
				case "oversize":
					return append(keys, port.index[3]), false, nil
				case "no-progress":
					return nil, false, nil
				case "invalid-key":
					return []string{"bad"}, false, nil
				case "source-change":
					changed := m.roots["owner"]
					changed.Head++
					m.roots["owner"] = changed
				case "expired":
					cost.now = expires
				}
				return keys, done, nil
			}
			var done bool
			var result PreparedCompaction
			for calls := 0; calls < 20 && !done && err == nil; calls++ {
				before := port.validated
				result, done, err = r.Advance(ctx, 2)
				if port.validated-before > 2 {
					t.Fatal("scope scan budget exceeded")
				}
				if mode == "fresh-cursor" && calls == 0 {
					first := port.scan
					r, err = p.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return cost.now }, expires.Add(time.Minute))
					if err != nil || port.scan == first || r.ExaminedScopes() != 0 {
						t.Fatal("fresh scan inherited proof", err)
					}
				}
			}
			positive := mode == "normal" || mode == "fresh-cursor"
			if positive {
				if err != nil || !done || r.RenewedScopes() != 20 || r.ExaminedScopes() != 20 {
					t.Fatal("complete scan failed", done, err, r.RenewedScopes())
				}
			} else {
				if err == nil || done || result.destination != "" {
					t.Fatal("bad scan accepted", done, err)
				}
				before, calls, reads := port.validated, port.scan.calls, cost.blobs
				if _, done, e := r.Advance(ctx, 2); done || e != err || port.validated != before || port.scan.calls != calls || cost.blobs != reads {
					t.Fatal("failed scan retried", done, e)
				}
				if mode == "unknown-batch" && !errors.Is(err, lostReply) {
					t.Fatal(err)
				}
			}
			t.Logf("OWNER_SCAN mode=%s fallback_census=%d examined=%d renewed=%d marker_validations=%d complete=%t err=%v", mode, port.listed, r.ExaminedScopes(), r.RenewedScopes(), port.validated, done, err)
		})
	}
}
