package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type ownerWitnessModel struct {
	*ownerScopeModelPort
	validated int
	hook      func(string) error
}

func (p *ownerWitnessModel) ValidateOwnerScope(c context.Context, token, k string) error {
	if err := c.Err(); err != nil {
		return err
	}
	p.validated++
	p.latency.charge()
	if !p.registry[token][k] {
		return errors.New("missing owner registration")
	}
	if p.hook != nil {
		return p.hook(k)
	}
	return nil
}

func TestGraphCompactionOwnerWitnessScopeBudget(t *testing.T) {
	for _, mode := range []string{"normal", "reservations100000", "lost-later", "expired-during-witness", "source-change", "corrupt-marker", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "witness-budget", 12)
			base := &ownerScopeModelPort{Port: m, registry: map[string]map[string]bool{}}
			p.Port = base
			oldExpiry, newExpiry := epoch.Add(time.Minute), epoch.Add(2*time.Minute)
			plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, oldExpiry, nil)
			if err != nil {
				t.Fatal(err)
			}
			token := plan.publication.Token
			if mode == "reservations100000" {
				for i := 0; i < 100000; i++ {
					base.registry[token][authorityKey(key([]byte(fmt.Sprint("reservation", i))), token)] = true
				}
			}
			cost := &renewalLatencyPort{Port: m, now: epoch.Add(40 * time.Second), rng: rand.New(rand.NewSource(42)), latency: time.Millisecond}
			base.Port, base.latency = cost, cost
			witness := &ownerWitnessModel{ownerScopeModelPort: base}
			p.Port = witness
			renew, err := p.BeginCompactionIntentRenewal(ctx, plan, func() time.Time { return cost.now }, newExpiry)
			if err != nil || witness.validated != 0 || cost.blobs != 0 {
				t.Fatal("setup witnessed scopes", err, witness.validated, cost.blobs)
			}
			// Fail a later batch after two scopes were already confirmed. Nothing
			// from an uncertain/changed/expired marker can authorize scope I/O.
			witness.hook = func(string) error {
				if witness.validated != 4 {
					return nil
				}
				switch mode {
				case "lost-later":
					return lostReply
				case "expired-during-witness":
					cost.now = oldExpiry
				case "source-change":
					changed := m.roots["owner"]
					changed.Head++
					m.roots["owner"] = changed
				case "corrupt-marker":
					return errors.New("noncanonical marker")
				}
				return nil
			}
			var done bool
			var result PreparedCompaction
			for calls := 0; calls < 20 && !done && err == nil; calls++ {
				before := witness.validated
				result, done, err = renew.Advance(ctx, 2)
				if witness.validated-before > 2 {
					t.Fatal("witness scope budget exceeded")
				}
				if (mode == "reservations100000" || mode == "cancel") && calls == 0 {
					break
				}
			}
			switch mode {
			case "normal":
				if err != nil || !done || witness.validated != 20 || !result.expires.Equal(newExpiry) {
					t.Fatal("healthy witness scan", done, err, witness.validated)
				}
			case "reservations100000":
				if done || err != nil || witness.validated != 2 || renew.ExaminedScopes() != 2 || result.destination != "" {
					t.Fatal("large census batch not bounded", done, err, witness.validated)
				}
			case "cancel":
				cancelled, stop := context.WithCancel(ctx)
				stop()
				before := witness.validated
				if _, done, e := renew.Advance(cancelled, 2); done || !errors.Is(e, context.Canceled) || witness.validated != before {
					t.Fatal("cancelled witness work", done, e)
				}
			default:
				if err == nil || done || result.destination != "" || witness.validated != 4 || cost.blobs != 3 || renew.ExaminedScopes() != 3 {
					t.Fatal("failed witness used", done, err, witness.validated, cost.blobs, renew.ExaminedScopes())
				}
				before := *cost
				if _, done, e := renew.Advance(ctx, 2); done || e != err || witness.validated != 4 || !reflect.DeepEqual(before, *cost) {
					t.Fatal("failed witness retried", done, e)
				}
			}
			if mode != "source-change" && !reflect.DeepEqual(root, m.roots["owner"]) {
				t.Fatal("renewal published")
			}
			t.Logf("OWNER_WITNESS mode=%s census=%d setup_witnesses=0 validated=%d examined=%d blobs=%d complete=%t err=%v", mode, len(base.registry[token]), witness.validated, renew.ExaminedScopes(), cost.blobs, done, err)
		})
	}
}
