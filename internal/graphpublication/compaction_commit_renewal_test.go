package graphpublication

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGraphCompactionVerificationRenewalPrivateProgressAndFences(t *testing.T) {
	for _, mode := range []string{"records", "nodes", "repeated", "verifier-port", "wrong-stage", "cancel", "expired", "append", "collector", "collector-during", "drop", "lost"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, "verify-renew", 12)
			original := copyCompactionRoot(root)
			expires := epoch.Add(time.Minute)
			now := epoch.Add(20 * time.Second)
			clock := func() time.Time { return now }
			stage, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 5, 1024, expires, []byte("cursor"))
			if err != nil {
				t.Fatal(err)
			}
			plan, done, err := stage.Advance(ctx, 12)
			if err != nil || !done {
				t.Fatal(done, err)
			}
			commit, err := p.BeginCompactionCommit(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			budget := uint64(2)
			if mode == "nodes" {
				budget = 12
			}
			if _, done, err := commit.Advance(ctx, budget, 2); done || err != nil {
				t.Fatal(done, err)
			}
			records, nodes := commit.NextIndex(), commit.VerifiedNodes()
			if mode == "wrong-stage" {
				foreign, err := p.BeginPrefixCompaction(ctx, "owner", root.Head, 5, 1024, expires, []byte("cursor"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := commit.BeginIntentRenewal(ctx, foreign, clock, epoch.Add(3*time.Minute)); !errors.Is(err, ErrConflict) {
					t.Fatal("wrong stage paired", err)
				}
				return
			}
			if mode == "expired" {
				now = expires
				if _, err := commit.BeginIntentRenewal(ctx, stage, clock, epoch.Add(3*time.Minute)); !errors.Is(err, ErrRevoked) {
					t.Fatal("expired verification renewed", err)
				}
				return
			}
			if mode == "verifier-port" {
				_, foreign := newModel("foreign")
				stage.protocol = foreign
			}
			renewal, err := commit.BeginIntentRenewal(ctx, stage, clock, epoch.Add(3*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			if _, done, err := commit.Advance(ctx, 12, 100); done || !errors.Is(err, ErrConflict) || commit.NextIndex() != records || commit.VerifiedNodes() != nodes {
				t.Fatal("verification escaped freeze", done, err)
			}
			if _, done, err := stage.Advance(ctx, 12); done || !errors.Is(err, ErrConflict) {
				t.Fatal("stage escaped freeze", done, err)
			}
			if _, err := stage.Checkpoint(); !errors.Is(err, ErrConflict) {
				t.Fatal("pending checkpoint emitted", err)
			}
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if done, err := renewal.Advance(cancelled, 3); done || !errors.Is(err, context.Canceled) {
					t.Fatal(done, err)
				}
			}
			if mode == "drop" {
				renewal.operation.protocol.Port = &renewalDropPort{Port: m, token: plan.publication.Token, drop: true}
			}
			if mode == "lost" || mode == "collector-during" {
				m.blobAfter = func(_ string, f Fence) error {
					if f.Owner != plan.publication.Token {
						return nil
					}
					m.blobAfter = nil
					if mode == "lost" {
						return lostReply
					}
					_, err := p.SweepWithReaders(ctx, expires)
					return err
				}
			}
			if mode == "append" {
				appendOne(t, m, p, "owner", []byte("raced"), nil)
			}
			if mode == "collector" {
				if _, err := p.SweepWithReaders(ctx, expires); err != nil {
					t.Fatal(err)
				}
			}
			for {
				before := renewal.ExaminedScopes()
				done, err = renewal.Advance(ctx, 3)
				if renewal.ExaminedScopes()-before > 3 || commit.NextIndex() != records || commit.VerifiedNodes() != nodes {
					t.Fatal("renewal changed verification progress")
				}
				if err != nil || done {
					break
				}
			}
			negative := mode == "append" || mode == "collector" || mode == "collector-during" || mode == "drop" || mode == "lost"
			if negative {
				if err == nil || done {
					t.Fatal("unsafe renewal accepted", done, err)
				}
				if _, done, again := commit.Advance(ctx, 12, 100); done || again != err {
					t.Fatal("failed verifier reused", done, again, err)
				}
				if _, done, again := stage.Advance(ctx, 12); done || again != err {
					t.Fatal("failed stage reused", done, again, err)
				}
				if m.roots["owner"].Token == plan.publication.Token {
					t.Fatal("failed renewal published")
				}
				return
			}
			if err != nil || !done || !commit.prepared.expires.Equal(epoch.Add(3*time.Minute)) || !stage.prepared.expires.Equal(commit.prepared.expires) {
				t.Fatal(done, err)
			}
			if mode == "repeated" {
				if _, done, err := commit.Advance(ctx, 2, 2); done || err != nil {
					t.Fatal(done, err)
				}
				records, nodes = commit.NextIndex(), commit.VerifiedNodes()
				now = expires.Add(30 * time.Second)
				renewal, err = commit.BeginIntentRenewal(ctx, stage, clock, epoch.Add(5*time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				for {
					done, err := renewal.Advance(ctx, 3)
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
				}
				if commit.NextIndex() != records || commit.VerifiedNodes() != nodes {
					t.Fatal("second renewal reset private progress")
				}
			}
			now = expires.Add(time.Second)
			if _, err := p.SweepWithReaders(ctx, now); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(original, m.roots["owner"]) {
				t.Fatal("renewal fenced source")
			}
			for {
				_, done, err := commit.Advance(ctx, 2, 3)
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if commit.NextIndex() != 12 || commit.VerifiedNodes() != 19 {
				t.Fatal("verification incomplete")
			}
			if _, err := commit.BeginIntentRenewal(ctx, stage, clock, epoch.Add(10*time.Minute)); !errors.Is(err, ErrConflict) {
				t.Fatal("published verifier renewed", err)
			}
			t.Logf("VERIFICATION_RENEWAL mode=%s preserved_records=%d preserved_nodes=%d records=12 nodes=19 root_unchanged_after_old_expiry=true", mode, records, nodes)
		})
	}
}
