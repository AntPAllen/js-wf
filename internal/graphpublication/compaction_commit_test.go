package graphpublication

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/blobpublication"
)

func TestGraphPrefixCompactionCommitBatchesAndFences(t *testing.T) {
	for number, mode := range []string{"normal", "inherited", "record-deadline", "node-deadline", "append", "reader", "retire", "collector-records", "collector-nodes", "collector-at-cas", "node-grant", "drop", "lost", "unconfirmed", "plan-ownership"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root := stageFixture(t, fmt.Sprintf("verify-%02d", number), 12)
			if mode == "inherited" {
				plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 2, 1024, epoch.Add(time.Minute), nil)
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.CommitPrefixCompaction(ctx, plan)
				if err != nil {
					t.Fatal(err)
				}
			}
			plan, err := p.PreparePrefixCompaction(ctx, "owner", root.Head, 5, 1024, epoch.Add(time.Minute), []byte("cursor"))
			if err != nil {
				t.Fatal(err)
			}
			original := cloneRoot(root)
			op, err := p.BeginCompactionCommit(ctx, plan)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "plan-ownership" {
				plan.publication.Application[0] = 'x'
				plan.publication.Graph.Frontier[0].Link.Hash = "forged"
				plan.base.Graph.Frontier[0].Link.Hash = "forged"
			}
			result, done, err := op.Advance(ctx, 2, 3)
			if err != nil || done || result.Head != 0 || op.NextIndex() != 2 || op.VerifiedNodes() != 0 || !reflect.DeepEqual(original, m.roots["owner"]) {
				t.Fatal("partial commit", done, err, result, op.NextIndex(), op.VerifiedNodes())
			}
			if mode == "record-deadline" {
				reads := 0
				m.readBlobHook = func(string) error {
					reads++
					if reads == 5 {
						return context.DeadlineExceeded
					}
					return nil
				}
				if _, done, err = op.Advance(ctx, 5, 3); done || !errors.Is(err, context.DeadlineExceeded) || op.NextIndex() != 4 {
					t.Fatal("record deadline lost progress", done, err, op.NextIndex())
				}
				m.readBlobHook = nil
			}
			if mode == "append" {
				appendOne(t, m, p, "owner", []byte("raced"), nil)
			}
			if mode == "reader" {
				if _, _, err = p.AcquireReader(ctx, "owner", root.Head, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "retire" {
				if err = p.RetireLive(ctx, "owner", root.Head); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "collector-records" {
				if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "append" || mode == "reader" || mode == "retire" || mode == "collector-records" {
				before := cloneRoot(m.roots["owner"])
				if _, done, err = op.Advance(ctx, 2, 3); done || !errors.Is(err, ErrConflict) || !reflect.DeepEqual(before, m.roots["owner"]) {
					t.Fatal("changed source committed", done, err)
				}
				return
			}
			for op.NextIndex() < root.Graph.Count {
				before := op.NextIndex()
				result, done, err = op.Advance(ctx, 2, 3)
				if err != nil || done || result.Head != 0 || op.NextIndex()-before > 2 || op.VerifiedNodes() > 3 || !reflect.DeepEqual(original, m.roots["owner"]) {
					t.Fatal("invalid record batch", done, err, op.NextIndex(), op.VerifiedNodes())
				}
			}
			if mode == "node-deadline" {
				reads := 0
				m.readBlobHook = func(string) error {
					reads++
					if reads == 2 {
						return context.DeadlineExceeded
					}
					return nil
				}
				before := op.VerifiedNodes()
				if _, done, err = op.Advance(ctx, 2, 4); done || !errors.Is(err, context.DeadlineExceeded) || op.VerifiedNodes() != before+1 {
					t.Fatal("node deadline lost progress", done, err, op.VerifiedNodes(), before)
				}
				m.readBlobHook = nil
			}
			if mode == "collector-nodes" {
				if _, err = p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "node-grant" {
				// Revoke an as-yet-unchecked leaf, independently of source-head checks.
				tree := op.prepared.publication.Graph.Frontier[len(op.prepared.publication.Graph.Frontier)-1]
				scope, _, err := objectAuthority(blobpublication.Object{Key: tree.Link.Hash, Reference: tree.Link.Reference})
				if err != nil {
					t.Fatal(err)
				}
				grant := m.blobs[scope]
				grant.Fence.Intents = nil
				grant.Fence.Phase = "closed"
				grant.Fence.Object = ""
				m.blobs[scope] = grant
			}
			if mode == "collector-at-cas" {
				m.rootBefore = func(string, uint64, Root) error { _, err := p.SweepWithReaders(ctx, epoch.Add(time.Hour)); return err }
			}
			if mode == "drop" {
				m.rootBefore = func(string, uint64, Root) error { return lostReply }
			}
			if mode == "lost" {
				m.rootAfter = func() error { return lostReply }
			}
			if mode == "unconfirmed" {
				m.rootAfter = func() error { m.readRootHook = func(string) error { return lostReply }; return lostReply }
			}
			calls := 0
			for !done {
				before := op.VerifiedNodes()
				result, done, err = op.Advance(ctx, 2, 3)
				calls++
				if op.VerifiedNodes()-before > 3 {
					t.Fatal("node budget exceeded", before, op.VerifiedNodes())
				}
				if err != nil {
					break
				}
				if !done && (result.Head != 0 || !reflect.DeepEqual(original, m.roots["owner"])) {
					t.Fatal("partial nodes published")
				}
			}
			bad := mode == "collector-nodes" || mode == "collector-at-cas" || mode == "node-grant" || mode == "drop" || mode == "unconfirmed"
			if bad {
				if err == nil || done {
					t.Fatal("failed verification committed", done, err)
				}
				if _, again, e := op.Advance(ctx, 2, 3); again || e == nil {
					t.Fatal("failed finalization reused", again, e)
				}
				m.readRootHook = nil
				if mode == "drop" || mode == "unconfirmed" {
					if _, err = p.CommitPrefixCompaction(ctx, plan); err != nil {
						t.Fatal("fresh operation failed", err)
					}
				}
			} else {
				if err != nil || !done || result.Head != root.Head+1 || op.NextIndex() != root.Graph.Count {
					t.Fatal("complete commit failed", done, err, result.Head)
				}
				if mode != "inherited" && op.VerifiedNodes() != 19 {
					t.Fatal("missing node checks", op.VerifiedNodes())
				}
				result.Application[0] = 'x'
				again, done, err := op.Advance(ctx, 2, 3)
				if !done || err != nil || string(again.Application) != "cursor" {
					t.Fatal("caller changed committed result", done, err, string(again.Application))
				}
			}
			t.Logf("COMPACTION_COMMIT mode=%s records=%d nodes=%d final_node_calls=%d", mode, op.NextIndex(), op.VerifiedNodes(), calls)
		})
	}
}
