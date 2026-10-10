package retainedgraph

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestNodeIteratorBoundedFreshTraversalAndFailures(t *testing.T) {
	ctx := context.Background()
	store, root := newStore(), Empty()
	for index := uint64(0); index < 131; index++ {
		var err error
		root, err = Append(ctx, store, root, Record{Data: number(index)})
		if err != nil {
			t.Fatal(err)
		}
	}
	var expected []Tree
	if err := WalkNodes(ctx, store, root, func(tree Tree) error { expected = append(expected, tree); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"normal", "deadline", "cancel", "corrupt", "visitor"} {
		t.Run(mode, func(t *testing.T) {
			iterator, err := NewNodeIterator(ctx, store, root)
			if err != nil {
				t.Fatal(err)
			}
			store.gets = 0
			var visited []Tree
			maxPending := 0
			visit := func(tree Tree) error {
				if len(iterator.pending) > maxPending {
					maxPending = len(iterator.pending)
				}
				visited = append(visited, tree)
				return nil
			}
			if done, err := iterator.Advance(ctx, 0, visit); done || err == nil || store.gets != 0 {
				t.Fatal(done, err, store.gets)
			}
			if mode == "deadline" {
				attempts := 0
				done, err := iterator.Advance(ctx, 9, func(tree Tree) error {
					attempts++
					if attempts == 3 {
						return context.DeadlineExceeded
					}
					return visit(tree)
				})
				if done || !errors.Is(err, context.DeadlineExceeded) || iterator.Count() != 2 || store.gets != 3 {
					t.Fatal(done, err, iterator.Count(), store.gets)
				}
			}
			if mode == "cancel" {
				cancelled, cancel := context.WithCancel(ctx)
				done, err := iterator.Advance(cancelled, 9, func(tree Tree) error { cancel(); return visit(tree) })
				if done || !errors.Is(err, context.Canceled) || iterator.Count() != 1 || store.gets != 1 {
					t.Fatal(done, err, iterator.Count(), store.gets)
				}
			}
			if mode == "corrupt" || mode == "visitor" {
				if done, err := iterator.Advance(ctx, 7, visit); done || err != nil {
					t.Fatal(done, err)
				}
				link := iterator.pending[len(iterator.pending)-1].Link
				original := store.objects[link.Reference.Object]
				stop := errors.New("stopped visitor")
				callback := visit
				if mode == "corrupt" {
					store.objects[link.Reference.Object] = []byte(`{}`)
				} else {
					callback = func(Tree) error { return stop }
				}
				done, err := iterator.Advance(ctx, 7, callback)
				if done || err == nil || iterator.Count() != 7 {
					t.Fatal(done, err, iterator.Count())
				}
				store.objects[link.Reference.Object] = original
				gets := store.gets
				if done, err = iterator.Advance(ctx, 7, visit); done || err == nil || store.gets != gets {
					t.Fatal("fatal node failure reused", done, err, store.gets, gets)
				}
				return
			}
			for {
				before := iterator.Count()
				done, err := iterator.Advance(ctx, 7, visit)
				if err != nil || iterator.Count()-before > 7 {
					t.Fatal(done, err, iterator.Count()-before)
				}
				if done {
					break
				}
			}
			wantGets := 259
			if mode == "deadline" {
				wantGets++
			}
			if !reflect.DeepEqual(expected, visited) || iterator.Count() != 259 || store.gets != wantGets || maxPending > 10 {
				t.Fatal("order/rescan/space mismatch", len(visited), iterator.Count(), store.gets, maxPending)
			}
			t.Logf("NODE_ITERATOR mode=%s records=131 nodes=259 gets=%d max_pending=%d", mode, store.gets, maxPending)
		})
	}
}
