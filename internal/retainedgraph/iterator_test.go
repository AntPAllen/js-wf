package retainedgraph

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestRangeIteratorStreamsFreshNodesAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	store, root := newStore(), Empty()
	for i := uint64(0); i < 131; i++ {
		var err error
		root, err = Append(ctx, store, root, Record{Data: number(i), Blobs: []Link{external()}})
		if err != nil {
			t.Fatal(err)
		}
	}
	leaves := map[uint64]Tree{}
	store.gets = 0
	nodes := 0
	if err := WalkNodes(ctx, store, root, func(tree Tree) error {
		nodes++
		if tree.Height == 0 {
			leaves[tree.First] = tree
		}
		return nil
	}); err != nil || nodes != 259 || store.gets != 259 || len(leaves) != 131 {
		t.Fatal("node traversal loaded payloads or repeated branches", nodes, store.gets, len(leaves), err)
	}
	for _, bounds := range [][2]uint64{{0, 131}, {63, 65}, {128, 131}, {77, 77}} {
		store.gets = 0
		it, err := NewRangeIterator(ctx, store, root, bounds[0], bounds[1])
		if err != nil {
			t.Fatal(err)
		}
		next := bounds[0]
		for {
			index, record, ok, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if index != next || !bytes.Equal(record.Data, number(index)) || len(record.Blobs) != 1 || record.Blobs[0] != external() {
				t.Fatal(index, next, record)
			}
			next++
			if len(it.pending) > 16 {
				t.Fatal("unbounded pending traversal", len(it.pending))
			}
		}
		if next != bounds[1] {
			t.Fatal("range truncated", next, bounds)
		}
		if bounds == [2]uint64{0, 131} && store.gets != 259 {
			t.Fatal("repeated branch reads", store.gets)
		}
		if bounds[0] == bounds[1] && store.gets != 0 {
			t.Fatal("empty range read bytes")
		}
	}
	it, err := NewRangeIterator(ctx, store, root, 0, root.Count)
	if err != nil {
		t.Fatal(err)
	}
	if index, _, ok, err := it.Next(); err != nil || !ok || index != 0 {
		t.Fatal(index, ok, err)
	}
	name := leaves[1].Link.Reference.Object
	good := bytes.Clone(store.objects[name])
	store.objects[name] = []byte(`{}`)
	if _, _, ok, err := it.Next(); !errors.Is(err, ErrInvalid) || ok {
		t.Fatal("corrupt later leaf accepted", ok, err)
	}
	store.objects[name] = good
	if _, _, ok, err := it.Next(); !errors.Is(err, ErrInvalid) || ok {
		t.Fatal("partial failed iterator resurrected", ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	it, err = NewRangeIterator(canceled, store, root, 0, root.Count)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := it.Next(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	cancel()
	if _, _, ok, err := it.Next(); !errors.Is(err, context.Canceled) || ok {
		t.Fatal(ok, err)
	}
	stop := errors.New("stop node traversal")
	visits := 0
	if err = WalkNodes(ctx, store, root, func(Tree) error { visits++; return stop }); !errors.Is(err, stop) || visits != 1 {
		t.Fatal(visits, err)
	}
	store.objects[root.Frontier[0].Link.Reference.Object] = []byte(`{}`)
	visits = 0
	if err = WalkNodes(ctx, store, root, func(Tree) error { visits++; return nil }); !errors.Is(err, ErrInvalid) || visits != 0 {
		t.Fatal("corrupt root accepted", visits, err)
	}
}
