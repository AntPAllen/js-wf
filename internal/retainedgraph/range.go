package retainedgraph

import "context"

// RangeIterator streams an authenticated range with O(log population) pending
// trees. It retains no record bytes across Next calls and never caches nodes.
// A partial iteration does not prove that the rest of the range is readable.
type RangeIterator struct {
	ctx              context.Context
	store            Store
	pending          []Tree
	first, end, next uint64
	err              error
}

func NewRangeIterator(ctx context.Context, store Store, root Root, first, end uint64) (*RangeIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	if first > end || end > root.Count {
		return nil, ErrIndex
	}
	if store == nil {
		return nil, invalid("range store")
	}
	i := &RangeIterator{ctx: ctx, store: store, first: first, end: end, next: first}
	if first != end {
		for n := len(root.Frontier) - 1; n >= 0; n-- {
			i.pending = append(i.pending, root.Frontier[n])
		}
	}
	return i, nil
}

// Next returns one absolute index and record, or ok=false at the end. Errors
// are sticky: another call cannot turn an unreadable partial range into success.
func (i *RangeIterator) Next() (index uint64, record Record, ok bool, err error) {
	if i.err != nil {
		return 0, Record{}, false, i.err
	}
	defer func() {
		if err != nil {
			i.err = err
		}
	}()
	if err = i.ctx.Err(); err != nil {
		return
	}
	for len(i.pending) > 0 {
		last := len(i.pending) - 1
		tree := i.pending[last]
		i.pending = i.pending[:last]
		if tree.First >= i.end || tree.First+width(tree.Height) <= i.first {
			continue
		}
		var node node
		if node, err = load(i.ctx, i.store, tree); err != nil {
			return
		}
		if tree.Height == 0 {
			if tree.First != i.next {
				err = invalid("range coverage")
				return
			}
			if record, err = canonicalRecord(*node.Record); err != nil {
				return
			}
			i.next++
			return tree.First, record, true, nil
		}
		height := tree.Height - 1
		i.pending = append(i.pending,
			Tree{First: tree.First + width(height), Height: height, Link: node.Children[1]},
			Tree{First: tree.First, Height: height, Link: node.Children[0]})
	}
	if i.next != i.end {
		err = invalid("range coverage")
	}
	return
}

// ReadRange visits [first,end) in order, loading each intersecting tree node
// once. Traversal holds O(log population) nodes and never caches bytes across
// calls. The caller supplies authority and retention; callbacks before an error
// are a partial traversal, not proof that the complete range is readable.
func ReadRange(ctx context.Context, store Store, root Root, first, end uint64, visit func(uint64, Record) error) error {
	iterator, err := NewRangeIterator(ctx, store, root, first, end)
	if err != nil {
		return err
	}
	if visit == nil {
		return invalid("range store or visitor")
	}
	for {
		index, record, ok, err := iterator.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err = visit(index, record); err != nil {
			return err
		}
	}
}
