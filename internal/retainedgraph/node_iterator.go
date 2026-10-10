package retainedgraph

import (
	"context"
	"errors"
)

// NodeIterator visits freshly authenticated nodes with O(log population) pending
// coordinates. It retains no node bytes and accepts a fresh context per batch.
// Coordinates establish traversal membership, never grant or reader authority.
type NodeIterator struct {
	store   Store
	pending []Tree
	count   uint64
	err     error
}

func NewNodeIterator(ctx context.Context, store Store, root Root) (*NodeIterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, invalid("node iterator store")
	}
	i := &NodeIterator{store: store}
	for n := len(root.Frontier) - 1; n >= 0; n-- {
		i.pending = append(i.pending, root.Frontier[n])
	}
	return i, nil
}

func (i *NodeIterator) Count() uint64 { return i.count }

// Advance calls visit before recording progress. Deadline/cancellation retains
// only completed visits, so retry freshly reloads the unfinished node. Other
// errors are sticky. maxNodes bounds visits, not all work in the caller's visit.
func (i *NodeIterator) Advance(ctx context.Context, maxNodes uint64, visit func(Tree) error) (done bool, err error) {
	if i.err != nil {
		return false, i.err
	}
	if maxNodes == 0 || visit == nil {
		return false, invalid("node iterator budget or visitor")
	}
	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			i.err = err
		}
	}()
	for visited := uint64(0); visited < maxNodes && len(i.pending) > 0; visited++ {
		if err = ctx.Err(); err != nil {
			return false, err
		}
		last := len(i.pending) - 1
		tree := i.pending[last]
		n, loadErr := load(ctx, i.store, tree)
		if loadErr != nil {
			return false, loadErr
		}
		if err = visit(tree); err != nil {
			return false, err
		}
		i.pending = i.pending[:last]
		if tree.Height > 0 {
			height := tree.Height - 1
			i.pending = append(i.pending, Tree{First: tree.First + width(height), Height: height, Link: n.Children[1]}, Tree{First: tree.First, Height: height, Link: n.Children[0]})
		}
		i.count++
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	return len(i.pending) == 0, nil
}
