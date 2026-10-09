package retainedgraph

import "context"

// ReadRange visits [first,end) in order, loading each intersecting tree node
// once. Traversal holds O(log population) nodes and never caches bytes across
// calls. The caller supplies authority and retention; callbacks before an error
// are a partial traversal, not proof that the complete range is readable.
func ReadRange(ctx context.Context, store Store, root Root, first, end uint64, visit func(uint64, Record) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Validate(); err != nil {
		return err
	}
	if first > end || end > root.Count {
		return ErrIndex
	}
	if store == nil || visit == nil {
		return invalid("range store or visitor")
	}
	if first == end {
		return ctx.Err()
	}
	var walk func(Tree) error
	walk = func(tree Tree) error {
		if tree.First >= end || tree.First+width(tree.Height) <= first {
			return nil
		}
		n, err := load(ctx, store, tree)
		if err != nil {
			return err
		}
		if tree.Height == 0 {
			record, err := canonicalRecord(*n.Record)
			if err != nil {
				return err
			}
			if err = visit(tree.First, record); err != nil {
				return err
			}
			return ctx.Err()
		}
		height := tree.Height - 1
		if err = walk(Tree{First: tree.First, Height: height, Link: n.Children[0]}); err != nil {
			return err
		}
		return walk(Tree{First: tree.First + width(height), Height: height, Link: n.Children[1]})
	}
	for _, tree := range root.Frontier {
		if err := walk(tree); err != nil {
			return err
		}
	}
	return ctx.Err()
}
