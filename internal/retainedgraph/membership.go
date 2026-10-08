package retainedgraph

import "context"

// FindTree resolves the canonical receipt of an aligned subtree by following
// only its ancestry. It validates each consumed ancestor, but does not read
// the target's bytes: a referenced object remains protected even if missing.
// ErrIndex means the requested subtree is outside this snapshot. Other errors
// are uncertainty, never evidence that an object is unreferenced.
// The root must come from the caller's canonical authority. This lookup alone
// is not an ownership grant or a fence against a concurrent root publication.
func FindTree(ctx context.Context, store Store, root Root, first uint64, height uint8) (Tree, error) {
	if err := ctx.Err(); err != nil {
		return Tree{}, err
	}
	if err := root.Validate(); err != nil {
		return Tree{}, err
	}
	if store == nil || height > 62 || first%width(height) != 0 {
		return Tree{}, invalid("subtree store or alignment")
	}
	span := width(height)
	if first >= root.Count || span > root.Count-first {
		return Tree{}, ErrIndex
	}
	for _, tree := range root.Frontier {
		if first < tree.First || first-tree.First >= width(tree.Height) {
			continue
		}
		for tree.Height > height {
			n, err := load(ctx, store, tree)
			if err != nil {
				return Tree{}, err
			}
			childHeight := tree.Height - 1
			right := first-tree.First >= width(childHeight)
			child := 0
			childFirst := tree.First
			if right {
				child = 1
				childFirst += width(childHeight)
			}
			tree = Tree{First: childFirst, Height: childHeight, Link: n.Children[child]}
		}
		if tree.First != first || tree.Height != height {
			return Tree{}, invalid("subtree range")
		}
		if err := ctx.Err(); err != nil {
			return Tree{}, err
		}
		return tree, nil
	}
	return Tree{}, invalid("uncovered subtree")
}

// ContainsNode compares the exact hash and physical generation/name receipt,
// not just content identity. Errors must stop a collector. A false result must
// still be coupled to a publication fence before reclaiming the object.
func ContainsNode(ctx context.Context, store Store, root Root, candidate Tree) (bool, error) {
	if !validLink(candidate.Link) {
		return false, invalid("candidate receipt")
	}
	tree, err := FindTree(ctx, store, root, candidate.First, candidate.Height)
	// A storage adapter can also return ErrIndex. Only the structurally
	// outside coordinate proves absence; an ancestry read failure never does.
	if err == ErrIndex && candidate.Height <= 62 && (candidate.First >= root.Count || width(candidate.Height) > root.Count-candidate.First) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return tree.Link == candidate.Link, nil
}

// ContainsBlob checks one leaf's bounded external edge list. A hash at a
// different physical generation/name does not match. As with ContainsNode,
// this is a snapshot lookup, not a reclamation decision or a reader pin.
func ContainsBlob(ctx context.Context, store Store, root Root, index uint64, candidate Link) (bool, error) {
	if !validLink(candidate) {
		return false, invalid("candidate payload receipt")
	}
	record, err := Read(ctx, store, root, index)
	if err == ErrIndex && index >= root.Count {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, link := range record.Blobs {
		if link == candidate {
			return true, nil
		}
	}
	return false, nil
}
