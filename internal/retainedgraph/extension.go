package retainedgraph

import (
	"context"
	"math"
)

// AppendDelta describes only the new right spine and final record. Nodes are
// ordered from the new frontier down to the appended leaf. Its size is bounded
// by63 nodes and128 payload edges, independent of retained history length.
// A delta proves structural inheritance, not ownership or publication success.
type AppendDelta struct {
	Nodes  []Tree
	Record Record
}

// ValidateAppend verifies that next extends the exact canonical base by one
// record, preserving every inherited physical receipt. Only new spine nodes
// are read. Unchanged subtrees are not audited or reacquired here.
// The caller must bind base to its original authoritative destination/head,
// verify publication intents for all delta receipts and fence the eventual CAS.
// Neither an arbitrary base nor successful validation grants subtree ownership.
func ValidateAppend(ctx context.Context, store Store, base, next Root) (AppendDelta, error) {
	if err := ctx.Err(); err != nil {
		return AppendDelta{}, err
	}
	if err := base.Validate(); err != nil {
		return AppendDelta{}, err
	}
	if err := next.Validate(); err != nil {
		return AppendDelta{}, err
	}
	if store == nil || base.Count == math.MaxInt64 || next.Count != base.Count+1 {
		return AppendDelta{}, invalid("append store or population")
	}
	merges := 0
	for i := len(base.Frontier) - 1; i >= 0 && base.Frontier[i].Height == uint8(merges); i-- {
		merges++
	}
	prefix := len(base.Frontier) - merges
	if len(next.Frontier) != prefix+1 {
		return AppendDelta{}, invalid("append frontier shape")
	}
	for i := 0; i < prefix; i++ {
		if next.Frontier[i] != base.Frontier[i] {
			return AppendDelta{}, invalid("changed inherited frontier receipt")
		}
	}
	carry := next.Frontier[prefix]
	if carry.Height != uint8(merges) || carry.First != base.Count-(width(uint8(merges))-1) {
		return AppendDelta{}, invalid("append spine range")
	}
	delta := AppendDelta{Nodes: make([]Tree, 0, merges+1)}
	for i := prefix; i < len(base.Frontier); i++ {
		n, err := load(ctx, store, carry)
		if err != nil {
			return AppendDelta{}, err
		}
		if n.Children[0] != base.Frontier[i].Link {
			return AppendDelta{}, invalid("changed inherited child receipt")
		}
		delta.Nodes = append(delta.Nodes, carry)
		height := carry.Height - 1
		carry = Tree{First: carry.First + width(height), Height: height, Link: n.Children[1]}
	}
	if carry.First != base.Count || carry.Height != 0 {
		return AppendDelta{}, invalid("appended leaf position")
	}
	n, err := load(ctx, store, carry)
	if err != nil {
		return AppendDelta{}, err
	}
	delta.Nodes = append(delta.Nodes, carry)
	record, err := canonicalRecord(*n.Record)
	if err != nil {
		return AppendDelta{}, err
	}
	delta.Record = record
	if err := ctx.Err(); err != nil {
		return AppendDelta{}, err
	}
	return delta, nil
}
