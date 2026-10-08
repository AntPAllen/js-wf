package graphpublication

import (
	"context"
	"errors"
)

// UpdateApplication publishes an application-owned bounded descriptor at the
// original observed head, preserving graph receipts and reader pins. Consumers
// must validate their own versioned logical index/epoch/lifecycle schema. This
// protocol treats its bytes as opaque and does not derive object grants from it.
// The permanent v3 authority schema prevents old adapters erasing the descriptor.
func (p Protocol) UpdateApplication(ctx context.Context, destination string, expected uint64, data []byte) (Root, error) {
	if len(data) > MaxApplicationBytes {
		return Root{}, errors.New("application byte limit")
	}
	root, err := p.readerRoot(ctx, destination, expected)
	if err != nil {
		return Root{}, err
	}
	root.Schema = ApplicationSchema
	root.Application = append([]byte(nil), data...)
	return p.readerCAS(ctx, destination, expected, root)
}
