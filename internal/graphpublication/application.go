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
	if schemaRank(root.Schema) < 3 {
		root.Schema = ApplicationSchema
	}
	root.Application = append([]byte(nil), data...)
	return p.readerCAS(ctx, destination, expected, root)
}

// UpdateApplicationAndReleaseReader publishes a bounded application descriptor
// and removes exactly the supplied reader pin at the original observed head.
// The caller validates its logical transition and any consumed payload first.
// All live forests, ownership receipts and other reader snapshots are preserved.
// An uncertain CAS is resolved only by readerCAS's exact publication witness.
func (p Protocol) UpdateApplicationAndReleaseReader(ctx context.Context, reader Reader, expected uint64, data []byte) (Root, error) {
	if len(data) > MaxApplicationBytes {
		return Root{}, errors.New("application byte limit")
	}
	root, err := p.readerRoot(ctx, reader.destination, expected)
	if err != nil {
		return Root{}, err
	}
	i, err := readerIndex(root, reader)
	if err != nil {
		return Root{}, err
	}
	root.Readers = copyReaders(root.Readers)
	root.Readers = append(root.Readers[:i], root.Readers[i+1:]...)
	if len(root.Readers) == 0 {
		root.Readers = nil
	}
	if schemaRank(root.Schema) < 3 {
		root.Schema = ApplicationSchema
	}
	root.Application = append([]byte(nil), data...)
	return p.readerCAS(ctx, reader.destination, expected, root)
}

// RetireLiveWithApplication atomically clears the live graph and publishes its
// application lifecycle descriptor. Reader pins and permanent heads survive.
// Callers enforce their own terminal/generation transition before this CAS.
func (p Protocol) RetireLiveWithApplication(ctx context.Context, destination string, expected uint64, data []byte) (Root, error) {
	if len(data) > MaxApplicationBytes {
		return Root{}, errors.New("application byte limit")
	}
	root, err := p.readerRoot(ctx, destination, expected)
	if err != nil {
		return Root{}, err
	}
	if schemaRank(root.Schema) < 3 {
		root.Schema = ApplicationSchema
	}
	root.Application = append([]byte(nil), data...)
	clearLive(&root)
	return p.readerCAS(ctx, destination, expected, root)
}
